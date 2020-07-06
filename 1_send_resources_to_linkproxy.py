import click
import dataset as dataset_lib
import requests

from datetime import datetime, timedelta

db = dataset_lib.connect("sqlite:///orchestrator.db")

EXCLUDED_PATTERNS = [
    "resources/donnees-temps-reel-de-mesure-des-concentrations-de-polluants-atmospheriques-reglementes-1",
    "files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes",
    # already ignored by link-proxy but let's not clobber things up
    "files.geo.data.gouv.fr/link-proxy/"
]


@click.group()
def cli():
    pass


def get_last_run():
    table = db["runs"]
    last_run = table.find_one(order_by='-date')
    if not last_run:
        return datetime.now() - timedelta(days=1)
    else:
        return last_run["date"]


def record_run(count, count_ignored):
    table = db["runs"]
    table.insert({
        "nb_resources": count,
        "nb_resources_ignored": count_ignored,
        "date": datetime.now()
    })


def get_datasets(page):
    r = requests.get(f"https://www.data.gouv.fr/api/1/datasets/?sort=-last_modified&page={page}")
    return r.json()["data"]


def modified_datasets(last_run):
    got_everything = False
    results = []
    page = 1

    while not got_everything:
        data = get_datasets(page)
        for d in data:
            modified = datetime.fromisoformat(d["last_modified"])
            got_everything = (modified < last_run)
            if not got_everything:
                results.append(d)
            else:
                break
        if got_everything:
            break
        else:
            page += 1

    return results


def send_to_linkproxy(url):
    r = requests.post("http://pad-01.infra.data.gouv.fr:5000", json={
        "location": url
    })
    r.raise_for_status()
    return r.json()


def handle_dataset(dataset, last_run):
    count = 0
    count_ignored = 0
    table = db["checks"]
    for resource in dataset["resources"]:
        modified_date = datetime.fromisoformat(resource["last_modified"])
        if last_run > modified_date:
            continue
        if any([excl in resource["url"] for excl in EXCLUDED_PATTERNS]):
            count_ignored += 1
            continue
        try:
            res = send_to_linkproxy(resource["url"])
        except requests.HTTPError as e:
            click.secho(f"Error while creating check: {e}", err=True, fg="red")
        else:
            count += 1
            existing = table.find_one(check_id=res["_id"], dataset_id=dataset["id"], resource_id=resource["id"])
            data = {
                "check_id": res["_id"],
                "modified_at": datetime.now(),
                "dataset_id": dataset["id"],
                "resource_id": resource["id"],
                "url": resource["url"],
                "dataset_title":dataset["title"],
                "resource_title":resource["title"]
            }
            if not existing:
                data["created_at"] = datetime.now()
                table.insert(data)
            else:
                table.update(data, ["check_id", "resource_id", "dataset_id"])
    return count, count_ignored


@cli.command()
def run():
    last_run = get_last_run()
    click.echo(f"Last run: {last_run}")
    datasets = modified_datasets(last_run)
    count = 0
    count_ignored = 0
    with click.progressbar(datasets, label=f"Analysing {len(datasets)} datasets") as all_datasets:
        for dataset in all_datasets:
            _count, _count_ignored = handle_dataset(dataset, last_run)
            count += _count
            count_ignored += _count_ignored
    record_run(count, count_ignored)
    click.secho("Done!", fg="green")


if __name__ == "__main__":
    cli()
