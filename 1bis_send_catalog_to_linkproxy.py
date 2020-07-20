import click
import dataset as dataset_lib
import requests
import calendar
from datetime import datetime, timedelta
from datetime import date
import urllib
import pandas as pd
from queue import Queue
from threading import Thread

records = []

db = dataset_lib.connect("sqlite:///orchestrator.db")

@click.group()
def cli():
    pass

class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue 
    def run(self):
        while True:
            df_subset, i = self.queue.get()
            try:
                j = 0
                for index,r in df_subset.iterrows():
                    if(j%10 == 0):
                        print(str(i)+" - "+str(j))
                    j = j + 1
                    record = {}
                    record = r
                    req = urllib.request.Request(r['url'], method='HEAD')
                    try:
                        f = urllib.request.urlopen(req, timeout=1)
                        if((f.status != None) & (f.status == 200)):
                            if(f.headers['Content-Type'] != None):
                                record['resource_mime_python'] = f.headers['Content-Type']
                            else:
                                record['resource_mime_python'] = None
                        record['reachable'] = True
                    except Exception as e:
                        record['resource_mime_python'] = None
                        record['reachable'] = False
                    records.append(record)
                    del record
                print(str(i)+" Terminated")
            finally:
                self.queue.task_done()

def filter_mimes_to_send(df, today):
    try:
        dfmime = pd.read_csv("./utils/mimes.csv")
        dfmime = dfmime.rename(columns={'extension':'catalog_extension'})
        df = pd.merge(df,dfmime,on='mime',how='left')
        dfmime = dfmime.rename(columns={'mime':'resource_mime_python','catalog_extension':'python_extension'})
        df = pd.merge(df,dfmime,on='resource_mime_python',how='left')
        df['url_extension'] = df['url'].apply(lambda x: None if x != x else str(x).split('/')[-1].split('.')[1] if(len(str(x).split('/')[-1].split('.')) > 1) else None)
        #df[(df['catalog_extension'] == 'csv') | (df['python_extension'] == 'csv') | (df['url_extension'] == 'csv') | (df['catalog_extension'] == 'xls') | (df['python_extension'] == 'xls') | (df['url_extension'] == 'xls') | (df['catalog_extension'] == 'zip') | (df['python_extension'] == 'zip') | (df['url_extension'] == 'zip')]
        df = df[(df['python_extension'] == 'csv') | (df['url_extension'] == 'csv') | (df['python_extension'] == 'xls') | (df['url_extension'] == 'xls') | (df['python_extension'] == 'zip') | (df['url_extension'] == 'zip')]
        df.to_csv("./catalogs/"+today+"-check.csv")
        click.echo("Filter Catalok OK. Saved.")
    except Exception as e:
        click.echo("Filter resources to send KO. Bye!")
        exit()



def check_mime_from_header(df):
    chunk_size = int(df.shape[0] / 100)
    queue = Queue()
    i = 0
    for x in range(100):
        worker = DownloadWorker(queue)
        worker.daemon = True
        worker.start()
    for start in range(0, df.shape[0], chunk_size):
        i = i + 1
        df_subset = df.iloc[start:start + chunk_size]
        queue.put((df_subset, i))
    queue.join()

def filterCatalog(today):
    try:
        df = pd.read_csv("./catalogs/"+today+".csv",sep=";")
        df = df[df['downloads'] > 10]
        check_mime_from_header(df)
        print(df.shape[0])
        df = pd.DataFrame(records)
        filter_mimes_to_send(df, today)
    except Exception as e:
        click.echo("Filter Catalog KO. Bye!")
        exit()


def downloadCatalog(today):
    try:
        url = 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'
        urllib.request.urlretrieve(url, './catalogs/'+today+'.csv')
        click.echo("Download Catalog OK.")
    except Exception as e:
        click.echo("Download Catalog KO. Bye!")
        exit()

def findDay(date): 
    year, month, day = (int(i) for i in date.split('-'))     
    dayNumber = calendar.weekday(year, month, day) 
    # 0 = Lundi
    return (dayNumber) 

def record_run(count):
    table = db["runs"]
    table.insert({
        "nb_resources": count,
        "nb_resources_ignored": 0,
        "date": datetime.now()
    })


def send_to_linkproxy(url):
    click.echo(url)
    click.echo("--")
    r = requests.post("http://localhost:5010", json={
        "location": url
    })
    r.raise_for_status()
    return r.json()


def manageResourcesToSend(today):
    df = pd.read_csv("catalogs/"+today+"-check.csv")
    df = df.sample(frac=1).reset_index(drop=True)
    record_run(df.shape[0])
    table = db["checks"]
    i = 0
    for index,resource in df.iterrows():
        i = i + 1
        try:
            res = send_to_linkproxy(resource["url"])
        except requests.HTTPError as e:
            click.secho(f"Error while creating check: {e}", err=True, fg="red")
        else:
            existing = table.find_one(check_id=res["_id"], dataset_id=resource["dataset.id"], resource_id=resource["id"])
            data = {
                "check_id": res["_id"],
                "modified_at": datetime.now(),
                "dataset_id": resource["dataset.id"],
                "resource_id": resource["id"],
                "url": resource["url"],
                "dataset_title":resource["dataset.title"],
                "resource_title":resource["title"]
            }
            if not existing:
                data["created_at"] = datetime.now()
                table.insert(data)
            else:
                table.update(data, ["check_id", "resource_id", "dataset_id"])

@cli.command()
def run():
    todaydate = date.today()
    today = todaydate.strftime("%Y-%m-%d")
    # To change tout les samedi
    if findDay(today) == 6:
        click.echo("Already downloaded!")
        downloadCatalog(today)
    else:
        click.echo("Not a good day. Bye!")
        exit()   
    filterCatalog(today)
    manageResourcesToSend(today)
    click.echo("Sent to linkproxy OK.")

if __name__ == "__main__":
    cli()
