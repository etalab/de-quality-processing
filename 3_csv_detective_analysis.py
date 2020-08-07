import dataset as dataset_lib
import click
import datetime
from os import listdir
import requests
import json
import subprocess
import urllib.request
import glob
import os

db = dataset_lib.connect("sqlite:///orchestrator.db")


@click.group()
def cli():
    pass


def find_json_filenames(path, suffix=".json" ):
    filenames = listdir(path)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

def save_tmp_resource(jsonfilepath):
    with open(jsonfilepath) as jsonfile:        
        try:
            webhookdata = json.load(jsonfile)
            linkId = webhookdata['check']['linkId']
            print(linkId)
            r = requests.get("http://localhost:5010/"+linkId)
            miniodata = r.json()
            resource_name = miniodata['downloads'][0]['files'][0]['name']
            resource_ext = miniodata['downloads'][0]['type']
            resource_url = miniodata['downloads'][0]['url']
            table = db["checks"]
            existing = table.find_one(check_id=linkId)
            resource_id = existing['resource_id']
            dataset_id = existing['dataset_id']
            print(resource_id+"--"+dataset_id)
            if(resource_ext == 'csv'):
                urllib.request.urlretrieve(resource_url, '/tmp/dataworkflow/'+dataset_id+'---'+resource_id+'.'+resource_ext)
        except:
            print("Error in download")

@cli.command()
def run():
    files = glob.glob('/tmp/dataworkflow/*')
    for f in files:
        os.remove(f)

    today = str(datetime.datetime.today()).split()[0]
    jsonfiles = find_json_filenames("static/"+today+"/")
    for jsonfile in jsonfiles:
        save_tmp_resource("static/"+today+"/"+jsonfile)
        
    subprocess.Popen("cd csv-detective-ml && python analyze_csv_cli.py /tmp/dataworkflow /srv/datamanufactory/data-workflow/csv-detective-results/ "+today+" --rb_ml_analysis=both", shell=True)

    #search every json file folder date today
    

if __name__ == "__main__":
    cli()

