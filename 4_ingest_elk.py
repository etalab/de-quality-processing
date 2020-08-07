import requests, json, os
from elasticsearch import Elasticsearch
import configparser
import datetime
import sys
import pandas as pd
import dataset as dataset_lib

config = configparser.ConfigParser()
config.read('./config.ini')


db = dataset_lib.connect("sqlite:///orchestrator.db")

if len(sys.argv) > 1:
    daytoprocess = sys.argv[1]
else:
    daytoprocess = str(datetime.datetime.today()).split()[0]


url_es = config['ELK']['HOST']
port_es = config['ELK']['PORT']
user_es = config['ELK']['USERNAME']
pwd_es = config['ELK']['PASSWORD']



es = Elasticsearch(
    [url_es],
    port=port_es,
    http_auth=(user_es, pwd_es)
)

cats = ['siret',
 'commune',
 'date',
 'url',
 'latitude_wgs',
 'json_geojson',
 'booleen',
 'code_commune_insee',
 'latlon_wgs',
 'code_departement',
 'latitude_l93',
 'adresse',
 'code_postal',
 'tel_fr',
 'email',
 'year',
 'code_region',
 'sexe',
 'siren',
 'code_waldec',
 'longitude_l93',
 'pays',
 'departement',
 'uai',
 'region',
 'iso_country_code',
 'date_fr',
 'insee_ape700',
 'code_rna',
 'code_fantoir',
 'datetime_iso',
 'longitude_wgs',
 'insee_canton',
 'csp_insee',
 'jour_de_la_semaine',
 'code_csp_insee',
 'twitter']

with open("/srv/datamanufactory/data-workflow/csv-detective-results/"+daytoprocess+".json", "r") as filo:
    jsonfile = json.load(filo)

table = db["checks"]

for i in range(len(jsonfile)):

    el = []
    jsonfile[i][1]['dataset_id'] = jsonfile[i][0].split("---")[0]
    jsonfile[i][1]['resource_id'] = jsonfile[i][0].split("---")[1].split('.')[0]
    print(i,end=" - ")
    print(jsonfile[i][1]['resource_id'])

    
    existing = table.find_one(dataset_id=jsonfile[i][1]['dataset_id'], resource_id=jsonfile[i][1]['resource_id'], order_by='-created_at')

    jsonfile[i][1]['title'] = existing['resource_title']
    jsonfile[i][1]['resource_url'] = existing['url']
    # a corriger : 
    jsonfile[i][1]['dataset_title'] = existing['dataset_title']


    if "columns_ml" in jsonfile[i][1]:
        jsonfile[i][1].pop("columns_ml")
        print("columns ml removed")

    if "columns" in jsonfile[i][1]:
        arr2 = []
        for key in jsonfile[i][1]['columns']:
            arr = []
            for m in (range(len(jsonfile[i][1]['columns'][key]))):
                if((float(jsonfile[i][1]['columns'][key][m]['score_ml']) < 0.1) & (float(jsonfile[i][1]['columns'][key][m]['score_rb']) < 0.1)):
                    arr.append(m)
            if(len(arr) == len(jsonfile[i][1]['columns'][key])):
                arr2.append(key)
            else:
                arr.sort(reverse=True)
                if(arr is not None):
                    for a in arr:
                        jsonfile[i][1]['columns'][key].pop(a)
        arr2.sort(reverse=True)
        if(arr2 is not None):
            for a in arr2:
                jsonfile[i][1]['columns'].pop(a)


    el = jsonfile[i][1]

    el['ingested_date'] = daytoprocess
    


    docket_content = json.dumps(el)
    print(docket_content)

    #es.delete_by_query(index='csvresource', body={"query": {"bool": {"must": [{ "match": { "dataset_id": el['dataset_id']}}, { "match": { "resource_id": el['resource_id']}}]}, "must_not" : [{ "match" : { "ingested_date" : "tototo" }}]}})

    es.index(index='csvmetadtgv', ignore=400, body=json.loads(docket_content))
    print("------")

print("import into ELK ok")
