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

with open("/srv/datamanufactory/data-workflow/csvdetective/analysis_results/"+daytoprocess+".json", "r") as filo:
    jsonfile = json.load(filo)

table = db["checks"]

for i in range(len(jsonfile)):

    el = []
    jsonfile[i][1]['dataset_id'] = jsonfile[i][0].split("---")[0]
    jsonfile[i][1]['resource_id'] = jsonfile[i][0].split("---")[1].split('.')[0]
    print(i,end=" - ")

    
    existing = table.find_one(dataset_id=jsonfile[i][1]['dataset_id'], resource_id=jsonfile[i][1]['resource_id'], order_by='-created_at')

    jsonfile[i][1]['title'] = existing['resource_title']
    jsonfile[i][1]['resource_url'] = existing['url']
    # a corriger : 
    jsonfile[i][1]['dataset_title'] = existing['dataset_title']
    print(jsonfile[i][1]['resource_id'])
    el = jsonfile[i][1]
    
    el['columns_types'] = []
    for key in cats:
        el[key] = []
    if "columns_ml" in jsonfile[i][1]:
        for j in range(len(jsonfile[i][1]['columns_ml'])):
            for key in jsonfile[i][1]['columns_ml']:
                if jsonfile[i][1]['columns_ml'][key][0] in cats:
                    el[jsonfile[i][1]['columns_ml'][key][0]].append(key)
                    el['columns_types'].append(jsonfile[i][1]['columns_ml'][key][0])
            for key in jsonfile[i][1]['columns_rb']:
                if jsonfile[i][1]['columns_rb'][key][0] in cats:
                    el[jsonfile[i][1]['columns_rb'][key][0]].append(key)
                    el['columns_types'].append(jsonfile[i][1]['columns_rb'][key][0])

    el['columns_types'] = list(dict.fromkeys(el['columns_types']))
                    
    el.pop("columns_rb",None)
    el.pop("columns_ml",None)
    for key in cats:
        el[key] = list(dict.fromkeys(el[key]))
        if(len(el[key]) == 0):
            el.pop(key,None)
    el['ingested_date'] = daytoprocess
    
    docket_content = json.dumps(el)
    print(docket_content)

    #es.delete_by_query(index='csvresource', body={"query": {"bool": {"must": [{ "match": { "dataset_id": el['dataset_id']}}, { "match": { "resource_id": el['resource_id']}}]}, "must_not" : [{ "match" : { "ingested_date" : "tototo" }}]}})

    es.index(index='csvlinkproxy', ignore=400, body=json.loads(docket_content))
    print("------")

print("import into ELK ok")