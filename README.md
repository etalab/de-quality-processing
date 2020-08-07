# Data Manufactory workflow

Prerequisite : a running Linkproxy

Airflow dag to orchestrate these scripts : dag/resource_and_metadata_storage_workflow.py

## Send to Linkproxy
File : 1_send_resources_to_linkproxy

Get the latest modified datasets/resources from data.gouv.fr and send a check to link-proxy. (hosted in another server)

Keeps tab on when the script has been run and which checks have been created.

```
python orchestrator.py run
```

#### Questions

- how are plunger/link-proxy temporary files handled? looks like it's filling up /tmp/ with discarded files.

## Webhook collector
File : 2_flask_subscriber_to_linkproxy

Prerequisite : Subscribe to linkproxy webhook with correct url

Linkproxy send a message for every document that it is storing into Minio. This flask app only consists to wait for new messages from linkproxy and store it locally.

```
python 2_flask_subscriber_to_linkproxy.py
```

## CSV Dective Analysis
File : 3_csv_detective_analysis

Once documents are stored locally, this script download and runs every new resources into csvdetective to extract metadata.

```
python 3_csv_detective_analysis.py
```

## Send metadata to ELK
File : 4_ingest_elk

Once metadata is collected, this script send results to an elasticsearch.

```
python 4_ingest_elk.py
```

Mapping for elk : 
``` PUT /csvmetadtgv PUT /csvmetadtgv/_mapping { "properties": { "encoding": { "type": "text"}, "separator": { "type": "text"}, 
    "header_row_idx": { "type": "text"}, "header": { "type": "keyword"}, "total_lines": { "type": "text"}, "heading_columns": { "type": "text"}, 
    "trailing_columns": { "type": "text"}, "ints_as_floats": { "type": "text"}, "continuous": { "type": "text"}, "categorical": { "type": "text"}, 
    "dataset_id": { "type": "keyword"}, "dataset_title": { "type": "keyword"}, "resource_id": { "type": "text"}, "title": { "type": "text"}, "resource_url": { 
    "type": "text"}, "columns": {
      "properties": { "siret": { "type":"nested"}, "commune": { "type":"nested"}, "date": { "type":"nested"}, "url": { "type":"nested"}, "latitude_wgs": { 
        "type":"nested"}, "json_geojson": { "type":"nested"}, "booleen": { "type":"nested"}, "code_commune_insee": { "type":"nested"}, "latlon_wgs": { 
        "type":"nested"}, "code_departement": { "type":"nested"}, "latitude_l93": { "type":"nested"}, "adresse": { "type":"nested"}, "code_postal": { 
        "type":"nested"}, "tel_fr": { "type":"nested"}, "email": { "type":"nested"}, "year": { "type":"nested"}, "code_region": { "type":"nested"}, "sexe": { 
        "type":"nested"}, "siren": { "type":"nested"}, "code_waldec": { "type":"nested"}, "longitude_l93": { "type":"nested"}, "pays": { "type":"nested"}, 
        "departement": { "type":"nested"}, "uai": { "type":"nested"}, "region": { "type":"nested"}, "iso_country_code": { "type":"nested"}, "date_fr": { 
        "type":"nested"}, "insee_ape700": { "type":"nested"}, "code_rna": { "type":"nested"}, "code_fantoir": { "type":"nested"}, "datetime_iso": { 
        "type":"nested"}, "longitude_wgs": { "type":"nested"}, "insee_canton": { "type":"nested"}, "csp_insee": { "type":"nested"}, "jour_de_la_semaine": { 
        "type":"nested"}, "code_csp_insee": { "type":"nested"}, "twitter": { "type":"nested"}
      }
    }
  }
}
```

