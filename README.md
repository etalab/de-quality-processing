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