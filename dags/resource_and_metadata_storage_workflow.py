from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'geoffrey.aldebert',
    'depends_on_past': False,
    'email': ['geoffrey.aldebert@data.gouv.fr'],
    'start_date': datetime(2020, 7, 3, 7, 42, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

dag = DAG('dtgv_resource_and_metadata_storage', default_args=default_args, schedule_interval=timedelta(days=1))

t0 = BashOperator(
    task_id='remove_tmp_plunger_folders',
    bash_command='rm -rf /tmp/plunger_*',
    dag=dag)

t1 = BashOperator(
    task_id='send_to_linkproxy',
    bash_command='su datamanufactory -c "cd /srv/datamanufactory/data-workflow/ && /anaconda3/bin/python 1_send_resources_to_linkproxy.py run"',
    dag=dag)


t1bis = BashOperator(
    task_id='send_to_linkproxy_weekly_catalog',
    bash_command='su datamanufactory -c "cd /srv/datamanufactory/data-workflow/ && /anaconda3/bin/python 1bis_send_catalog_to_linkproxy.py run"',
    dag=dag)

t2 = BashOperator(
    task_id='wait_webhook_to_hook',
    bash_command='su datamanufactory -c "sleep 9200"',
    dag=dag)

t3 = BashOperator(
    task_id='csv_detective_analysis',
    bash_command='su datamanufactory -c "source /anaconda3/etc/profile.d/conda.sh && cd /srv/datamanufactory/data-workflow/ && conda activate csvdeploy && python 3_csv_detective_analysis.py run"',
    dag=dag)


t4 = BashOperator(
    task_id='send_metadata_to_elk',
    bash_command='su datamanufactory -c "cd /srv/datamanufactory/data-workflow/ && /anaconda3/bin/python 4_ingest_elk.py"',
    dag=dag)


t1.set_upstream(t0)
t1bis.set_upstream(t0)
t2.set_upstream(t1)
t2.set_upstream(t1bis)
t3.set_upstream(t2)
t4.set_upstream(t3)
