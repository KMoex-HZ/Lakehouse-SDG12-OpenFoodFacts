from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kelompok13',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='lakehouse_sdg12_pipeline',
    default_args=default_args,
    description='Pipeline Lakehouse SDG12 — Bronze → Silver → Gold → ML',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'sdg12', 'spark', 'delta-lake'],
) as dag:

    bronze_task = BashOperator(
        task_id='bronze_layer',
        bash_command='docker exec -e PYTHONPATH=/usr/local/spark-3.5.1-bin-hadoop3/python:/usr/local/spark-3.5.1-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip spark-jupyter /opt/conda/bin/python /home/jovyan/work/scripts/bronze_layer.py',
    )

    silver_task = BashOperator(
        task_id='silver_layer',
        bash_command='docker exec -e PYTHONPATH=/usr/local/spark-3.5.1-bin-hadoop3/python:/usr/local/spark-3.5.1-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip spark-jupyter /opt/conda/bin/python /home/jovyan/work/scripts/silver_layer.py',
    )

    gold_task = BashOperator(
        task_id='gold_layer',
        bash_command='docker exec -e PYTHONPATH=/usr/local/spark-3.5.1-bin-hadoop3/python:/usr/local/spark-3.5.1-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip spark-jupyter /opt/conda/bin/python /home/jovyan/work/scripts/gold_layer.py',
    )

    ml_task = BashOperator(
        task_id='ml_layer',
        bash_command='docker exec -e PYTHONPATH=/usr/local/spark-3.5.1-bin-hadoop3/python:/usr/local/spark-3.5.1-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip spark-jupyter /opt/conda/bin/python /home/jovyan/work/scripts/ml_layer.py',
    )

    bronze_task >> silver_task >> gold_task >> ml_task