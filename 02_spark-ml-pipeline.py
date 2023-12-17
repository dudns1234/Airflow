from datetime import datetime
from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # 잡제출

default_args = {
    "start_date" : datetime(2023,11,1)
    
}

with DAG(
    dag_id = "spark-ml-pipeline",
    schedule_interval = "@daily",
    default_args = default_args,
    tags = ["spark","ml","taxi"],
    catchup = False
) as dag:
    
    preprocess = SparkSubmitOperator(
        task_id = "preprocess",
        conn_id = "spark_local",
        application = "/home/ubuntu/airflow/dags/03_preprocess.py"
    )
    
    tune_hyperparameter = SparkSubmitOperator(
        task_id = 'tune_hyperparameter',
        conn_id = "spark_local",
        application = "/home/ubuntu/airflow/dags/04_tune_hyperparameter.py"
    )
    
    train_model = SparkSubmitOperator(
        task_id = "train_model",
        conn_id = "spark_local",
        application = "/home/ubuntu/airflow/dags/05_train_model.py"
    )
    
    preprocess >> tune_hyperparameter >> train_model