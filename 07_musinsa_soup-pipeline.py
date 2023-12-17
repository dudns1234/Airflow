from datetime import datetime
from airflow import DAG


from crawler.musinsa_examples import get_musinsa_product
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import pandas as pd
def save_result(ti):
    musinsa_product_data = ti.xcom_pull(task_ids=["musinsa_product_result"])
    
    # 예외처리
    if not len(musinsa_product_data):
        raise ValueError("없음")

    result_df = pd.DataFrame(musinsa_product_data[0])
    
    now_time = datetime.now().strftime("%y%m%d_%H")
    result_df.to_csv(f"/home/ubuntu/airflow/dags/musinsa_result_{now_time}.csv", index=None, header=True)

default_args = {
    "start_date" : datetime(2023,11,21)
}

with DAG(
    dag_id = "musinsa-crawling-pipeline",
    schedule_interval = "@hourly",
    default_args = default_args,
    tags = ["musinsa", "crawling"],
    catchup = False
) as dag:
    
    # Task1. 스크래이핑
    musinsa_product_result = PythonOperator(
        task_id = "musinsa_product_result",
        python_callable = get_musinsa_product
    )
    # Task2. csv 저장
    musina_product_save = PythonOperator(
        task_id = "musina_product_save",
        python_callable = save_result
    )

    # Task1, 2 연결
    # musinsa_product_result >> musina_product_save
    
    # Task3. 브랜드별 가격 평균 집계 (Pandas)
    musinsa_agg = SparkSubmitOperator(
        task_id = "musinsa_agg",
        conn_id = "spark_local",
        application = "/home/ubuntu/airflow/dags/08_musinsa_agg.py"
    )
    
    # Task1, 2, 3 연결
    musinsa_product_result >> musina_product_save >> musinsa_agg
    
# Task4. 집계 결과 CSV 저장 -> 흠..?