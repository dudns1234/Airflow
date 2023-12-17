from datetime import datetime
from airflow import DAG

from crawler.naver_examples import get_naver_finance

# Python Operator - 파이썬 코드를 실행하기 위한 오퍼레이터
from airflow.operators.python import PythonOperator

import pandas as pd

default_args = {
    "start_date" : datetime(2023,11,21)
}

def preprocessing_result(ti):
    naver_finance_data = ti.xcom_pull(task_ids=["naver_finance_result"])
    
    # 예외 처리
    if not len(naver_finance_data):
        raise ValueError("검색 결과가 없습니다.")
    
    result_df = pd.DataFrame(naver_finance_data[0])
    
    now_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    result_df.to_csv(f"/home/ubuntu/airflow/dags/naver_result_{now_time}.csv", index=None, header=True)
    
with DAG(
    dag_id = "naver-finance-pipeline",
    schedule_interval = "@hourly",
    default_args = default_args,
    tags = ["naver", "soup"],
    catchup = False
) as dag:
    
    # 1. 네이버 증권 정보에서 환율 크롤링
    naver_finance_result = PythonOperator(
        task_id = "naver_finance_result",
        python_callable = get_naver_finance
    )
    
    # 2. 저장
    naver_finance_save = PythonOperator(
        task_id = "naver_finance_save",
        python_callable = preprocessing_result
    )
    
    # 2개의 task 연결!!!
    naver_finance_result >> naver_finance_save