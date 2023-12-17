from datetime import datetime # 스케쥴링을 위해 시간 정보가 필요함
from airflow import DAG

from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pandas as pd

default_args = {
    "start_date" : datetime(2023, 11, 1)
}

KAKAO_API_KEY="96e4f824da9ff6f39eb707dbf075dfe7"

def preprocessing(ti):
    # ti : task instance. DAG 내의 TASK 정보를 얻어 낼 수 있는 객체
    search_result = ti.xcom_pull(task_ids=["extract_kakao"])
    
    print("================PREPROCESSING XCOM=================")
    print(search_result[0]['documents'])
    
    documents = search_result[0]['documents']
    df = pd.DataFrame(documents)
    
    df.to_csv("/home/ubuntu/airflow/dags/processed_result.csv", index=None, header=False)

with DAG(
    dag_id = "kakao-pipeline",
    schedule_interval = "@daily",
    default_args = default_args,
    tags = ["kakao","api","pipeline"],
    catchup = False # 11/1부터 21까지 20일의 공백 시간이 있는데 20일까지의 작업을 없는걸로 침.
) as dag:
    
    # 테이블 생성
    creating_table = SqliteOperator(
        task_id="creating_table",
        sqlite_conn_id="sqlite_db",
        sql="""
            CREATE TABLE IF NOT EXISTS kakao_search_result(
                created_at TEXT,
                contents TEXT,
                title TEXT,
                URL TEXT
            )
        """
    )
    
    # HTTPSensor를 이용하여 해당 api에 접속이 가능한지 확인
    is_api_available = HttpSensor(
        task_id = "is_api_available",
        http_conn_id = "kakao_api",
        endpoint = "v2/search/web",
        headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"},
        request_params = {"query":"2023 롤드컵 결승"},
        response_check = lambda response : response.json()
    )
    
    # response_check의 함수는 "반드시" 리턴(ex.response.json())을 해야 한다.
    # 리턴이 안되면 무한 대기하게 됨. (응답이 될 때 까지 무한 대기)
    
    # 실제 HTTP 요청 후 데이터 받아오기
    extract_kakao = SimpleHttpOperator(
        task_id = "extract_kakao",
        http_conn_id = "kakao_api",
        endpoint = "v2/search/web",
        headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"},
        data = {"query":"2023 롤드컵 결승"},
        method = "GET",
        response_filter = lambda res : res.json(),
        log_response = True
    )
    
    preprocess_result = PythonOperator(
        task_id = "preprocess_result",
        python_callable = preprocessing  # 핵심!
        
    )
    
    # BashOperator : 리눅스 시스템에서 사용하는 명령어를 실행하기 위함
    #  데이터를 다루기 위햇 사용하는건 아님.
    store_result = BashOperator(
        task_id = 'store_result',
        bash_command = 'echo -e ".separator ","\n.import /home/ubuntu/airflow/dags/processed_result.csv kakao_search_result" | sqlite3 /home/ubuntu/airflow/airflow.db'
    )
    
    # Operator를 엮어서 파이프라인화 시키기
    creating_table >> is_api_available >> extract_kakao >> preprocess_result >> store_result