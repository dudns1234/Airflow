# Spark 작업이 들어갈 어플리케이션
## airflow와는 연관이 없음!!
# 아래 내용 다 노트북으로 했던 것임. 원래 주피터로 하는거 아닌데 흐름/확인해 보려고 했던 것!!
## 노트북으로 초벌하고 완성된 걸 파이썬 파일로 진행하는 것과 같음!!

# 1. 전처리를 위한 컴포넌트임 !!!!
from pyspark.sql import SparkSession

MAX_MEMEORY = '5g'
spark = (
        SparkSession.builder.appName("taxi-fare-prediction")
            .config("spark.executor.memory", MAX_MEMEORY)
            .config("spark.driver.memory", MAX_MEMEORY)
            .getOrCreate())

directory = "/home/ubuntu/working/datasource"
trip_files = "/trips/*"

trips_df = spark.read.csv(f"file:///{directory}/{trip_files}", inferSchema=True, header=True)
trips_df.createOrReplaceTempView("trips") # trips_df 등록!

# 데이터 정제
query = """
SELECT
    t.passenger_count,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    t.trip_distance,
    HOUR(tpep_pickup_datetime) as pickup_time,
    DATE_FORMAT(TO_DATE(tpep_pickup_datetime), 'EEEE') as day_of_week,
    
    t.total_amount

FROM trips t

WHERE t.total_amount < 200
  AND t.total_amount > 0
  AND t.passenger_count < 5
  AND TO_DATE(t.tpep_pickup_datetime) >= '2021-01-01'
  AND TO_DATE(t.tpep_pickup_datetime) < '2021-08-01'
  AND t.trip_distance < 10
  AND t.trip_distance > 0
"""

# 정제 쿼리 실행
data_df = spark.sql(query) # transformation까지만 해주기. action하면 오래걸리기 때문

# 훈련 / 테스트 세트 분리
train_sdf, test_sdf = data_df.randomSplit([0.8,0.2],seed=42)

# cache()를 안하는 이유?
## 지금은 preprocess 단계이기 때문에!!

# 훈련 / 테스트 데이터를 저장
data_dir = "/home/ubuntu/airflow/ml-data"

train_sdf.write.format("parquet").mode("overwrite").save(f"{data_dir}/train/")
test_sdf.write.format("parquet").mode("overwrite").save(f"{data_dir}/test/")

spark.stop()

# 실행 방법
# python 03.preprocess.py -> 테스트 실행
# spark-submit -> 정석 실행