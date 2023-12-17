from pyspark.sql import SparkSession
from datetime import datetime

MAX_MEMEORY = '5g'
spark = (
        SparkSession.builder.appName("taxi-fare-prediction")
            .config("spark.executor.memory", MAX_MEMEORY)
            .config("spark.driver.memory", MAX_MEMEORY)
            .getOrCreate())

musinsa_result_dir = "/home/ubuntu/airflow/dags/musinsa_result_231121/13/*.ㅊㄴㅍ"

musinsa_df = spark.read.csv(f"file:///{musinsa_result_dir}", inferSchema=True, header=True)
# musinsa_df.show()

musinsa_df.createOrReplaceTempView("musinsa")

query = """

    SELECT 
        product_title, 
        count(*) CNT,
        max(product_price) max_price,
        min(product_price) min_price,
        avg(product_price) avg_price
    FROM musinsa
    GROUP BY product_title

"""
# 집계 확인
musinsa_agg_sdf = spark.sql(query)

# 데이터 저장

data_dir = "/home/ubuntu/airflow/dags"
now_time = datetime.now().strftime("%y%m%d_%H")

musinsa_agg_df = musinsa_agg_sdf.toPandas()
musinsa_agg_df.to_csv(f"{data_dir}/musinsa_agg_{now_time}.csv", index=False)

spark.stop()