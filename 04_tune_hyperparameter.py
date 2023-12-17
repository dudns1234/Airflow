# 어떠한 모델의 최적의 하이퍼파라미터를 구하는게 목적
## crossvalidation ... 등이 있어야함.

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

# feature 준비
from pyspark.ml.feature import OneHotEncoder, StringIndexer # 범주형 데이터 다루기
from pyspark.ml.feature import StandardScaler, VectorAssembler

# Model
from pyspark.ml.regression import LinearRegression

# 검증, 튜닝
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

MAX_MEMEORY = '5g'
spark = (
        SparkSession.builder.appName("taxi-fare-prediction")
            .config("spark.executor.memory", MAX_MEMEORY)
            .config("spark.driver.memory", MAX_MEMEORY)
            .getOrCreate())

data_dir = "/home/ubuntu/airflow/ml-data"

train_df = spark.read.parquet(f"{data_dir}/train/")
toy_df = train_df.sample(False, 0.01, seed=42) # 전체 데이터가 아닌 1%의 데이터만 사용

# Spark ML 파이프라인 구성
stages = []

# 1. 범주형 데이터에 대한 파이프라인 구성
cat_features = [
    "pickup_location_id",
    "dropoff_location_id",
    "day_of_week"
]

for c in cat_features:
    cat_indexer = StringIndexer(inputCol=c, outputCol=c+"_idx").setHandleInvalid("keep")
    onehot_encoder = OneHotEncoder(inputCols=[cat_indexer.getOutputCol()], outputCols=[c+"_onehot"])
    stages += [cat_indexer, onehot_encoder]
    

# 2. 수치형 데이터에 대한 파이프라인 구성
num_features = [
    "passenger_count",
    "trip_distance",
    "pickup_time"
]

for n in num_features:
    num_assembler = VectorAssembler(inputCols=[n], outputCol=n+"_vector")
    num_scaler = StandardScaler(inputCol=num_assembler.getOutputCol(), outputCol=n+"_scaled")
    stages += [num_assembler, num_scaler]
    
# 훈련 데이터(Feature Vector)를 만들기 위한 실제로 사용할 데이터에 대해 Assemble
assembler_input = [c + "_onehot" for c in cat_features] + [n + "_scaled" for n in num_features]
assembler = VectorAssembler(inputCols=assembler_input, outputCol="features")
stages += [assembler]

# 모델 생성
lr = LinearRegression(
    maxIter=30,
    solver='normal',
    labelCol="total_amount",
    featuresCol="features"
)

stages += [lr]

# 파이프라인 생성
pipeline = Pipeline(stages=stages)

# 검사할 하이퍼 파라미터 등록
param_grid = ParamGridBuilder()\
                .addGrid(lr.elasticNetParam, [0.1, 0.2, 0.3, 0.4, 0.5])\
                .addGrid(lr.regParam, [0.1, 0.2, 0.3, 0.4, 0.5])\
                .build()

# 교차 검증 계획 수립
cross_val = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=RegressionEvaluator(labelCol="total_amount"),
    numFolds=5
)

# 훈련
cv_model = cross_val.fit(toy_df)

# 훈련이 끝났으면 성능이 제일 좋았던 모델 가져오기
best_model = cv_model.bestModel.stages[-1]

# 성능이 가장 뛰어난 모델로 부터 최적의 하이퍼 파라미터 가져오기
bestElasticNetParam_alpha = best_model._java_obj.getElasticNetParam()
bestRegParam = best_model._java_obj.getRegParam()

hyper_parameters = {
    "alpha": [bestElasticNetParam_alpha],
    "reg_param": [bestRegParam]
}

import pandas as pd
hyper_df = pd.DataFrame(hyper_parameters)
hyper_df.to_csv(f"{data_dir}/hyperparameter.csv") # 하이퍼파라미터 저장

spark.stop()