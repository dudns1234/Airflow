from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

# feature 준비
from pyspark.ml.feature import OneHotEncoder, StringIndexer # 범주형 데이터를 다루기 위함
from pyspark.ml.feature import VectorAssembler, StandardScaler # 수치형 데이터를 다루기 위함

# Model
from pyspark.ml.regression import LinearRegression

# Validate, Tuning은 필요 없다! 이미 효과가 좋았던 하이퍼 파라미터를 tune_hypermeters에서 구해 놨다!
# 검증을 할 필요가 없다!!!
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.ml.evaluation import RegressionEvaluator

MAX_MEMORY = "5g"
spark = SparkSession.builder.appName("taxi-fare-prediction")\
            .config("spark.executor.memory", MAX_MEMORY)\
            .config("spark.driver.memory", MAX_MEMORY)\
            .getOrCreate()

# 훈련, 테스트 데이터 불러오기
data_dir = "/home/ubuntu/airflow/ml-data"
train_df = spark.read.parquet(f"{data_dir}/train/")
test_df  = spark.read.parquet(f"{data_dir}/test/")

# 구해 놨던 하이퍼 파라미터 가져오기
import pandas as pd
hyper_df = pd.read_csv(f"{data_dir}/hyperparameter.csv")

alpha = float(hyper_df.iloc[0]['alpha'])
reg_param = float(hyper_df.iloc[0]['reg_param'])

# Spark ML 파이프라인 구성하기
stages = []

# 범주형 데이터에 대한 파이프라인 구성
# StringIndexer => OneHotEncoder
cat_features = [
    "pickup_location_id",
    "dropoff_location_id",
    "day_of_week"
]

for c in cat_features:
    cat_indexer = StringIndexer(inputCol=c, outputCol=c+"_idx").setHandleInvalid("keep")
    onehot_encoder = OneHotEncoder(inputCols=[cat_indexer.getOutputCol()], outputCols=[c+"_onehot"])
    stages += [cat_indexer, onehot_encoder]


# 수치형 데이터들에 대한 처리
# 각 열을 모아서(VectorAssembler) => 표준화(StandardScaler)
num_features = [
    "passenger_count",
    "trip_distance",
    "pickup_time"
]

for n in num_features:
    num_assembler = VectorAssembler(inputCols=[n], outputCol=n+"_vector")
    num_scaler = StandardScaler(inputCol=num_assembler.getOutputCol(), outputCol=n+"_scaled")
    stages += [num_assembler, num_scaler]

# 훈련 데이터(feature vector)를 만들기 위한 실제로 사용할 데이터에 대한 Assemble
assembler_input = [c + "_onehot" for c in cat_features] + [n+"_scaled" for n in num_features]
assembler = VectorAssembler(inputCols=assembler_input, outputCol="features")
stages += [assembler]

# 모델은 tune_hyperparameter에서 구한 하이퍼 파라미터를 구해서 따로 만들어서 저장

# 오로지 데이터 변환(Transformation)만을 위한 파이프라인 생성
pipeline = Pipeline(stages=stages)

# 데이터 변환
fitted_transformer = pipeline.fit(train_df) # Transformation 작업
vec_train_df = fitted_transformer.transform(train_df) # Action

# 모델 정의
lr = LinearRegression(
    maxIter=50,
    solver='normal',
    labelCol='total_amount',
    featuresCol = 'features',

    # GridSearch로 구한 파라미터 넣어주기
    elasticNetParam=alpha,
    regParam=reg_param
)

# 모델 객체 생성은 훈련(fit)이 끝나고 만들어 진다.
model = lr.fit(vec_train_df)

# 테스트 세트 예측
vec_test_df = fitted_transformer.transform(test_df)
predictions = model.transform(vec_test_df)
predictions.select(["trip_distance", "day_of_week", "total_amount", "prediction"]).show()

# 모델 저장
model_dir = "/home/ubuntu/airflow/ml-data/model"
model.write().overwrite().save(model_dir)