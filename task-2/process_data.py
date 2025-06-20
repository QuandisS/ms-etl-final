from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, DoubleType, StringType, ByteType
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("Transactions_v2 ETL with Logging to S3").getOrCreate()

source_path = "s3a://ms-etl-final/raw-data/transactions_v2.csv"
target_path = "s3a://ms-etl-final/processed-data/transactions_v2-processed.parquet"

try:
    print(f"Чтение данных из: {source_path}")
    df = spark.read.option("header", "true").csv(source_path)

    print("Схема исходных данных:")
    df.printSchema()

    # Приведение типов согласно структуре таблицы
    df = df.withColumn("id", col("id").cast(IntegerType())) \
           .withColumn("step", col("step").cast(IntegerType())) \
           .withColumn("type", col("type").cast(StringType())) \
           .withColumn("amount", col("amount").cast(DoubleType())) \
           .withColumn("nameOrig", col("nameOrig").cast(StringType())) \
           .withColumn("oldbalanceOrig", col("oldbalanceOrig").cast(DoubleType())) \
           .withColumn("newbalanceOrig", col("newbalanceOrig").cast(DoubleType())) \
           .withColumn("isFraud", col("isFraud").cast(ByteType())) \
           .withColumn("isFlaggedFraud", col("isFlaggedFraud").cast(ByteType()))

    print("Схема после приведения типов:")
    df.printSchema()

    # удаляем строки с пропущенными обязательными полями
    df = df.na.drop(subset=["id", "step", "type", "amount", "nameOrig", "oldbalanceOrig", "newbalanceOrig", "isFraud", "isFlaggedFraud"])

    # если oldbalanceOrig < 0, считаем это ошибкой и ставим null
    df = df.withColumn("oldbalanceOrig", when(col("oldbalanceOrig") < 0, None).otherwise(col("oldbalanceOrig")))

    print("Пример данных после обработки:")
    df.show(5, truncate=False)

    print(f"Запись в Parquet: {target_path}")
    df.write.mode("overwrite").parquet(target_path)

    print("Данные сохранены в Parquet.")

except Exception as e:
    print("Ошибка:", e)

spark.stop()
