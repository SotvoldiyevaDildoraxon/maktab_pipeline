from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Spark Session yaratish
spark = SparkSession.builder \
    .appName("Maktab Big Data Pipeline") \
    .getOrCreate()

print("Spark ishga tushdi")

# CSV ni o‘qish
df = spark.read.csv(
    "data/raw/Umumta’lim muassasalarida o‘quvchilar soni (jami).csv",
    header=True,
    inferSchema=True
)

print("Schema:")
df.printSchema()

print("5 qator:")
df.show(5)

# Filter qilish (misol uchun 2024 yil)
if "2024" in df.columns:
    df_2024 = df.select("Klassifikator", col("2024").alias("value_2024"))
    df_2024.show(5)

# Aggregation (masalan 2024 jami)
if "2024" in df.columns:
    total_2024 = df.select(sum(col("2024"))).collect()
    print("2024 Jami:", total_2024)

# Parquet ga yozish (Data Lake Silver)
df.write.mode("overwrite").parquet("data/silver/spark_output")

print("Parquet yozildi")

spark.stop()