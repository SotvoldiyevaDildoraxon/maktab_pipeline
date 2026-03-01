from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .appName("Spark Transform") \
    .getOrCreate()

df = spark.read.csv(
    "data/raw/Umumta’lim muassasalarida o‘quvchilar soni (jami).csv",
    header=True,
    inferSchema=True
)

# Wide → Long (stack funksiyasi bilan)
year_columns = [c for c in df.columns if c.isdigit()]
n = len(year_columns)

stack_expr = "stack({}, {}) as (year, value)".format(
    n,
    ", ".join([f"'{c}', `{c}`" for c in year_columns])
)

df_long = df.selectExpr("Klassifikator as region", stack_expr)

df_long.show(10)

df_long.write.mode("overwrite").parquet("data/silver_long/spark_long/spark_to_long.parquet")

spark.stop()