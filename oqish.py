import pandas as pd
df=pd.read_csv("data/bronze/11-sinf bitiruvchilari (jami).csv")
print(df.head(10))
import pandas as pd
df1=pd.read_parquet("data/silver_long/11-sinf bitiruvchilari (jami).parquet")
print(df1.head(10))
df3=pd.read_parquet("data/gold/kpi_region_year.parquet")
print(df3.head(10))