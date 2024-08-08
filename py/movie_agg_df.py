from pyspark.sql import SparkSession
import sys

dt = sys.argv[1]
spark = SparkSession.builder.appName("agg_df").getOrCreate()

df = spark.read.parquet(f"/home/j25ng/data/movie/hive/load_dt={dt}")
df.show()
df_f = df['movieCd', 'multiMovieYn', 'repNationCd']
df_f.createOrReplaceTempView("movie")

df_m = spark.sql(f"SELECT * FROM movie WHERE multiMovieYn=='Y'")
m = df_m.groupby("multiMovieYn").count()
m.createOrReplaceTempView("sum_m")
sum_m = spark.sql(f"SELECT '{dt}' AS load_dt, * FROM sum_m")

df_n = spark.sql(f"SELECT * FROM movie WHERE repNationCd=='F'")
n = df_n.groupby("repNationCd").count()
n.createOrReplaceTempView("sum_n")
sum_n = spark.sql(f"SELECT '{dt}' AS load_dt, * FROM sum_n")

sum_m.write.mode('append').partitionBy("load_dt").parquet(f"/home/j25ng/data/movie/sum-multi")
sum_n.write.mode('append').partitionBy("load_dt").parquet(f"/home/j25ng/data/movie/sum-nation")

spark.stop()
