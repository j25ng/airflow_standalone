from pyspark.sql import SparkSession
import sys

dt = sys.argv[1]
spark = SparkSession.builder.appName("agg_df").getOrCreate()

df = spark.read.parquet(f"/home/j25ng/data/movie/hive/load_dt={dt}")
df.show()
df.createOrReplaceTempView("movie")

df_m = spark.sql(f"""
SELECT
    sum(salesAmt) AS salesAmt,
    sum(audiCnt) AS audiCnt,
    sum(showCnt) AS showCnt,
    multiMovieYn,
    '{dt}' AS load_dt
FROM movie
GROUP BY multiMovieYn
""")

df_n = spark.sql(f"""
SELECT
    sum(salesAmt) AS salesAmt,
    sum(audiCnt) AS audiCnt,
    sum(showCnt) AS showCnt,
    repNationCd,
    '{dt}' AS load_dt
FROM movie
GROUP BY repNationCd
""")

df_m.write.mode('append').partitionBy("load_dt").parquet(f"/home/j25ng/data/movie/sum-multi")
df_n.write.mode('append').partitionBy("load_dt").parquet(f"/home/j25ng/data/movie/sum-nation")

spark.stop()
