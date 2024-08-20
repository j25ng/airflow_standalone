from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

df = spark.read.parquet("/home/j25ng/code/movdata/data/pars")
df.createOrReplaceTempView("movie")

df_dir = spark.sql("""
SELECT
    directorNm,
    count(directorNm) AS directorCnt
FROM movie
GROUP BY directorNm
ORDER BY directorCnt DESC
""")
df_dir.write.mode('overwrite').parquet("/home/j25ng/code/movdata/data/sel/director")

df_com = spark.sql("""
SELECT
    companyCd,
    companyNm,
    count(companyCd) AS companyCnt
FROM movie
GROUP BY companyCd, companyNm
ORDER BY companyCnt DESC
""")
df_com.write.mode('overwrite').parquet("/home/j25ng/code/movdata/data/sel/company")

spark.stop()
