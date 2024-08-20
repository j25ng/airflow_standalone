from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode_outer
import sys

APP_NAME = sys.argv[1]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

jdf = spark.read.option("multiline","true").json('/home/j25ng/code/movdata/data/movies')

edf = jdf.withColumn("company", explode_outer("companys"))
eedf = edf.withColumn("director", explode_outer("directors"))
sdf = eedf.withColumn("directorNm", col("director.peopleNm"))
rdf = sdf.select("movieCd", "movieNm", "genreAlt", "typeNm", "directorNm", "company.companyCd", "company.companyNm")

rdf.write.mode("overwrite").parquet('/home/j25ng/code/movdata/data/pars')

spark.stop()
