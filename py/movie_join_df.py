from pyspark.sql import SparkSession
import sys

dt = sys.argv[1]
spark = SparkSession.builder.appName("join_df").getOrCreate()

df1 = spark.read.parquet(f"/home/j25ng/data/movie/repartition/load_dt={dt}")

df2= df1['movieCd', 'multiMovieYn', 'repNationCd']
df2.createOrReplaceTempView("movie_type")

df3 = spark.sql("SELECT movieCd, repNationCd FROM movie_type WHERE multiMovieYn IS NULL")
df3.createOrReplaceTempView("multi_null")

df4 = spark.sql("SELECT movieCd, multiMovieYn FROM movie_type WHERE repNationCd IS NULL")
df4.createOrReplaceTempView("nation_null")

df = spark.sql("SELECT m.movieCd, n.multiMovieYn, m.repNationCd FROM multi_null m, nation_null n WHERE m.movieCd = n.movieCd")

df.write.partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/j25ng/data/movie/hive/")

spark.stop()
