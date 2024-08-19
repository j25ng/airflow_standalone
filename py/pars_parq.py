from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, size

spark = SparkSession.builder.appName("parsingParquet").getOrCreate()

# pyspark 에서 multiline(배열) 구조 데이터 읽기
jdf = spark.read.option("multiline","true").json('/home/j25ng/data/json/movie.json')

## companys, directors 값이 다중으로 들어가 있는 경우 찾기 위해 count 컬럼 추가
ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))

# companys, directors 값이 다중으로 들어가 있는 경우 찾기
fdf = ccdf.filter(ccdf.company_count > 1).filter(ccdf.directors_count > 2)

# 2015년 movieCd 20141663 인 영화는 company_count = 2, directors_count = 3 임
fdf.collect()[0]['movieCd']
fdf.collect()[0]['companys'][0]['companyCd']
fdf.collect()[0]['companys'][0]['companyNm']

# 펼치기
edf = fdf.withColumn("company", explode("companys"))

# 또 펼치기
eedf = edf.withColumn("director", explode("directors"))

spark.stop()
