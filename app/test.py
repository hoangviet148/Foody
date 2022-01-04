from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.context import SparkContext

from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

conf = SparkConf().setAppName("ESTest")
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)



es_reader = (spark.read
    .format("org.elasticsearch.spark.sql")
    .option("inferSchema", "true")
    .option("es.read.field.as.array.include", "tags")
    .option("es.nodes","elasticsearch:9200")
    .option("es.net.http.auth.user","elastic")
)

df = es_reader.load("sedu")

from pyspark.sql.functions import lit
df = df.withColumn("Cot moi", lit(100))
df.show()
df.write.format(
   "org.elasticsearch.spark.sql"
).option(
   "es.resource", 'sedu3/metrics'
).option(
   "es.nodes", 'elasticsearch'
).option(
   "es.port", '9200'
).mode("append").save()