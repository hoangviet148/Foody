from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from utils import process

# configure environment
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
conf = SparkConf().setAppName("ESTest")
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# read data from ES
es_reader = (spark.read
    .format("org.elasticsearch.spark.sql")
    .option("inferSchema", "true")
    .option("es.read.field.as.array.include", "tags")
    .option("es.nodes","elasticsearch:9200")
    .option("es.net.http.auth.user","elastic")
)

df = es_reader.load("sedu")

# process data and make predictions
predictions = process(df)

# upload data to ES
predictions.write.format(
   "org.elasticsearch.spark.sql"
).option(
   "es.resource", 'predictions/metrics'
).option(
   "es.nodes", 'elasticsearch'
).option(
   "es.port", '9200'
).mode("append").save()