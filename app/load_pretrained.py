from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from utils import process
from mqtt import MQTTUtils
import json

# configure environment
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
conf = SparkConf().setAppName("ESTest")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 10)

# read data from ES
mqttStream = MQTTUtils.createStream(
    ssc, 
    "tcp://rabbitmq:1883",  # Note both port number and protocol
    "foody",
    "hoang",
    "hoang"
)

def pprint(stream, num=10):
    """
    Print the first num elements of each RDD generated in this DStream.

    @param num: the number of elements from the first will be printed.
    """
    def takeAndPrint(time, rdd):
         taken = rdd.take(num + 1)
         print("-------------------------------------------")
         print("Time: %s" % time)
         print("-------------------------------------------")
         data = []
         for record in taken[:num]:
            data.append(json.loads(record))
         
         if len(data) > 0: 
            df = spark.createDataFrame(data)
            df.show()
            
            # upload input data to ES
            df.write.format(
               "org.elasticsearch.spark.sql"
            ).option(
               "es.resource", 'sedu/metrics'
            ).option(
               "es.nodes", 'elasticsearch'
            ).option(
               "es.port", '9200'
            ).mode("append").save()
            
            # process data and make predictions
            predictions = process(df)

            # upload predictions to ES
            predictions.write.format(
               "org.elasticsearch.spark.sql"
            ).option(
               "es.resource", 'predictions/metrics'
            ).option(
               "es.nodes", 'elasticsearch'
            ).option(
               "es.port", '9200'
            ).mode("append").save()

    stream.foreachRDD(takeAndPrint)
    
pprint(mqttStream)
# es_reader = (spark.read
#     .format("org.elasticsearch.spark.sql")
#     .option("inferSchema", "true")
#     .option("es.read.field.as.array.include", "tags")
#     .option("es.nodes","elasticsearch:9200")
#     .option("es.net.http.auth.user","elastic")
# )

# df = es_reader.load("sedu")


ssc.start()
ssc.awaitTermination()
ssc.stop()