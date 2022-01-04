from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils
import json

# configure environment
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
ssc = StreamingContext(sc, 10)

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
        print(data)
        df = spark.createDataFrame(data)

    stream.foreachRDD(takeAndPrint)
    
pprint(mqttStream)

ssc.start()
ssc.awaitTermination()
ssc.stop()