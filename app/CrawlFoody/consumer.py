from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils

sc = SparkContext()
ssc = StreamingContext(sc, 10)

mqttStream = MQTTUtils.createStream(
    ssc, 
    "tcp://rabbitmq:1883",  # Note both port number and protocol
    "foody"                  # The same routing key as used by producer
)
print(type(mqttStream))
mqttStream.count().pprint()
ssc.start()
ssc.awaitTermination()
ssc.stop()