import sys
import operator

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils
# from pyspark.streaming.mqtt import MQTTUtils
print("=========================== ALO ========================")
sc = SparkContext(appName="Foody")
ssc = StreamingContext(sc, 10)
# ssc.checkpoint("checkpoint")

# broker URI
brokerUrl = "tcp://rabbitmq:1883" 
topic = "foody"

stream = MQTTUtils.createStream(ssc, brokerUrl, topic, username="hoang", password="hoang")

# store inputs to hdfs

# process
words = stream.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
print("=========================== BLO ========================")
stream.count().pprint()
def printHistogram(time, rdd):
    c = rdd.collect()
    print("-------------------------------------------")
    print("Time: %s" % time)
    print("-------------------------------------------")
    for record in c:
    	# "draw" our lil' ASCII-based histogram
        print(str(record[0]) + ': ' + '#'*record[1])
    print("")
wordCounts.foreachRDD(printHistogram)

# send output to elasticsearch
ssc.start()
ssc.awaitTermination()
ssc.stop()