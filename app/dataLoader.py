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
df.show()
print("=======================chat luong=============================")
df.filter(df["Chất lượng"] == 8.0).show()

r=df.rdd.collect()
id = r[0][1]['Chất lượng']
df2=df.withColumn("Chất lượng 2", lit(id)) 

esconf={}
esconf["es.mapping.id"] = "_id"
esconf["es.nodes"] = "elasticsearch"
esconf["es.port"] = "9200"
esconf["es.update.script.inline"] = "ctx._source.location = params.location"
esconf["es.update.script.params"] = "location:<Cambridge>"
esconf["es.write.operation"] = "upsert"

df2.write.format("org.elasticsearch.spark.sql").options(**esconf).mode("append").save("sedu/info")
# save output to es
# df = df.groupBy("Link")
print(type(df))
print(type(df.rdd))
# python_rdd = df.rdd.map(lambda item: ('key', {
#     'Link': 1
# }))
# print(python_rdd)
# es_conf = {"es.nodes" : "elasticsearch",
#     "es.port" : "9200", "es.resource" : "sedu2/metrics"}
# df.rdd.saveAsNewAPIHadoopFile(
# path='-',
# outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
# keyClass="org.apache.hadoop.io.NullWritable",
# valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_conf)


# df2 = spark.createDataFrame([{'num': i} for i in range(10)])
# df2 = df.drop('_id')
# df2.write.format(
#    "org.elasticsearch.spark.sql"
# ).option(
#    "es.resource", 'sedu2/metrics'
# ).option(
#    "es.nodes", 'elasticsearch'
# ).option(
#    "es.port", '9200'
# ).mode("append").save("school/info")