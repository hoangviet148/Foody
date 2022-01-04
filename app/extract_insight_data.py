from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import functools
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.feature import HashingTF, IDF
import re

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

dataset = es_reader.load("sedu")

a = 'Hà Giang, Cao Bằng, Bắc Kạn, Lạng Sơn, Tuyên Quang, Thái Nguyên, Phú Thọ, Bắc Giang, Quảng Ninh, \
    Lào Cai, Yên Bái, Điện Biên, Hoà Bình, Lai Châu, Sơn La, Bắc Ninh, Hà Nam, Hà Nội, Hải Dương, Hải Phòng, \
    Hưng Yên, Nam Định, Ninh Bình, Thái Bình, Vĩnh Phúc, Thanh Hóa, Nghệ An, Hà Tĩnh, Quảng Bình, Quảng Trị, Huế, \
    Đà Nẵng, Quảng Nam, Quảng Ngãi, Bình Định, Phú Yên, Khánh Hòa, Ninh Thuận, Bình Thuận, Kon Tum, Gia Lai, Đắk Lắk,\
    Đắk Nông, Lâm Đồng, Bình Phước, Bình Dương, Đồng Nai, Tây Ninh, Vũng Tàu, Hồ Chí Minh, Long An, Đồng Tháp, Tiền Giang,\
    An Giang, Bến Tre, Vĩnh Long, Trà Vinh, Hậu Giang, Kiên Giang, Sóc Trăng, Bạc Liêu, Cà Mau, Cần Thơ'


city = a.split(sep = ', ')

def no_accent_vietnamese(s):
    s = s.lower()
    s = re.sub('[áàảãạăắằẳẵặâấầẩẫậ]', 'a', s)
    s = re.sub('[éèẻẽẹêếềểễệ]', 'e', s)
    s = re.sub('[óòỏõọôốồổỗộơớờởỡợ]', 'o', s)
    s = re.sub('[íìỉĩị]', 'i', s)
    s = re.sub('[úùủũụưứừửữự]', 'u', s)
    s = re.sub('[ýỳỷỹỵ]', 'y', s)
    s = re.sub('đ', 'd', s)
    return s

city_list = []
for i in city:
    city_list.append(no_accent_vietnamese(i).strip().lower().replace(" ","-"))

print(city_list)

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

def segmentation_remove_punctuation(value):
    punc = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    if value is None:
        return "no data"
    else:
        for ch in punc:
            value = value.replace(ch, ' ')
    return value

def detect_city(value):
    if value is None:
        return "no data"
    else:
        for city in city_list:
            if city in value:
                return city
                break 


udf_detect_city = udf(lambda x:detect_city(x),StringType())
udf_star_desc = udf(lambda x:segmentation_remove_punctuation(x),StringType())

# unioned_df = pre_processing(dataset)
unioned_df = dataset
unioned_df = unioned_df.withColumn("city",udf_detect_city(col("Link")))
unioned_df = unioned_df.drop('concat')
unioned_df = unioned_df.na.drop()

unioned_df = unioned_df.filter(col("Điểm đánh giá") != "_._")

unioned_df.groupBy('city').count().show()

"""## Extract Data"""

extract_data = unioned_df

# pho, cafe, lau, nuong
def get_cafe_from_name(value):
    if value is None:
        return 0
    else:
        value = value.lower()
        value = segmentation_remove_punctuation(value)
        if "cafe" in value or "coffee" in value or "tea" in value:
            return 1
        else:
            return 0

def get_lau_from_name(value):
    if value is None:
        return 0
    else:
        value = value.lower()
        value = segmentation_remove_punctuation(value)
        if "lẩu" in value or "nướng" in value:
            return 1
        else:
            return 0

def get_pho_from_name(value):
    if value is None:
        return 0
    else:
        value = value.lower()
        value = segmentation_remove_punctuation(value)
        if "phở" in value :
            return 1
        else:
            return 0

def get_banh_mi_from_name(value):
    if value is None:
        return 0
    else:
        value = value.lower()
        value = segmentation_remove_punctuation(value)
        if "bánh mì" in value :
            return 1
        else:
            return 0

udf_cafe_detect = udf(lambda x:get_cafe_from_name(x),IntegerType())
udf_lau_nuong_detect = udf(lambda x:get_lau_from_name(x),IntegerType())
udf_pho_detect = udf(lambda x:get_pho_from_name(x),IntegerType())
udf_banh_mi_detect = udf(lambda x:get_banh_mi_from_name(x),IntegerType())

extract_data = extract_data.withColumn("cafe",udf_cafe_detect(col("Tên quán")))
extract_data = extract_data.withColumn("lau_nuong",udf_lau_nuong_detect(col("Tên quán")))
extract_data = extract_data.withColumn("pho",udf_pho_detect(col("Tên quán")))
extract_data = extract_data.withColumn("banh_mi",udf_banh_mi_detect(col("Tên quán")))
extract_data.show()

import pyspark.sql.functions as psf
insights = extract_data.groupBy("city").agg(psf.sum("cafe").alias("sum_cafe"),\
                                psf.sum("lau_nuong").alias("sum_lau_nuong"), \
                                psf.sum("pho").alias("sum_pho"), \
                                psf.sum("banh_mi").alias("sum_banh_mi")) \

insights.show()

# upload data to ES
insights.write.format(
   "org.elasticsearch.spark.sql"
).option(
   "es.resource", 'insights/metrics'
).option(
   "es.nodes", 'elasticsearch'
).option(
   "es.port", '9200'
).mode("append").save()