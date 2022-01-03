from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import functools
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.feature import HashingTF, IDF

#dataset = spark.read.option("header",True).option("multiLine",True).option("delimiter", ",").csv#("/content/drive/My Drive/20212/PySpark/data/data_clean_v3.csv")

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

def pre_processing(dataset):
    dataset = dataset.na.drop()
    dataset_0 = dataset.filter(dataset.mark_standard == '0')
    dataset_1 = dataset.filter(dataset.mark_standard == '1')
    dataset_2 = dataset.filter(dataset.mark_standard == '2')
    unioned_df = unionAll([dataset_0, dataset_1, dataset_2])

    return unioned_df

def segmentation_remove_punctuation(value):
    punc = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    for ch in punc:
        value = value.replace(ch, ' ')
    return value

udf_star_desc = udf(lambda x:segmentation_remove_punctuation(x),StringType() )

unioned_df = pre_processing(dataset)
unioned_df = unioned_df.withColumn("clean_data",udf_star_desc(col("concat")))
unioned_df = unioned_df.drop('concat')

pipeline = PipelineModel.load("/content/drive/My Drive/20212/PySpark/pipeline_1")
model = LogisticRegressionModel.load("/content/drive/My Drive/20212/PySpark/logistic_regression_1")

dataset = pipeline.transform(unioned_df)
predictions = model.transform(dataset)