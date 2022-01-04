from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import functools
from pyspark.sql.functions import udf, col, split, explode
from pyspark.sql.types import StringType
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.feature import HashingTF, IDF

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

def process(df):
    cmt = df.select("Bình Luận")
    cmt = cmt.withColumnRenamed("Bình Luận","concat")        

    udf_star_desc = udf(lambda x:segmentation_remove_punctuation(x),StringType())
    unioned_d = cmt
    unioned_df = unioned_d.withColumn("clean_data",udf_star_desc(col("concat")))
    unioned_df = unioned_df.drop('concat')

    df_split = unioned_df.select(split(col("clean_data"),"sep").alias("split_cmt")) \
        .drop("clean_data")

    df_flatten= df_split.withColumn('split_cmt', explode('split_cmt'))

    df_flatten = df_flatten.withColumnRenamed("split_cmt","clean_data")

    pipeline = PipelineModel.load("./pre_trained/pipeline_1")
    model = LogisticRegressionModel.load("./pre_trained/logistic_regression_1")

    dataset = pipeline.transform(df_flatten)
    predictions = model.transform(dataset)
    predictions = predictions.select('clean_data','prediction')
    predictions = predictions.withColumnRenamed("clean_data","Bình Luận")
    predictions = predictions.na.drop()
    predictions = predictions.filter(col("Bình Luận") != "no data")
    predictions.show()
    
    return predictions