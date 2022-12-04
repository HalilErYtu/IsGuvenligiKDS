from pyspark.ml.fpm import PrefixSpan
from pyspark.sql import Row, DataFrame
import doctest
import pyspark.ml.fpm
from pyspark.sql import SparkSession
import pyspark
import findspark 
findspark.init()

globs = pyspark.ml.fpm.__dict__.copy()
# The small batch size here ensures that we see multiple batches,
# even in these small test examples:
spark = pyspark.SparkContext()
sc = spark.sparkContext
globs["sc"] = sc
globs["spark"] = spark
import tempfile
temp_path = tempfile.mkdtemp()
globs["temp_path"] = temp_path

df = sc.parallelize([Row(sequence=[[1, 2], [3]]),
                     Row(sequence=[[1], [3, 2], [1, 2]]),
                     Row(sequence=[[1, 2], [5]]),
                     Row(sequence=[[6]])]).toDF()
prefixSpan = PrefixSpan()
prefixSpan.getMaxLocalProjDBSize()
prefixSpan.getSequenceCol()
prefixSpan.setMinSupport(0.5)
prefixSpan.setMaxPatternLength(5)
prefixSpan.findFrequentSequentialPatterns(
    df).sort("sequence").show(truncate=False)


def SparkPrefixSpan(data, minsup, mazpattern):
    df = sc.parallelize([Row(sequence=[[1, 2], [3]]),
                         Row(sequence=[[1], [3, 2], [1, 2]]),
                         Row(sequence=[[1, 2], [5]]),
                         Row(sequence=[[6]])]).toDF()
    prefixSpan = PrefixSpan()
    prefixSpan.getMaxLocalProjDBSize()
    prefixSpan.getSequenceCol()
    prefixSpan.setMinSupport(0.5)
    prefixSpan.setMaxPatternLength(5)
    return prefixSpan.findFrequentSequentialPatterns(df).sort("sequence").show(truncate=False)
