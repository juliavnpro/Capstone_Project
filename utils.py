import findspark
findspark.init()
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row, SQLContext

def _initialize_spark() -> SparkSession:
    # Kiểm tra xem liệu SparkContext đã tồn tại hay chưa
    if 'sc' not in globals():
        sc = SparkContext.getOrCreate()
    else:
        sc = globals()['sc']

    # Kiểm tra xem liệu SparkSession đã tồn tại hay chưa
    if 'spark' not in globals():
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = globals()['spark']
    # SparkContext.setSystemProperty('spark.executor.memory', '12g')
    # SparkContext.setSystemProperty('spark.driver.memory', '12g')
    # Cau hinh de doc tap tin tu HDFS
    # SparkContext.setSystemProperty('spark.hadoop.dfs.client.use.datanode.hostname', 'true')
    conf = SparkConf().setAppName("2_Recommendation_System").setMaster("local")
    conf.set("spark.network.timeout", "365d")
    conf.set("spark.executor.heartbeatInterval", "100s")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    return spark, sc