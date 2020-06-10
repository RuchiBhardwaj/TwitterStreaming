#connecting with pyspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
spark = SparkSession.builder.getOrCreate()
dataframe_mysql = spark.read.format("jdbc").options(
    url="jdbc:mysql://127.0.0.1:3306/Twitterdata?characterEncoding=UTF-8&autoReconnect=true",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "twitter",
    user="root",
    password="hello123").load()
dataframe_mysql.show(100)
