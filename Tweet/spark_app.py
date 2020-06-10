from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with window size 2 seconds
ssc = StreamingContext(sc, 1)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)
# def aggregate_tags_count(new_values, total_sum):
#     return sum(new_values) + (total_sum or 0)
def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']
def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        rdd_list = rdd.collect()
        print (rdd_list)
        # create a DF from the Row RDD
        # hashtags_df = sql_context.createDataFrame(rdd)
        if len(rdd_list) != 0:
            hashtags_df = sql_context.createDataFrame(rdd_list, ['hashtag', 'location', 'time'])
            # Register the dataframe as table
            hashtags_df.registerTempTable("hashtags")
            # get the top 10 hashtags from the table using SQL and print them
            # hashtag_counts_df = sql_context.sql(
            #     "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
            hashtags_df.show()
    except Exception as exe:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
# split each tweet into words
words = dataStream.map(lambda line: line.split("&%"))
# def take_input(rdd):
#     print rdd.collect()
# words.foreachRDD(take_input)
# do processing for each RDD generated in each interval
words.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()