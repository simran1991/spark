import re
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import Row
from pyspark.sql import functions


parts = [
    r'(?P<host>\S+)',                   # host %h
    r'\S+',                             # indent %l (unused)
    r'(?P<user>\S+)',                   # user %u
    r'\[(?P<time>.+)\]',                # time %t
    r'"(?P<request>.+)"',               # request "%r"
    r'(?P<status>[0-9]+)',              # status %>s
    r'(?P<size>\S+)',                   # size %b (careful, can be '-')
    r'"(?P<referer>.*)"',               # referer "%{Referer}i"
    r'"(?P<agent>.*)"',                 # user agent "%{User-agent}i"
]
pattern = re.compile(r'\s+'.join(parts)+r'\s*\Z')

def saveData(rdd):
    sparkSession=createSparkSession()
    df=sparkSession.createDataFrame(rdd, schema=None, samplingRatio=None, verifySchema=True)
    df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="logs", keyspace="movielens")\
        .save()

def createSparkSession():
    sparkSession = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
    return sparkSession

def extractURLRequest(line):
    exp = pattern.match(line)
    if exp:
        #request = exp.groupdict()["request"]
        status = exp.groupdict()["status"]
        
        #if request:
        if status:
           return status    
           #requestFields = request.split()
           #if (len(requestFields) > 1):
                #return requestFields[1]


if __name__ == "__main__":

    sc = SparkContext(appName="StreamingFlumeLogAggregator")
    

    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)

    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)

    lines = flumeStream.map(lambda x: x[1])
    urls = lines.map(extractURLRequest)

    # Reduce by URL over a 5-minute window sliding every second
    urlCounts = urls.map(lambda x: (x, 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y : x - y, 300, 5)
    
    urlCounts.forEachRDD(lambda rdd : saveData(sparkSession,rdd))
    # Sort and print the results
    sortedResults = urlCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
    
    

    sortedResults.pprint()
    
    ssc.checkpoint("/home/maria_dev/checkpoint")
    ssc.start()
    ssc.awaitTermination()
