import re
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.types import *
from time import strftime

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

schema=StructType([StructField("statuscode",StringType(),True),StructField("counts",StringType(),True)])

def printData(rdd):
    if not rdd.isEmpty():   
        print( rdd.take(10))

def saveData(time,rdd):
    sparkSession=createSparkSession()
    if not rdd.isEmpty():
        df=sparkSession.createDataFrame(rdd,schema, samplingRatio=None, verifySchema=True)
        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="logs", keyspace="movielens")\
            .save()
        print("current time is " + time.strftime("%H:%M:%S"))
        df.show()        
    
        
                

def createSparkSession():
    sparkSession = SparkSession.builder.config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
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

def updateFunction(newValues,count):
    print(newValues)
    print(count)
    if count is None:
        count=0
    return sum(newValues,count)

def createSparkStreamingContext():
    
    sc=SparkContext(appName="StateFullStreaming")
    sc.setLogLevel("ERROR")
    ssc=StreamingContext(sc,10)

    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)
    lines = flumeStream.map(lambda x: x[1])
    urls = lines.map(extractURLRequest)
    # Reduce by URL over a 5-minute window sliding every second
    urlCounts = urls.map(lambda x: (x, 1)).updateStateByKey(updateFunction)
    #urlCounts.foreachRDD(lambda rdd: printData(rdd))
    #urlCounts.pprint()
    urlCounts.cache()
    
    urlCounts.foreachRDD(saveData)
    ssc.checkpoint("hdfs:///user/maria_dev/checkpoint")

    return ssc


if __name__ == "__main__":
 
    #ssc=StreamingContext(sc,1)
    #ssc.checkpoint(checkpointDir)
    ssc = StreamingContext.getOrCreate("hdfs:///user/maria_dev/checkpoint", lambda : createSparkStreamingContext())
    #ssc.checkpoint(checkpointDir)
    

    ssc.start()
    ssc.awaitTermination()
