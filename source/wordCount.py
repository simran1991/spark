from pyspark import SparkConf, SparkContext
import re


if __name__ == "__main__":
    def filterEmpty(value):
        #print('working on value %s',value)	
        if(value==''):
            return False
        return True    	

   	# The main script - create our SparkContext
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf = conf)
  	# Load up data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/tuts.txt")
   	# Convert the lines into tokens and then inot key value pairs
    linesSplit = lines.flatMap(lambda x : x.strip().split(" ")).filter(lambda x:filterEmpty(x)).map(lambda x : (x,1))
	
    #linesSplit.persist()
    print("**************************************Now printing topTenlines************************************\n")
    topTenlines = linesSplit.take(10)
    
    for line in topTenlines:
        print(line)

    reducedResult=linesSplit.reduceByKey(lambda x,y:x+y)
    
    reducedResult = reducedResult.sortByKey()
    
    result=reducedResult.take(10)

    print("**************************************Now printing reduced result************************************\n")
    for line in result:
        print(line)
    #print(result)