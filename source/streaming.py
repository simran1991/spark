from pyspark import SparkConf, SparkContext
import re
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    def filterEmpty(value):
        #print('working on value %s',value)	
        if(value==''):
            return False
        return True    	

   	# The main script - create our SparkContext
    conf = SparkConf().setAppName("WordCount")

    sc = SparkContext(conf = conf)

    streamingContext=StreamingContext(sc,1)
    