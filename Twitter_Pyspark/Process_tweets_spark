import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pyspark 

def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    if "Fidelity" in json_tweet: 
        return True 
    return False

sc.stop()

 
sc = SparkContext("local[2]", "Twitter Demo")

ssc = StreamingContext(sc, 10) 
IP = "localhost"
Port = 5555
lines = ssc.socketTextStream(IP, Port)

lines.foreachRDD( lambda rdd: rdd.saveAsTextFile("./tweets_20190915/%f" % time.time()) )
 
rdd_alldata =  sc.parallelize([("k", 1)])
 
lines.foreachRDD( lambda rdd: rdd_alldata.join(rdd) )
 
ssc.start()
 
ssc.awaitTermination()

#ssc.stop()
