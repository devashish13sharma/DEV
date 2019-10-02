import sys
 
from pyspark import SparkContext, SparkConf
 
sc.stop()

conf = SparkConf().setAppName("read text file in pyspark")
sc = SparkContext(conf=conf)
 
lines = sc.textFile("tweets_20190915/*")

llist = lines.collect()

for line in llist:
	print(line)
  
lines.collect()
