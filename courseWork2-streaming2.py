from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from collections import defaultdict
import json

#Setup the Kafka Stream
sc = SparkContext("local[2]", "DistinctLicences")

batch_interval = 60

ssc = StreamingContext(sc, batch_interval)

#kafkaStream is now a DStream representing the continuous stream of data
kafkaStream = KafkaUtils.createDirectStream(ssc, ["licence"], {"metadata.broker.list": "localhost:9092"})

#just take the JSON string (before it was paired with "none")
jsonString = kafkaStream.map(lambda line: line[1])

#create a dicitonary of each field in the JSON string to its value using the JSON API
dict = jsonString.map(lambda string: json.loads(string))

#create a pair of the siteID value and the number 1 -> eg. (45, 1)
siteIDPair = dict.map(lambda dictionary: (dictionary['SiteID'], 1))		

#Then reduce this pair in a 60second window giving consecutive time sections of the DStream, counting the number of 
#occurences of the same sightID. Next, apply a filter to get the siteIDs have occured more than 200 times eg. (45, 245)
#Finally, map the siteIDs that have occured more than 200 times into a list on their own, then reduce each list so that 
#each list is added together so that I have a list of siteIDs that occured over 200 times for a 60 second window
siteIDFilter = siteIDPair.window(60, 60).reduceByKey(lambda y, x: x+y).filter(lambda pair: pair[1] > 200)
siteIDList = siteIDFilter.map(lambda pair: [pair[0]]).reduce(lambda y, x: x+y)

siteIDList.saveAsTextFiles("/home/ubuntu/saves3v2/number") 

ssc.start()
ssc.awaitTermination()
