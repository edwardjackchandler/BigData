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

#create a pair of the licence value and the number 1 -> eg. (FTG1092, 1)
mapLicence = dict.map(lambda dictionary: (dictionary['Licence'], 1))

#Then reduce this pair in a 60second window giving consecutive time sections of the DStream
#This gives pairs of (licence, freq) where if a car appeared more than once, it is recorded.
#Now just use count to get the number of unique cars
distinctCarCount = mapLicence.window(60, 60).reduceByKey(lambda y, x: x+y).count()
	
distinctCarCount.saveAsTextFiles("/home/ubuntu/saves2/number")

ssc.start()
ssc.awaitTermination()
