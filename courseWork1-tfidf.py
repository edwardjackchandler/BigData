from pyspark import SparkConf, SparkContext
from collections import Counter, defaultdict
import math
import os

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#PART 1

#Read the file into an RDD
docs = sc.textFile("/home/ubuntu/Data/wikipedia")

#PART 2

#turns each array of words (document) in the RDD to an list of words split up by a space character
words = docs.map(lambda d: d.split(" ")).filter(lambda x: x != '')
	
#transform the split up words RDD, first by mapping each document to a counter collection which also counts the frequency of each word, 
#then turn the counter into a dictionary 
wordFreq = words.map(lambda doc: Counter(doc)).map(lambda counter: dict(counter))

#PART 3

#transform the wordFreq RDD by flat mapping the keys out so that for each document a word that was included in that document is only
#featured once. Then I performed a simple map reduce with a value of 1 to each key to count the number of docuents each term appears in.
wordDocFreq = wordFreq.flatMap(lambda doc: doc.keys()).map(lambda x: (x, 1)).reduceByKey(lambda y, x: x+y)

#Collect the RDD as a dictionary (map) to use in the next part
wordDocFreqDict = wordDocFreq.collectAsMap()

#PART 4

#top half of the idf formula (Cardinality of the corpus + 1)
topHalf = wordFreq.count() + 1

#Function that performs the IDF function- takes the tophalf calculated earlier, the bottom half of the function
# which is the DF(t, D) worked out in the part 3 + 1, and then performs a natural log function on it.
def idfFunction(term):
	global topHalf
	global wordDocFreqDict
	botHalf = wordDocFreqDict[term] + 1
	
	return math.log(topHalf / botHalf)
	
#Function that performs the formula for TFIDF by multipling the answer from the IDF function and the frequency of the term
def tfidfFunction(term, freq):
	return (idfFunction(term) * freq)	
	
#Function that returns a dictionary of (term, tfidf) pairs for each document  
def step3(doc):
	tfidfDict = dict(map(lambda k: (k, tfidfFunction(k, doc[k])), doc.keys()))
	return tfidfDict		

#map through each document and apply the step3 function
tfidfRDD = wordFreq.map(step3)

#PART 5


def mapList(tuple):
	docID = tuple[1]
	tfidfDocIDList = list(map(lambda k: (k, [(tuple[0][k], docID)]), tuple[0].keys()))
	return tfidfDocIDList
	
def pairToDocID(tuple):
	term = tuple[0]
	tfidfDocIDList = list(map(lambda pair: pair[1], tuple[1]))
	return (term, tfidfDocIDList)
	
#Use the zipWithUniqueId funciton to, pair the dictionaries with their document id 
tfidfDocID = tfidfRDD.zipWithUniqueId()

#transform the RDD made of dictionaries with doc ids, by mapping so the RDD is now a list of (term, [(tfidf, docID)]) tuples for each document, 
#and then flatMap the tuples out. I then used the spark sortBy function to sort the list in ascending order according to the tfidf score, 
#then when the lists are added together in the next step they will be in the correct order. 
sortedTfidfDocID = tfidfDocID.map(mapList).flatMap(lambda tupleList: tupleList).sortBy(lambda tuple: tuple[1][0], ascending=True)

#reduce by key so that for each term there is now a list of pairs, [(tfidf, docID), (tfidf, docID)..], so I then mapped using a function that
#returned each term and a list of the document ids, still in the correct order.
tfidfListID = sortedTfidfDocID.reduceByKey(lambda y, x: x+y).map(pairToDocID)

tfidfListID.saveAsTextFile("/home/ubuntu/saves/wikipediaOutput")

