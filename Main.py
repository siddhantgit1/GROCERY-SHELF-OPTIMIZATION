from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType
from itertools import *
import sys
import gc

from operator import truediv

conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
       .set('spark.driver.memory', '8G')
       .set("spark.driver.host", "127.0.0.1"))

sc = SparkContext(conf = conf)
spark = SparkSession(sc)


#Generate the rules 
def rules(RDD, total, s):
    rulesNumber = 0 #to print the final number of rules
    RDDDict = [] 
    for rdd in RDD:
        RDDDict.append(rdd.collectAsMap())
    index = 1
    for rdd in RDDDict[1:]: #For each tuples sizes 
        for key in rdd.keys():  #looping on each frequent pairs, then each frequent triples ..etc
            for i in range (index, 0, -1): 
                combination = combinations(key, i) #creating first part of rule (for (A,B)->(C), generate (A,B))
                for A in combination:
                    B = list(set(key)-set(A)) #retrive the second part of the rule for (A,B)-> (C), retrive (C)
                    B.sort()

                    indexA = len(A)-1
                    indexB = len(B)-1
                    valueA, valueB = 0, 0

                    #retrive from RDD, the number of time, the FIRST part of the rules appears
                    if len(A) == 1:
                        valueA = RDDDict[indexA][A[0]]
                    else:
                        valueA = RDDDict[indexA][A]
                    
                    #retrive from RDD, the number of time, the SECOND part of the rules appears
                    if len(B) == 1:
                        valueB = RDDDict[indexB][B[0]]
                    else:
                        valueB = RDDDict[indexB][tuple(B)]
                    

                    confidence = rdd[key]/valueA #Confidence calculation
                    if confidence >= s: #display only rules equal or higher than confidence
                        print(str(A)+ " -> " + str(B) +" Confidence : "+str(confidence)+" and Interest : "+str(abs(confidence - valueB/total)))
                        
                        rulesNumber += 1 #incremente number of rules higher than confidence
        index += 1
    print("Number of rules : "+str(rulesNumber))


#Generate frequent tuples
def frequent(support, confidence):
    rawPurchases = sc.textFile("datas/T10I4D100K.dat") # Get the datas
    total = rawPurchases.count() # Total number of basket

    words = rawPurchases.flatMap(lambda line: line.split(" ")) # each single item
    purchases = rawPurchases.map(lambda x: x.split(" ")) 

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a + b) # Count frequent individual item
    wordCounts = wordCounts.filter(lambda x: len(x[0])>=1 ) #Remove retour à la ligne
    wordCounts = wordCounts.filter(lambda x: (x[1]/total)*100 >= support ) #Remove items below support

    wordsFreq = wordCounts.sortByKey(True) #Sort the items
    wordsFreq = wordsFreq.map(lambda x: x[0]) # get the item for the pair (item, count)

    tupleCount = wordCounts
    index = 2  
    print("________________________")
    print("Support : "+str(support)+"    Confidence :"+str(confidence))
    print("Number of tuples for size 1 :")
    print(tupleCount.count()) #print number of frequent idividual item
    RDD = [wordCounts] # Create a list containing a list of RDD, each RDD containing the pairs of frequent tuples which their frequency. Useful for generating rules


    #loop for each tuples sizes (pairs, triple, quad..etc)
    while tupleCount.count() > 0:
        print("Number of tuples for size "+str(index)+" :") #representing size of tuple
        tupleCountRules = tupleCount

        tupleFreq = tupleCount.map(lambda x: x[0]) # get the item for the pair (item, count)
   

        #create RDD of items with all frequent items from the previous tuples created
        tupleFreqSet = set(tupleFreq.collect())
        if index == 2:
            explodeTuple = tupleFreqSet
        else:
            explodeTuple = tupleFreq.reduce(lambda x,y: set(x) | set(y)) 
        wordsFreq = wordsFreq.filter(lambda x: x in explodeTuple) 
        

        #Create all possible n-tuples from frequent n-1tuples
        wordsFreqList = wordsFreq.collect()
        if index == 2:
            tuplePrec = tupleFreq.map(lambda x: [[x,i] for i in wordsFreqList])
        else:
            tuplePrec = tupleFreq.map(lambda x: [list(x) + [i] for i in wordsFreqList])


        tuplePrec = tuplePrec.map(lambda x: [sorted(i) for i in x])#Sort all created n-tuples
        tuplePrec = tuplePrec.flatMap(lambda x: x) 
        tuplePrec = tuplePrec.map(lambda x: tuple(x))

        tuplePrecDatas = tuplePrec.collect()
        dataSet = set(tuplePrecDatas)
        wordsFreqList = wordsFreq.collect()
        wordsFreqSet = set(wordsFreqList)

        tupleCount = purchases.filter(lambda x: len(list(set(x) & wordsFreqSet)) >= index) #Remove basket without not enought frequent item
        tupleCount = tupleCount.map(lambda x : sorted(x))                                  #sort 
        tupleCount = tupleCount.map(lambda x: list(combinations(x, index)))                #Create all possible combinaison 
        tupleCount = tupleCount.map(lambda x: list(set(x) & dataSet))                      #remove non frequent combinaison
        tupleCount = tupleCount.flatMap(lambda x: x)
        tupleCount = tupleCount.map(lambda pair: (pair, 1)).reduceByKey(lambda a,b: a + b) #count number of tuples
        tupleCount = tupleCount.filter(lambda x: (x[1]/total)*100 >= support)              #remove tuples below support

        if tupleCount.count() > 0:
            RDD.append(tupleCount)
        print(tupleCount.count())

        index += 1
    print("_____________________")
    print("Associations rules")

    rules(RDD, total, confidence) #Generating rules





if sys.argv[1] == "Frequent":
    support = float(sys.argv[2]) #Get the support parameter
    confidence = float(sys.argv[3]) #Get the confidence paramter for rules
    frequent(support, confidence) 
else:    
    frequent(1,0.9)

