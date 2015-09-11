import json
import time
from pyspark import SparkContext
from operator import itemgetter

def myfunc(mymap,arg):
    if(arg in mymap):
        return mymap[arg]
    else:
        return ""

def parseJson(line):
    try:
        return json.loads(line)
    except:
        return json.loads('{}')
    
def extractFromList(groupedvalue):
    lst = list(groupedvalue)
    sortlst = sorted(lst,key=lambda x: x[1], reverse=True)
    return sortlst[:10]
       

sc = SparkContext()
inputRDD = sc.textFile("datasets/*.gz")
inputRDD = inputRDD.map(lambda line: parseJson(line)) 
#converting json into dict

def top10urls(sc,inputRDD,dataFile,urlout):
    
    toptenurls = inputRDD.map(lambda mymap: myfunc(mymap,'u')).filter(lambda mymap: len(mymap)>0).map(lambda mymap : (mymap, 1)) \
                         .reduceByKey(lambda a, b : a + b) \
                         .map(lambda (a, b) : (b, a)) \
                         .sortByKey(ascending=False) \
                         .map(lambda (b, a) : (a, b))
    return sc.parallelize(toptenurls.take(10))


top10urlRDD = top10urls(sc, inputRDD, "datasets/*.gz", "top10urls")
#print str(top10urlRDD.take(10))
top10urlRDD.saveAsTextFile("sparkout/top10urls")

def top10urlspercity(sc,inputRDD,dataFile,urlout):
    
    toptencities = inputRDD.map(lambda mymap: myfunc(mymap,'cy')).filter(lambda mymap: len(mymap)>0).map(lambda mymap : (mymap, 1)) \
                         .reduceByKey(lambda a, b : a + b) \
                         .map(lambda (a, b) : (b, a)) \
                         .sortByKey(ascending=False) \
                         .map(lambda (b, a) : (a, b))
    return sc.parallelize(toptencities.take(10))

top10citiesRDD = top10urlspercity(sc, inputRDD, "datasets/*.gz", "top10cities")
#print str(top10citiesRDD.take(10))
top10citiesRDD.saveAsTextFile("sparkout/top10cities")


def top10urlspermonth(sc,inputRDD,dataFile,urlout):
    
    urltime = inputRDD.map(lambda mymap: (myfunc(mymap,'u'), myfunc(mymap, 't')))   \
                         .filter(lambda (url, timestamp): len(url)>0 and timestamp != '')   \
                         .map(lambda (url, timestamp): (url,time.strftime("%Y %m", time.localtime(timestamp)))) \
                         .map(lambda (url, timestamp) : ((timestamp, url) , 1)) \
                         .reduceByKey(lambda a, b : a + b) \
                         .map(lambda ((timestamp, url), count) : (timestamp, (url, count))) \
                         .groupByKey() \
                         .map(lambda (timestamp, grp) : (timestamp, extractFromList(grp)))
                    #     .map(lambda (a, b) : (b, a)) \
                     #    .sortByKey(ascending=False) \
                     #    .map(lambda (b, a) : (a, b))
    return urltime

top10monthsRDD = top10urlspermonth(sc, inputRDD, "datasets/*.gz", "top10months")
#print str(top10monthsRDD.collect())
top10monthsRDD.saveAsTextFile("sparkout/top10months")
