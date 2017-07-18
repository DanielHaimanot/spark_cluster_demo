from pyspark.sql import SQLContext
from dateutil.parser import parse
from datetime import datetime 

from pyspark.mllib.clustering import KMeans
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors 
from pyspark.mllib.stat import Statistics 
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

from math import radians, cos, sin, asin, sqrt
import numpy as np

sqlContex = SQLContext(sc)

def date_parse(s):
    y = (lambda y:(int(y[0]), int(y[1]), int(y[2])))
    return y((lambda x:x.split(' ',1)[0].split('-',3))(s))

def distance(lat1,lon1,lat2, lon2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

df_s = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://asgn1/pp-sampled.csv')
df_p = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://asgn1/ukpostcodes.csv')

sample = df_s.rdd
post = df_p.rdd

sample.count() #total entries count: 1826735

cleanup = sample.map(lambda x:(x.postcode,( date_parse(x.date),x.price, x.status)))
post = post.map(lambda x:(x.postcode, (x.latitude, x.longitude)))

join = post.join(cleanup).map(lambda (p,(c,(d,pp,s))): (p,c,d,pp,s))

join.count()# post codes (1821703)

valid = join.filter(lambda (x,c,(y,m,d),p,s):(p != None and p >=0) and (y >=1995 and y <= 2012 and m >= 1 and m<=12 and d>=1 and d<=31) and s != 'D')

valid.count() # 1821703

max_val = valid.map(lambda (p,c,(y,m,d),pp,s): (y,pp)).reduceByKey(lambda a,b: a if (a>b) else b)
max_val.collect()
min_val = valid.map(lambda (p,c,(y,m,d),pp,s): (y,pp)).reduceByKey(lambda a,b: a if (a<b) else b)
min_val.collect()


yp = valid.map(lambda (p,c,(y,m,d),pp,s): (y,(pp, 1))) #Average for each year
average = yp.aggregateByKey((0.0,0), \
			(lambda x,y:(x[0]+y[0],x[1]+1)), \
			(lambda r1,r2:(r1[0]+r2[0],r1[1]+r2[1]))) \
			.mapValues(lambda x: x[0]/x[1]) \
			.sortByKey(ascending=False) \
			.collect()

kv = valid.map(lambda (p,c,(y,m,d),pp,s): (y,pp))

median = kv.map(lambda x:(x[0], [ x[1]])) \
		.reduceByKey(lambda a,b: a+b) \
		.map(lambda x:(x[0], np.median(x[1]))) \
		.collect()

cleanup1 = sample.map(lambda x:(x.postcode,( date_parse(x.date),x.price, x.status, (x.txid,x.paon,x.street,x.locality))))

join1 = post.join(cleanup1).map(lambda (p,(c,(d,pp,s,x))): (p,c,d,pp,s,x))			

valid1 = join1.filter(lambda (p,c,(y,m,d),pp,s,x): \
(pp >=0) and (y >=1995 and y <= 2012 and m >= 1 and m<=12 and d>=1 and d<=31) and s != 'D')

p6 = valid1.map(lambda (p,c,(y,m,d),pp,s,x): ((y, (lambda z: z.split(' ',1)[0])(p)),((pp,x), (pp,x))))

diffPairs = p6.reduceByKey(lambda (a1,a2),(b1,b2):(( a1 if (a1[0]<b1[0]) else b1), (a2 if(a2[0]>b2[0]) else b2))) 

diff = diffPairs.map(lambda (k,((v1,x1),(v2,x2))):(k, abs(v1-v2),x1,x2))

ten = diff.filter(lambda ((k1,k2),d,x1,x2): k1 == 2008).takeOrdered(10,key=lambda ((y,p), d, x1,x2): -d)

p7 = valid.map(lambda (p,c,(y,m,d),pp,s): ((y, (lambda z: z.split(' ',1)[0])(p)),pp))
agg = p7.map(lambda ((y,p),pp): ((p, y),(pp,1))) \
 	.reduceByKey(lambda (a,b),(x,y): (a+x,b+y))

avg = agg.map(lambda ((x,y),(p,c)): (x,(y,p/float(c))))

allYears = avg.map(lambda (k,(y,p)): (k,[(y,p)])) \
       		.reduceByKey(lambda x,y: x + y)

maxRange = allYears.map(lambda (k,a): (k,(max(a, key=lambda x:x[0]), min(a,key=lambda x:x[0]))))
top10 = maxRange.map(lambda (k,((y2,avg2),(y1,avg1))): (k,(avg2 - avg1)/avg1)) \
		.takeOrdered(10, key=lambda x : -x[1])

avgList = avg.map(lambda (k, (y, avg)): (k,[(y,avg)])) \
.reduceByKey(lambda x,y: x+y) \
.filter(lambda (k,v): len(v) > 1)

srtVal = avgList.map(lambda (k,v):(k,sorted(v, key=lambda x: x[0])))

flip = srtVal.map(lambda (k, v):(k, map( lambda x: x[1], v)))

YoY = lambda x: [(x[1] - x[0])/x[0]] + YoY(x[1:]) if len(x) >= 2 else []

aagr = flip.map(lambda (k,v):(k,sum(YoY(v)) / (len(v)-1))).takeOrdered(10, key=lambda (k,f): -f)

p8 = valid.map(lambda (p,c,(y,m,d),pp,s): (m,1))

red = p8.reduceByKey(lambda c1,c2:c1+c2)
years = valid.map(lambda (p,c,(y,m,d),pp,s): y).distinct().count()

averages = red.map(lambda (m,t): (m,t/float(years))).takeOrdered(1,key=lambda x: -x[1])
# [(7, 9737.111111111111)]

p9 = valid.filter(lambda (k,c,(y,m,d),p,s): y == 2010) \
	  .map(lambda (k,(x,y),d,p,s): array([x,y]))	

clusters = KMeans.train(p9, 20, maxIterations=10, runs=10, initializationMode="random")
prediction = clusters.predict(p9)
centers = clusters.clusterCenters

sums = prediction.map(lambda x: (x,1)) \
		.reduceByKey(lambda x,y: x+y)

lats = sums.map(lambda (x,y): [centers[x][0]]).reduce(lambda x,y:x+y)
lons = sums.map(lambda (x,y): [centers[x][1]]).reduce(lambda x,y:x+y)
count = sums.map(lambda (x,y): [y]).reduce(lambda x,y:x+y)

# Plot on a Google Map using Bokeh (include Maps.py for the plot method)
# execfile('Maps.py')
# plot(lats, lons, count)

l = post.filter(lambda (k,x):k=='W1J 7NT').map(lambda (k,(c1,c2)): [c1,c2]).reduce(lambda x,y: x+y)
p10 = valid.filter(lambda (k,c,(y,m,d),p,s): y == 2010)

dist = p10.map(lambda (k,(c0,c1),d,p,s): (p,distance(c0,c1,l[0],l[1])))
vectors = dist.map(lambda (x,y): Vectors.dense([x,y]))

print(Statistics.corr(vectors,method='spearman'))

parsedData = dist.map(lambda (p,d): LabeledPoint(float(p),Vectors.dense(d)))
model = LinearRegression(maxIter=10,regParam=0.3, elasticNetParam=0.8).fit(parsedData.toDF())

Beta = model.coefficients
intercept = model.intercept
x = dist.map(lambda (p,d): Vectors.dense(d))
y = dist.map(lambda (p,d):p)
sd_y = y.sampleStdev()
sd_x = x.sampleStdev()
r = Beta / (sd_y/sd_x) 

predict = model.transform(parsedData.toDF())

evaluator = RegressionEvaluator(metricName='rmse')
RMSE = evaluator.evaluate(predict) #226861.44751570973


























