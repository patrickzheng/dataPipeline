from pyspark import SparkContext 
# sc = SparkContext(appName="TestApp")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#text_file = spark.textFile("s3://amazon-reviews/complete.json")
df = sqlContext.read.json("s3://patricks3db/meta_Movies_and_TV.json")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("/beaverData/jobTemp")
