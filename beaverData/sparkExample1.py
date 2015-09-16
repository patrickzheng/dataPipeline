from pyspark import SparkContext 
sc = SparkContext(appName="TestApp")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# text_file = sc.textFile("s3://amazon-reviews/complete.json")
# text_file = sc.textFile(    "s3://patricks3db/meta_Movies_and_TV.json")
# df = sqlContext.read.json("s3://patricks3db/meta_Movies_and_TV.json")

# connect to cassandra
from cassandra.cluster import Cluster
cluster = Cluster(['172.31.39.223', '172.31.39.224','172.31.39.225','172.31.39.226'])
session = cluster.connect()

insert_statment = session.prepare('INSERT INTO playground.metadata JSON ?')
# counts = text_file.map(lambda line: session.execute(insert_statment, line))

# test another way
from boto.s3.connection import S3Connection
from boto.s3.key import Key
bucket = conn.get_bucket('patricks3db')
key = bucket.get_key("meta_Movies_and_TV.json")
for line in  key.get_contents_as_string().splitlines():
	session.execute(insert_statment, line)
