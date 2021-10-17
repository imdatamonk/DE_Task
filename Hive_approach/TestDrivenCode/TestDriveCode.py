import os
from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
from pyspark.sql import SparkSession

os.environ['SPARK_HOME'] = '/usr/local/lib/python3.6/site-packages/pyspark'

master = 'local'
appName = 'PySpark_Dataframe Hive Operations'

warehouse_location = 'hdfs://127.0.0.1:9000/user/hive1/warehouse'
metastore_location = 'file:///usr/local/hive/'

config = SparkConf()\
    .set("spark.sql.catalogImplementation", "hive")\
    .set("spark.sql.warehouse.dir", warehouse_location)\
    .set('spark.hadoop.hive.metastore.warehouse.dir', metastore_location)\
    .setAppName(appName).setMaster(master)

sc = SparkContext(conf=config)

hiveContext = SparkSession.builder.enableHiveSupport().getOrCreate()

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

hiveContext.sql('SHOW DATABASES').show()
#hiveContext.sql('SHOW TABLES').show()

df = hiveContext.sql('SELECT * FROM incubytes.countrywise_data')
df.printSchema()

df.where(df.country=='IND').show()

