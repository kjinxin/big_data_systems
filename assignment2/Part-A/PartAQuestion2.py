from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window
import sys
#from pyspark import SparkContext
#sc = SparkContext()
#log4j = sc._jvm.org.apache.log4j
#log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

if __name__=='__main__':
	if len(sys.argv)!=3:
		exit(-1)
	input_dir=sys.argv[1]
	output_dir=sys.argv[2]

	spark = SparkSession.builder.appName('Count_RT_MT_RE').getOrCreate()
	
	Myschema = StructType([StructField('A', StringType(), True),
                     StructField('ID', StringType(), True),
                     StructField('C', StringType(), True),
                     StructField('D', StringType(), True)])

	lines = spark \
  		.readStream \
  		.schema(Myschema) \
  		.csv(input_dir)
	
	
	result = lines.select('ID').where("D='MT'")

	# Start running the query that prints the running counts to the console
	query = result \
    	.writeStream \
	.outputMode('append') \
	.format('parquet') \
	.option('path', output_dir) \
	.option("checkpointLocation",'./checkpoint') \
	.trigger(processingTime='10 seconds') \
    	.start()

	'''
	query = result \
        .writeStream \
        .outputMode('append') \
        .format('console') \
        .trigger(processingTime='10 seconds') \
        .start()
	'''

	query.awaitTermination()
