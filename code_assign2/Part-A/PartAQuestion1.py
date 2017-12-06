from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window
import sys
from pyspark import SparkContext
sc = SparkContext()
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

if __name__=='__main__':
	if len(sys.argv)!=2:
		exit(-1)
	input_dir=sys.argv[1]

	spark = SparkSession.builder.appName('Count_RT_MT_RE').getOrCreate()
	
	Myschema = StructType([StructField('A', StringType(), True),
                     StructField('B', StringType(), True),
                     StructField('C', StringType(), True),
                     StructField('Interaction', StringType(), True)])

	lines = spark \
  		.readStream \
  		.schema(Myschema) \
  		.csv(input_dir)
	
	
	result = lines.select('C','Interaction')

	# Generate running word count
	Counts = result.groupBy(window(result.C, "60 minutes", "30 minutes"), 'Interaction').count()

	# Start running the query that prints the running counts to the console
	query = Counts \
    	.writeStream \
    	.outputMode('complete') \
    	.format('console') \
    	.start()

	query.awaitTermination()
