from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window
import sys
from pyspark import SparkContext
sc = SparkContext()
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

if __name__=='__main__':
	if len(sys.argv)!=3:
		exit(-1)
	input_dir=sys.argv[1]
	user_file=sys.argv[2]
	
	spark = SparkSession.builder.appName('Count_RT_MT_RE').getOrCreate()
	
	Myschema = StructType([StructField('User', StringType(), True),
                     StructField('userB', StringType(), True),
                     StructField('Time', StringType(), True),
                     StructField('Interaction', StringType(), True)])
	
	Listschema=StructType([StructField('User', StringType(), True)])

	userlist = spark \
		   .read \
		   .schema(Listschema) \
		   .csv(user_file)

	userlist.show()

	lines = spark \
  		.readStream \
  		.schema(Myschema) \
  		.csv(input_dir)
	
	result=lines.join(userlist,'User','inner').groupBy('User').count()

	# Start running the query that prints the running counts to the console
	query = result \
    	.writeStream \
	.outputMode('complete') \
	.format('console') \
	.trigger(processingTime='5 seconds') \
    	.start()

	query.awaitTermination()
