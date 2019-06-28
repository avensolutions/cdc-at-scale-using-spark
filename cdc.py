#
# cdc.py
#
# Process config driven, CDC using Spark
#
# Prerequisites:
#	create the current_table_path, temp_current_table_path, history_table_path folders
#
# Example usage:
#
# spark-submit synthetic-cdc-data-generator.py 1000000 1000000 5 10 0.2 0.4 0.4 data/day1 data/day2
#
# spark-submit cdc.py config.yaml data/day1 2019-06-18
# spark-submit cdc.py config.yaml data/day2 2019-06-19

import yaml, sys, datetime
from functions import *
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

sys.stdout.reconfigure(encoding='utf-8')

config_file  = sys.argv[1]
incoming_data_file  = sys.argv[2]
bus_eff_date  = sys.argv[3]

# source configuration

with open(config_file, 'r') as stream:
	config = yaml.load(stream)

# initialize SparkSession	
	
print("Initializing SparkSession (%s)..." % (config["name"]))

spark = SparkSession \
	.builder \
	.appName(config["name"]) \
	.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext

# Load incoming data file into an RDD

incomingrdd_raw = sc.textFile(incoming_data_file).map(lambda x: x.split(config["delimiter"]))

print("ingesting %s new records..." % str(incomingrdd_raw.count()))

# generate key and non key hashes and cast to target data types

typed_with_hashes = incomingrdd_raw.map(lambda x: process_incoming_recs(x, config["columns"], bus_eff_date))

# convert rdd to a dataframe
incoming_df = spark.createDataFrame(typed_with_hashes, 
						schema=gen_tgt_schema(config["columns"]), 
						samplingRatio=None, 
						verifySchema=True)
						
# load current dataframe (just the keyhash and nonkeyhash columns)
curr_df = spark.read.schema(gen_tgt_schema(config["columns"])).parquet(config["current_table_path"])

# prepare history partition path

history_part_path = config["history_table_path"] + "/" + bus_eff_date

if len(curr_df.head(1)) == 0:
	# first run, mark everything as an INSERT and get out!
	
	# write out current
	
	print("Writing out all incoming records as INSERT to current object (%s)..." % (config["current_table_path"]))
	start = datetime.datetime.now()
	incoming_df.write.parquet(config["current_table_path"], mode='overwrite')
	finish = datetime.datetime.now()
	print("Finished writing out INSERT records to current object (%s)" % (str(finish-start)))
	
	# write out history
	
	print("Writing out INSERT records to history object (%s)..." % (history_part_path))
	start = datetime.datetime.now()
	incoming_df.write.parquet(history_part_path, mode='overwrite')
	finish = datetime.datetime.now()
	print("Finished writing out INSERT records to history object (%s)" % (str(finish-start)))						
else:
	# not the first run, there is work to do!
	
	outerjoined = curr_df.select("keyhash", "nonkeyhash").join(incoming_df.select("keyhash", "nonkeyhash"), "keyhash", "fullouter") \
						.select("keyhash", \
							curr_df.nonkeyhash.alias("curr_nonkeyhash"), \
							incoming_df.nonkeyhash.alias("new_nonkeyhash"))
	outerjoined.cache()

	#
	## INSERT
	#
	
	inserted_recs = outerjoined.filter(outerjoined.curr_nonkeyhash.isNull()) \
						.join(incoming_df, "keyhash", "inner") \
						.drop("curr_nonkeyhash") \
						.drop("nonkeyhash") \
						.drop("operation") \
						.drop("eff_start_date") \
						.withColumn("operation", lit("I")) \
						.withColumn("eff_start_date", lit(bus_eff_date)) \
						.withColumnRenamed("new_nonkeyhash", "nonkeyhash")

	# write out current
	
	print("Writing out INSERT records to current object (%s)..." % (config["temp_current_table_path"]))
	start = datetime.datetime.now()
	inserted_recs.write.parquet(config["temp_current_table_path"], mode='overwrite')
	finish = datetime.datetime.now()
	print("Finished writing out INSERT records to current object (%s)" % (str(finish-start)))
	
	# write out history
	
	print("Writing out INSERT records to history object (%s)..." % (history_part_path))
	start = datetime.datetime.now()
	inserted_recs.write.parquet(history_part_path, mode='overwrite')
	finish = datetime.datetime.now()
	print("Finished writing out INSERT records to history object (%s)" % (str(finish-start)))
		
	#
	## DELETE or MISSING
	#
					  
	not_present = outerjoined.filter(outerjoined.new_nonkeyhash.isNull()) \
				.drop("new_nonkeyhash") \
				.join(curr_df, "keyhash", "inner") \
				.drop("nonkeyhash") \
				.withColumnRenamed("curr_nonkeyhash", "nonkeyhash")
				
	if config["extract_type"] == "full":
		# DELETEs, missing key hashes from new data
		deleted_recs = not_present \
				.withColumn("operation", lit("D")) \
				.withColumn("eff_start_date", lit(bus_eff_date))
		# write out to history only		
		print("Writing out DELETE records to history object (%s)..." % (history_part_path))
		start = datetime.datetime.now()
		deleted_recs.write.parquet(history_part_path, mode='append')
		finish = datetime.datetime.now()
		print("Finished writing out DELETE records to history object (%s)" % (str(finish-start)))
	else:
		missing_recs = not_present \
				.withColumn("operation", lit("X"))
		# write out to current only		
		print("Writing out MISSING records to current object (%s)..." % (config["temp_current_table_path"]))
		start = datetime.datetime.now()
		missing_recs.write.parquet(config["temp_current_table_path"], mode='append')
		finish = datetime.datetime.now()
		print("Finished writing out MISSING records to current object (%s)" % (str(finish-start)))
		
	#
	## UPDATE or NOCHANGE
	#

	keys_present_in_both = outerjoined.filter(outerjoined.new_nonkeyhash.isNotNull()) \
								.filter(outerjoined.curr_nonkeyhash.isNotNull()) 		

	#
	## UPDATE
	#
	
	updated_recs =  keys_present_in_both.filter("curr_nonkeyhash != new_nonkeyhash") \
				.drop("curr_nonkeyhash") \
				.join(incoming_df, "keyhash", "inner") \
				.drop("nonkeyhash") \
				.withColumn("operation", lit("U")) \
				.withColumn("eff_start_date", lit(bus_eff_date)) \
				.withColumnRenamed("new_nonkeyhash", "nonkeyhash")
				
	# write out current
	
	print("Writing out UPDATE records to current object (%s)..." % (config["temp_current_table_path"]))
	start = datetime.datetime.now()
	updated_recs.filter("operation = 'U'") \
		.write.parquet(config["temp_current_table_path"], mode='append')
	finish = datetime.datetime.now()
	print("Finished writing out UPDATE records to current object (%s)" % (str(finish-start)))

	# write out history
	
	print("Writing out UPDATE records to history object (%s)..." % (history_part_path))
	start = datetime.datetime.now()
	updated_recs.write.parquet(history_part_path, mode='append')
	finish = datetime.datetime.now()
	print("Finished writing out UPDATE records to history object (%s)" % (str(finish-start)))
				
	#
	## NOCHANGE
	#
	
	matched_recs = keys_present_in_both.filter("curr_nonkeyhash == new_nonkeyhash") \
				.drop("new_nonkeyhash") \
				.join(curr_df, "keyhash", "inner") \
				.drop("nonkeyhash") \
				.withColumn("operation", lit("N")) \
				.withColumnRenamed("curr_nonkeyhash", "nonkeyhash")
				
	# write out to current only	
	
	print("Writing out NOCHANGE records to current object (%s)..." % (config["temp_current_table_path"]))
	start = datetime.datetime.now()
	matched_recs.write.parquet(config["temp_current_table_path"], mode='append')
	finish = datetime.datetime.now()
	print("Finished writing out NOCHANGE records to current object (%s)" % (str(finish-start)))

# now move the working current data to the active current path	
	
print("Finished CDC Processing!")	

# df = spark.read.parquet("file:///tmp/sample_data_current_temp")
# df.groupBy(df.operation).count().show()
