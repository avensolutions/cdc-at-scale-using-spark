#
# synthetic-cdc-data-generator.py
#
# Use Spark to generate random data to be used to simulate changes
#
# Arguments (by position):
#	no_init_recs (the number of initial records to generate)
#	no_incr_recs (the number of incremental records on the second run - should be >= no_init_recs)
#	no_keys (number of key columns in the dataset)
#	no_nonkeys (number of non-key columns in the dataset)
#	pct_del (percentage of initial records deleted on the second run - between 0.0 and 1.0)
#	pct_upd (percentage of initial records updated on the second run - between 0.0 and 1.0)
#	pct_unchanged (percentage of records unchanged on the second run - between 0.0 and 1.0)
#	initial_output (folder for initial output in CSV format)
#	incremental_output (folder for incremental output in CSV format)
#
#	NOTE THAT pct_del + pct_upd + pct_unchanged must equal 1.0
#
# Example usage:
#
# spark-submit synthetic-cdc-data-generator.py 100000 100000 5 10 0.2 0.4 0.4 data/day1 data/day2
#

import uuid, random, sys
from itertools import chain
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

print("Initializing SparkSession ...")

spark = SparkSession \
	.builder \
	.appName("synthetic-cdc-data-generator.py") \
	.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext

no_init_recs  = int(sys.argv[1])
no_incr_recs = int(sys.argv[2])
no_keys = int(sys.argv[3])
no_nonkeys = int(sys.argv[4])
pct_del = float(sys.argv[5])
pct_upd = float(sys.argv[6])
pct_unchanged = float(sys.argv[7])
initial_output = sys.argv[8]
incremental_output = sys.argv[9]

def create_keys(rec_no, no_keys):
	keys = []
	for i in range(no_keys):
		key = str(uuid.uuid1())
		keys.append(key)
	return tuple(keys)
	
def create_nonkeys(keys, no_nonkeys):
	nonkeys = []
	for i in range(no_nonkeys):
		nonkey = random.random()
		nonkeys.append(nonkey)
	return keys, tuple(nonkeys)
	
def flatten_data(keys, nonkeys):		
	tupleOfTuples = keys, nonkeys
	return [element for tupl in tupleOfTuples for element in tupl]
	
# create initial data set
initial_rdd = sc.range(0, end=no_init_recs, step=1) \
			.map(lambda x: create_keys(x, no_keys)) \
			.map(lambda x: create_nonkeys(x, no_nonkeys))

initial_rdd.cache()

# write out initial data 

spark.createDataFrame(initial_rdd \
		.map(lambda x: flatten_data(x[0], x[1]))) \
		.write.csv(initial_output, mode="overwrite")

# create change set

del_rec_max_idx = int(no_init_recs*pct_del) - 1
upd_rec_max_idx = del_rec_max_idx + int(no_init_recs*pct_upd)

del_removed_rdd = initial_rdd \
				.zipWithIndex() \
				.filter(lambda x: x[1] > del_rec_max_idx)
				
del_removed_rdd.cache()

remaining_rec_cnt = del_removed_rdd.count()

print("Removed %s records" % (str(initial_rdd.count()-remaining_rec_cnt)))

initial_rdd.unpersist()

updated_rdd = del_removed_rdd \
				.filter(lambda x: x[1] > upd_rec_max_idx) \
				.map(lambda x: x[0]) \
				.map(lambda x: create_nonkeys(x[0],no_nonkeys)) \
				.map(lambda x: flatten_data(x[0], x[1]))

updated_rdd.cache()
				
print("Updated %s records" % (str(updated_rdd.count())))				
				
unchanged_rdd = del_removed_rdd \
				.filter(lambda x: x[1] <= upd_rec_max_idx) \
				.map(lambda x: x[0]) \
				.map(lambda x: flatten_data(x[0], x[1]))
				
unchanged_rdd.cache()			

print("%s records unchanged" % (str(unchanged_rdd.count())))				

# write out updated and unchanged records

spark.createDataFrame(updated_rdd.union(unchanged_rdd)) \
		.write.csv(incremental_output, mode="overwrite")
		
updated_rdd.unpersist()
unchanged_rdd.unpersist()

# generate remaining records as INSERTs

no_addtl_recs = no_incr_recs-remaining_rec_cnt

print("Adding %s new records" % (no_addtl_recs))

addtl_recs_rdd = sc.range(0, end=no_addtl_recs, step=1) \
			.map(lambda x: create_keys(x, no_keys)) \
			.map(lambda x: create_nonkeys(x, no_nonkeys))

# write out new data 

spark.createDataFrame(addtl_recs_rdd \
		.map(lambda x: flatten_data(x[0], x[1]))) \
		.write.csv(incremental_output, mode="append")		