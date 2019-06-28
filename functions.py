import hashlib
from pyspark.sql.types import * 

def hash_cols(elem_list):
	cols = ""
	m = hashlib.md5()
	for elem in elem_list:
		if elem is None:
			elem = ""
		cols = cols + elem
	m.update(cols.encode())
	return m.hexdigest()	

def process_incoming_recs(data, cols_config, eff_start_date):
	# assume everything is an INSERT
	typed_hashed_rec = []
	key = []
	non_key = []	
	for idx, field in enumerate(data):
		if not cols_config[idx]["ignore"]:
			if cols_config[idx]["key"]:
				key.append(field)
			else:
				non_key.append(field)
		# type cols
		if cols_config[idx]["type"] == "int":
			typed_hashed_rec.append(int(field))
		elif cols_config[idx]["type"] == "float":	
			typed_hashed_rec.append(float(field))
		else:
			typed_hashed_rec.append(str(field))
	# hash keys
	typed_hashed_rec.append(hash_cols(key))
	# non keys	
	typed_hashed_rec.append(hash_cols(non_key))
	# operation
	typed_hashed_rec.append("I")
	# eff_start_date
	typed_hashed_rec.append(eff_start_date)
	return typed_hashed_rec
	
def gen_tgt_schema(cols):
	collist = []
	for col in cols:
		if col["type"] == "string":
			collist.append(StructField(col["name"], StringType(), True))
		elif col["type"] == "int":
			collist.append(StructField(col["name"], IntegerType(), True))
		elif col["type"] == "float":
			collist.append(StructField(col["name"], DoubleType(), True))
		# add support for additional datatypes here...
	# meta cols
	collist.append(StructField("keyhash", StringType(), True))
	collist.append(StructField("nonkeyhash", StringType(), True))
	collist.append(StructField("operation", StringType(), True))
	collist.append(StructField("eff_start_date", StringType(), True))
	return StructType(collist)