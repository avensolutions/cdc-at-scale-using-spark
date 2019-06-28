# Change Data Capture at Scale using Spark

This pattern is fundamentally based upon calculating a deterministic hash of the key and non-key attribute(s), and then using this hash as the basis for comparison.  The hashes are stored with each record in perpetuity as the pattern is premised upon immutable data structures (such as HDFS, S3, GCS, etc).  The pattern provides distillation and reconstitution of data during the process to minimize memory overhead and shuffle I/O for distributed systems, as well as breaking the pattern into discrete stages (designed to minimize the impact to other applications).  This pattern can be used to process delta or full datasets.

A high-level flowchart representing the basic pattern is shown here:

## The Example

The example provided here uses the [Synthetic CDC Data Generator application](https://puturlhere), configuring an incoming set with 5 uuid columns acting as a composite key, and 10 random number columns acting as non key values.  The initial days payload consists of 10,000 records, the subsequent days payload consists of another 10,000 records.  From the initial dataset, a `DELETE` operation was performed at the source system for 20% of records, an `UPDATE` was performed on 40% of the records and the remaining 40% of records were unchanged.  In this case the 20% of records that were deleted at the source, were replaced by new `INSERT` operations creating new keys.   

### Usage
#### Create Test Input Datasets:

    spark-submit synthetic-cdc-data-generator.py 10000 10000 5 10 0.2 0.4 0.4 data/day1 data/day2
  or specify your desired number of records for day 1 and day 2, key fields and non key fields (see LINK)

#### Day 1:

    spark-submit cdc.py config.yaml data/FIFA18_Day_1.csv 2019-06-18

#### Day 2:

    spark-submit cdc.py config.yaml data/FIFA19_Day_2.csv 2019-06-19

### The Results
The output from processing the Day 1 file should be:

    ingesting 10000 new records...
    Writing out all incoming records as INSERT to current object (file:///tmp/sample_data_current)...
    Finished writing out INSERT records to current object (0:00:02.630586)
    Writing out INSERT records to history object (file:///tmp/sample_data_history/2019-06-18)...
    Finished writing out INSERT records to history object (0:00:02.205599)
    Finished CDC Processing!

The output from processing the Day 2 file should be:

    ingesting 10000 new records...
    Writing out INSERT records to current object (file:///tmp/sample_data_current_temp)...
    Finished writing out INSERT records to current object (0:00:39.105516)
    Writing out INSERT records to history object (file:///tmp/sample_data_history/2019-06-19)...
    Finished writing out INSERT records to history object (0:00:05.191047)
    Writing out DELETE records to history object (file:///tmp/sample_data_history/2019-06-19)...
    Finished writing out DELETE records to history object (0:00:00.897985)
    Writing out UPDATE records to current object (file:///tmp/sample_data_current_temp)...
    Finished writing out UPDATE records to current object (0:00:05.292828)
    Writing out UPDATE records to history object (file:///tmp/sample_data_history/2019-06-19)...
    Finished writing out UPDATE records to history object (0:00:04.691484)
    Writing out NOCHANGE records to current object (file:///tmp/sample_data_current_temp)...
    Finished writing out NOCHANGE records to current object (0:00:01.010659)
    Finished CDC Processing!

Inspection of the resultant current state of the players object using:

    df = spark.read.parquet("file:///tmp/sample_data_current_temp")
    df.groupBy(df.operation).count().show()

Produces the following output:

    +---------+-----+
    |operation|count|
    +---------+-----+
    |        U| 4000|
    |        N| 4000|
    |        I| 2000|
    +---------+-----+

## Pattern Details
Details about the pattern and its implementation follow.

### Current and Historical Datasets

The output of each operation will yield a current dataset (that is the current stateful representation of a give object) and a historical dataset partition (capturing the net changes from the previous state in an appended partition).

This is useful, because often consumers will primarily query the latest state of an object.  The change sets (or historical dataset partitions) can be used for more advanced analysis by sophisticated users.


### Type 2 SCDs *(sort of)*

Two operational columns are added to each current and historical object:

**`OPERATION`** (representing the last known operation to the record, valid values include):
`I` (`INSERT`)
`U` (`UPDATE`)
`D` (`DELETE` – hard `DELETE`s, applies to full datasets only)
`X` (Not supplied, applies to delta processing only)
`N` (No change)

**`EFF_START_DATE`**
Since we are assuming immutable data structures and coarse-grained transformations, we only store the effective start date for each record, this is changed as needed with each coarse-grained operation on the current object.  The effective end date is *inferred* by the presence of a new effective start date (or change in the `EFF_START_DATE` value for a given record).


### The Configuration

The configuration for the routine is stored in a YAML document which is supplied as an input argument.  Important attributes included in the configuration are a list of keys and non keys and their datatype (this implementation does type casting as well).  Other important attributes include the table names and file paths for the current and historical data structures.

The configuration is read at the beginning of a routine as an input along with the path of an incoming data file (a CSV file in this case) and a business effective date (which will be used as the `EFF_START_DATE` for new or updated records).

Processing is performed using the specified key and non key attributes and the output datasets (current and historical) are written to columnar storage files (parquet in this case).  This is designed to make subsequent access and processing more efficient.


### Processing Stages
The routine is broken down into discrete stages, with some data shared between stages, the stages are detailed as follows:
#### Stage 1 – Type Cast and Hash Incoming Data
The first step is to create deterministic hashes of the configured key and non key values for incoming data.  The hashes are calculated based upon a list of elements representing the key and non key values using the MD5 algorithm.  The hashes for each record are then stored with the respective record.  Furthermore, the fields are casted their target datatype as specified in the configuration.  Both of these operations can be performed in a single pass of each row using a `map()` operation.

Importantly, hashes are only calculated ***once*** upon arrival of new data, as the hashes are persisted for the life of the data – and the data structures are immutable – the hashes should never change or be invalidated.  
#### Stage 2 – Determine `INSERT`s

Incoming Hashes are compared with previously calculated hash values for the (previous day’s) current object.  If no current object exists for the dataset, then it can be assumed this is a first run.  In this case every record is considered as an `INSERT` with an `EFF_START_DATE` of the business effective date supplied.

If there is a current object, then the key and non key hash values (only the hash values) are read from the current object.  These are then compared to the respective hashes of the incoming data (which should still be in memory).

Given the full outer join:

> `incoming_data(keyhash, nonkeyhash)` 
> `FULL OUTER JOIN` 
> `current_data(keyhash, nonkeyhash) ON keyhash`

Keys which exist in the left entity which do not exist in the right entity must be the results of an `INSERT` operation.

Tag these records with an operation of `I` with an `EFF_START_DATE` of the business effective date, then rejoin only these records with their full attribute payload from the incoming dataset.  Finally, write out these records to the current and historical partition in `overwrite` mode.
#### Stage 3 - Determine `DELETE`s or Missing Records
Referring the previous full outer join operation, keys which exist in the right entity (current object) which do not appear in the left entity (incoming data) will be the result of a (hard) `DELETE` operation if you are processing full snapshots, otherwise if you are processing deltas these would be missing records (possibly because there were no changes at the source).

Tag these records as `D` or `X` respectively with an `EFF_START_DATE` of the business effective date, rejoin these records with their full attribute payload from the current dataset, then write out these records to the current and historical partition in `append` mode.
#### Stage 4 - Determine `UPDATE`s or Unchanged Records
Again, referring to the previous full outer join, keys which exist in both the incoming and current datasets must be either the result of an `UPDATE` or they could be unchanged.  To determine which case they fall under, compare the non key hashes.  If the non key hashes differ, it must have been a result of an `UPDATE` operation at the source, otherwise the record would be unchanged.

Tag these records as `U` or `N` respectively with an `EFF_START_DATE` of the business effective date (in the case of an update - otherwise maintain the current `EFF_START_DATE`), rejoin these records with their full attribute payload from the incoming dataset, then write out these records to the current and historical partition in `append` mode.
### Key Pattern Callouts
A summary of the key callouts from this pattern are:

 - Use the RDD API for iterative record operations (such as type casting and hashing)
 - Persist hashes with the records
 - Use Dataframes for `JOIN` operations
 - Only perform `JOIN`s with the `keyhash` and `nonkeyhash` columns – this minimizes the amount of data shuffled across the network
 - Write output data in columnar (Parquet) format
 - Break the routine into stages, covering each operation, culminating with a `saveAsParquet()` action – this may seem expensive but for large datsets it is more efficient to break down DAGs for each operation
 - Use caching for objects which will be reused between actions

### Metastore Integration
Although not included in this example, you could easily integrate this pattern with a metastore (such as a Hive metastore or AWS Glue Catalog), by using table objects and `ALTER TABLE` statements to add historical partitions.

### Further optimisations
If the incoming data is known to be relatively small (in the case of delta processing for instance), you could consider a `broadcast` join where the smaller incoming data is distributed to all of the different Executors hosting partitions from the current dataset.

Furthermore, you could add a key to the column config to configure a column to be nullable or not.