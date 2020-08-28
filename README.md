# gmDataLoad

This project provides a set of tools for the GM POC. It includes three main components
  * Data load script
  * Transaction monitoring
  * Sharding setup and deployment
  
## Data Load

**This script was designed for a replica set. If it used for a sharded cluster with zones and/or presplitting, it will need to be modified so that it does drop collections before loading data.**

The primary data load script is loadData.sh. This script uses mongoimport to load the relevant CSV files into MongoDB. The load process is as follows:
  1. Drop all the collections (see dropCollections.js). Also creates an account_number index on the summary collection to support the join in buildSummary.js (step 5).
  2. Load all the journal files
  3. Load the trial balance summary files
  4. Load the stock record summary files
  5. Build the summaryCol by joining the trial balance summary collection with the summary collection (see buildSummaryCol.js)

The .txt files in the top level directory define the structure of the CSV files and the types of the columns. These are used as arguments to mongoimport.


## Transaction Monitoring



## Sharding

The setUpSharding.js script is used to configure sharding so that the following collections are sharded:
  * summaryCol
  * journalCol
  * summary
  * transactionHistory
  * trialBalanceSummary
  
This script is based upon the example here: https://docs.mongodb.com/manual/reference/method/sh.updateZoneKeyRange/#pre-define-zone-range-example
  
In addition, these collections are sharded by account number so that all the information for a particular account is on the same shard for each of these collections. In other words, the information for account 1234567890 will be on shard0 for all 5 collections. This ensures that the transactions do not have to execute a two-phase commit across multiple shards.

To implement this, a zone is created for each partition and that zone is assigned to a shard. Each zone is programmatically assigned an account number range. For example, for 3 shards, 3 zones are created. The zone ranges are 1000000000-4000000000, 4000000000-7000000000, 9000000000-9999999999 for zones 0, 1, and 2 respectively. Shards 0 - 2 are assigned to zones 0 - 2, respectively.

This is implemented through the following programmatic steps:
  1. Calculate the zone ranges and associated each shard with a zone (sh.addShardToZone)
  2. Define the zone range for each zone (sh.updateZoneKeyRange). Note, step #1 simply calculated teh zone ranges and created the zones. Step 2 tells MongoDB the range of account numbers for each zone.
  3. Enable sharding in the database
  4. Shard each collection. (The numInitialChunks is ignored so I had to presplit the collection. See Step 5 below.)
  5. Each collection is presplit into 3840 chunks. (A chunk is at max 64 MB and we want the chunks to be roughly 32 MB. The datasize of the collection was assumed to be 120 GB so that got us 3840 chunks.) Presplitting will reduce the load time as MongoDB will not have to perform splits during load.
  
**For whatever reason, all the collections except transactionHistory use the field name "account_number". transactionHistory uses accountNumber.**

# WARNINGS
	1. ** If a collection is dropped, this entire process will need to be repeated for the collection.
	2. See https://jira.mongodb.org/browse/SERVER-17397 for potential issues when trying to drop sharded databases
