# gmDataLoad

This project provides a set of tools for the GM POC. It includes three main components
  * Data load script
  * Transaction monitoring
  * Sharding setup and deployment
  
## Data Load

**This script was designed for a replica set. If it used for a sharded cluster with zones and/or presplitting, it will need to be modified so that it does drop collections before loading data.**

The primary data load script is loadData.sh. This script uses mongoimport to load the relevant CSV files into MongoDB. The load process is as follows:
  1. Drop all the collections (see dropCollections.js). Also, this script creates an account_number index on the summary collection to support the join in buildSummary.js (step 5).
  2. Load all the journal files
  3. Load the trial balance summary files
  4. Load the stock record summary files
  5. Build the summaryCol by joining the trial balance summary collection with the summary collection (see buildSummaryCol.js). This creates the denormalized summaryCol document structure used for the POC.

The .txt files in the top level directory define the structure of the CSV files and the types of the columns. These are used as arguments to mongoimport.

### Limitations
  1. Load time could be reduced by executing the mongoimports in parallel instead of sequentially as done now.


## Transaction Monitoring

_These scripts are a work in progress_

Since Atlas does not track transaction commits per second, these scripts are designed to pull this information from MongoDB in real time. These scripts are wrappers around mongostat, which is used to capture transaction commits.

There are two main scripts:
 * process.sh
 * process2.sh
 
These scripts take a different approach to processing the data.
 
### process.sh

**WARNING: I don't think this script will work on a sharded cluster. Use process2.sh**

process.sh uses mongostat to capture the transaction commits on each node of a MongoDB cluster. Therefore, this script needs to be run on the primary of each replica set or on every node in a cluster if performing a failover test. 

The implementation of process.sh is as follows:
  * mongostat with the argument -O='transactions.totalCommitted' is run to capture the total transactions committed on the server
  * jq is used to process the JSON produced by mongostat and extract the # of transaction commits
  * the output of jq is pumped through a simple loop that calculates the commits each second by comparing the current total commits to the total commits from the mongostat output from the previous second. 
process.sh produces a CSV file with 1 row per second, where each row contains the current time and the number of commits that were executed during that second.

#### process.sh arguments
process.h takes 1 argument. The host name of the server being monitored. I typically pipe the output of process.sh to tee so I can view it while logging it to a file as follows:

```bash
./process.sh functionaltest-shard-00-00.zq5bn.mongodb.net | tee test28Aug2020-00-1a.csv
```

After a test is complete, simply ctrl-c to stop execution. The output of process.sh can be loaded into Google Sheets as a CSV and used to create a chart. See https://docs.google.com/spreadsheets/d/1aZ9SQUGkvxqpev5H7shh_ndW1CToj6kL8yJ4Pa490Eo/edit?usp=sharing for an example.

##### Notes
  1. process.sh will only print out information, if transactions are being executed on a server. Nothing will be displayed if there isn't any transaction processing occuring.


### process2.sh

_process2.sh is a work in progress_

process2.sh uses MongoDB charts to visualize the test results. Instead of outputing the results as CSV, process2.sh pushes the output of mongostat directly into a MongoDB cluster by piping it into mongoimport. In addition, this script has the following improvements over process.sh:
  * it collects metrics from all nodes in a cluster
  * it captures all the sharding transaction metrics.

#### process2.sh arguments

process2.sh requires 3 arguments:
  1. TESTNAME - the name of the test. This name will be used as the name of the output file (<argument1.json>) and the collection name in MongoDB where the data is stored
  2. SECONDARY - the hostname of  mongos in a sharded cluster or a secondary in a replica set
  3. PORT - 27016 for mongos and 27017 for replica set members

Run process2.sh as shown below:

```bash
./process2.sh sTest2 shardingtest-shard-00-00.zq5bn.mongodb.net 27016
```

#### Steps for using process2.sh
  1. Update REPORTING_DB_URI variable in process2.sh, if necessary. This is the URI connection string for the MongoDB instance use to collect the monitoring data.
  2. Start process2.sh before the beginning of the test.
  3. After the test is complete, ctr-c to stop process2.sh. Verify that the data has been loaded into the reporting MongoDB instance. If the data has not been loaded, load it by executing the following command:
     ```bash
     mongoimport --uri "$REPORTING_DB_URI" --db gmTests --collection $TESTNAME --type json
     ```
  4. On the MongoDB cluster containing the output of process2.sh, create two views on the TESTNAME collection created in step 3: 
     1. <TESTNAME>Report - This view cleans up the JSON produced by mongostat, correctly types all the information, etc. The aggregation pipeline code for this view can be found in processPipeline.js
	 2. <TESTNAME>TxnReport - This view is defined on the <TESTNAME>Report view. It calculates the transaction deltas. The aggregation pipeline code for this view can be found in transactionDeltas.js
  5. Create the desired charts on the <TESTNAME>TxnReport collection. An example chart is viewable here: https://charts.mongodb.com/charts-runkel-bbjup/public/dashboards/e517606e-b1fc-4476-bd88-ceb34a46e088
  
#### Limitations
  1. Lots of manual steps including:
     * the pipe of mongostat | mongoimport doesn't seem to work (I don't know why). After the test I need to manually run mongoimport to load the data.
	 * two views must be created on the data. Currently, the creation of the views is manual.
	 * the charts to visualize the data must be created manually. I haven't spent a lot of time figuring out what these should look like.
  2. Due to the way mongostat works, metrics are only collected approximately ever 10 seconds on each server. An enhancement would be to have this script spawn a separate mongostat instance to collect data from each node directly.
  3. the <TESTNAME>TxnReport may need to be updated to calculate transactions per second.

## Sharding

The setUpSharding.js script is used to configure sharding so that the following collections are sharded:
  * summaryCol
  * journalCol
  * summary
  * transactionHistory
  * trialBalanceSummary
  
This script is based upon the example here: https://docs.mongodb.com/manual/reference/method/sh.updateZoneKeyRange/#pre-define-zone-range-example
  
All collections are sharded by account number so that all the information for a particular account is on the same shard for each of these collections. In other words, the information for account 1234567890 will be on shard0 for all 5 collections. This ensures that the transactions do not have to execute a two-phase commit across multiple shards.

To implement this, a zone is created for each partition and that zone is assigned to a shard. Each zone is programmatically assigned an account number range. For example, for 3 shards, 3 zones are created. The zone ranges are 1000000000-4000000000, 4000000000-7000000000, 9000000000-9999999999 for zones 0, 1, and 2 respectively. Shards 0 - 2 are assigned to zones 0 - 2, respectively.

This is implemented through the following programmatic steps:
  1. Calculate the zone ranges and associated each shard with a zone (sh.addShardToZone)
  2. Define the zone range for each zone (sh.updateZoneKeyRange). Note, step #1 simply calculates the zone ranges and creates the zones. This step tells MongoDB the range of account numbers for each zone.
  3. Enable sharding in the database
  4. Shard each collection. (The numInitialChunks is ignored so I had to presplit the collection. See Step 5 below.)
  5. Each collection is presplit into 3840 chunks. (A chunk is at max 64 MB and we want the chunks to be roughly 32 MB. The datasize of the collection was assumed to be 120 GB so that gets us to 3840 chunks.) Presplitting will reduce the load time as MongoDB will not have to perform chunk splits during load.
  
**For whatever reason, all the collections except transactionHistory use the field name "account_number". transactionHistory uses accountNumber.**

# WARNINGS
	1. **If a collection is dropped, this entire process will need to be repeated for the collection.**
	2. **See https://jira.mongodb.org/browse/SERVER-17397 for potential issues when trying to drop sharded databases**
