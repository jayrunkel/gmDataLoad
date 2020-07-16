#!/bin/bash

#Copy from mongoimport URL under command line tools in Atlas
MONGOIMPORT_URL="atlas-12nzn7-shard-0/functionaltest-shard-00-00.zq5bn.mongodb.net:27017,functionaltest-shard-00-01.zq5bn.mongodb.net:27017,functionaltest-shard-00-02.zq5bn.mongodb.net:27017"
MDB_USER="admin"
MDB_PASSWORD="GreenMeadows"
DATABASE_NAME="loadTest"
#Copy from mongoshell connection string under the Connect button in Atlas
MONGOSHELL_URL="mongodb+srv://functionaltest.zq5bn.mongodb.net/$DATABASE_NAME"
DATA_DIR='/tmp/csv-mongo'   	# Directory that contains csv files
THREADS=2			# # of threads dedicated to mongoimport


#Input Files
#  Required to Build SummaryCol
TRIAL_BALANCE_SUMMARY_FILES=("trialBalanceSummary-newAccountData.csv")
STOCK_RECORD_SUMMARY_FILES=("stockRecordSummary-executionData-high.csv" "stockRecordSummary-executionData-low.csv" "stockRecordSummary-executionData-med.csv")
#  Required to build journalCol
JOURNAL_FILES=("journal-clientExecutionData-high3.csv" "journal-clientExecutionData-low.csv" "journal-clientExecutionData-med.csv" "journal-clientNewAccountData.csv" "journal-fidelityNewAccountData.csv")

echo "-----------------------------"
echo "Drop existing collections"
mongo $MONGOSHELL_URL dropCollections.js --username $MDB_USER --password $MDB_PASSWORD

echo "-----------------------------"
echo "LOADING Journal Files files..."
for i in ${JOURNAL_FILES[@]}; do
    echo "loading $i..."
    sed 's/\\\N//' "$DATA_DIR/$i" > "$DATA_DIR/${i}Clean"
    mongoimport --host $MONGOIMPORT_URL --ssl --username $MDB_USER --password $MDB_PASSWORD --authenticationDatabase admin --db $DATABASE_NAME --collection journalCol --type csv --ignoreBlanks --columnsHaveTypes --fieldFile journalExecutionFields.txt --numInsertionWorkers $THREADS --file "$DATA_DIR/${i}Clean"
done


echo "-----------------------------"
echo "LOADING Trial Balance Summary files..."
for i in ${TRIAL_BALANCE_SUMMARY_FILES[@]}; do
    echo "loading $i..."
    echo "mongoimport --host $MONGOIMPORT_URL --ssl --username $MDB_USER --password $MDB_PASSWORD --authenticationDatabase admin --db $DATABASE_NAME --collection trialBalanceSummary --type csv --ignoreBlanks --columnsHaveTypes --fieldFile balanceSummaryFields.txt --file $i"
    mongoimport --host $MONGOIMPORT_URL --ssl --username $MDB_USER --password $MDB_PASSWORD --authenticationDatabase admin --db $DATABASE_NAME --collection trialBalanceSummary --type csv --ignoreBlanks --columnsHaveTypes --fieldFile balanceSummaryFields.txt --numInsertionWorkers $THREADS --file "$DATA_DIR/$i"
done

echo "-----------------------------"
echo "LOADING Stock Record Summary files..."
for i in ${STOCK_RECORD_SUMMARY_FILES[@]}; do
    echo "loading $i..."
    mongoimport --host $MONGOIMPORT_URL --ssl --username $MDB_USER --password $MDB_PASSWORD --authenticationDatabase admin --db $DATABASE_NAME --collection summary --type csv --ignoreBlanks --columnsHaveTypes --fieldFile stockRecordSummaryFields.txt --numInsertionWorkers $THREADS --file "$DATA_DIR/$i"
done

echo " "
echo "DATA LOAD COMPLETE"
echo "-----------------------------"
echo "Building SummaryCol"
mongo $MONGOSHELL_URL buildSummaryCol.js --username $MDB_USER --password $MDB_PASSWORD
