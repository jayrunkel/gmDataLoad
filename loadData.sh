#!/bin/bash

#Copy from mongoimport URL under command line tools in Atlas
MONGO_URL="TradeService-shard-0/tradeservice-shard-00-00-aamtz.gcp.mongodb.net:27017,tradeservice-shard-00-01-aamtz.gcp.mongodb.net:27017,tradeservice-shard-00-02-aamtz.gcp.mongodb.net:27017"
MDB_USER="admin"
MDB_PASSWORD="GreenMeadows"
DATABASE_NAME="fidelity"

#Input Files
#  Required to Build SummaryCol
TRIAL_BALANCE_SUMMARY_FILES=("trialBalanceSummary-newAccountData.csv")
STOCK_RECORD_SUMMARY_FILES=("stockRecordSummary-executionData-high.csv" "stockRecordSummary-executionData-low.csv" "stockRecordSummary-executionData-med.csv")
#  Required to build journalCol
JOURNAL_FILES=("journal-ClientExecutionData-high3.csv" "journal-ClientExecutionData-low.csv" "journal-ClientExecutionData-med.csv" "journal-clientNewAccountData.csv" "journal-fidelityNewAccountData.csv")

echo "-----------------------------"
echo "LOADING Trial Balance Summary files..."
for i in ${TRIAL_BALANCE_SUMMARY_FILES[@]}; do
    echo "loading $i..."
    echo "mongoimport --host $MONGO_URL --ssl --username $MDB_USER --password $MDB_PASSWORD --authenticationDatabase admin --db $DATABASE_NAME --collection trialBalanceSummary --type csv --ignoreBlanks --columnsHaveTypes --fieldFile balanceSummaryFields.txt --file $i"
done

echo "-----------------------------"
echo "LOADING Stock Record Summary files..."
for i in ${STOCK_RECORD_SUMMARY_FILES[@]}; do
    echo "loading $i..."
    echo "mongoimport --host $MONGO_URL --ssl --username $MDB_USER --password $MDB_PASSWORD --authenticationDatabase admin --db $DATABASE_NAME --collection summary --type csv --ignoreBlanks --columnsHaveTypes --fieldFile stockRecordSummaryFields.txt --file $i"
done

echo "-----------------------------"
echo "LOADING Journal Files files..."
for i in ${JOURNAL_FILES[@]}; do
    echo "loading $i..."
    echo "mongoimport --host $MONGO_URL --ssl --username $MDB_USER --password $MDB_PASSWORD --authenticationDatabase admin --db $DATABASE_NAME --collection journalCol --type csv --ignoreBlanks --columnsHaveTypes --fieldFile journalExecutionFields.txt --file $i"
done
