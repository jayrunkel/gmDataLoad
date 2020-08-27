#!/bin/bash


TESTNAME=$1
#SECONDARY=functionaltest-shard-00-01.zq5bn.mongodb.net
SECONDARY=$2			# OR mongos in a sharded cluster
#PORT=27017
PORT=$3				# 27016 for Atlas cluster mongos
STARTTIME=`date +"%T"`
REPORTING_DB_URI=mongodb+srv://admin:power_low12@tradeservice.aamtz.gcp.mongodb.net

mongostat --host=$SECONDARY --port=$PORT --authenticationDatabase admin --ssl  --username admin --password GreenMeadows --all --discover --json \
	  -O='transactions.totalCommitted,transactions.commitTypes.noShards.successful,transactions.commitTypes.singleShard.successful,transactions.commitTypes.singleWriteShard.successful,transactions.commitTypes.readOnly.successful,transactions.commitTypes.twoPhaseCommit.successful,transactions.commitTypes.recoverWithToken.successful' | tee "$TESTNAME.json" | \
    mongoimport --uri "$REPORTING_DB_URI" --db gmTests --collection $TESTNAME --type json 



