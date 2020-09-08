#!/bin/bash

# Example ./process2.sh sTest2 shardingtest-shard-00-00.zq5bn.mongodb.net 27016 
TESTNAME=$1
#SECONDARY=functionaltest-shard-00-01.zq5bn.mongodb.net
SECONDARY=$2			# OR mongos in a sharded cluster
#PORT=27017
PORT=$3				# 27016 for Atlas cluster mongos
STARTTIME=`date +"%T"`
REPORTING_DB_URI=mongodb+srv://admin:power_low12@tradeservice.aamtz.gcp.mongodb.net
#REPORTING_DB_URI=mongodb+srv://main_admin:bugsyBoo@m10basicagain.vmwqj.mongodb.net/gm_test
ITERATIONS=2
NUMROWS=100
inc=0
while [ $inc -le $ITERATIONS ] 
do
    rm -f "$TESTNAME.json"
    mongostat --host=$SECONDARY --port=$PORT --authenticationDatabase admin --ssl  --username admin --password GreenMeadows --rowcount=$NUMROWS --all --discover --json \
	  -O='transactions.totalCommitted,transactions.commitTypes.noShards.successful,transactions.commitTypes.singleShard.successful,transactions.commitTypes.singleWriteShard.successful,transactions.commitTypes.readOnly.successful,transactions.commitTypes.twoPhaseCommit.successful,transactions.commitTypes.recoverWithToken.successful,opcounters,extra_info.user_time_us,extra_info.system_time_us' \
       | tee "$TESTNAME.json"
    mongoimport --uri "$REPORTING_DB_URI" --collection $TESTNAME --type json --file "$TESTNAME.json"
    ((inc=$inc+1))
done



# mongoimport --uri "mongodb+srv://main_admin:bugsyBoo@m10basicagain.vmwqj.mongodb.net/gm_test" --collection sTest2 --type json
# mongostat --uri mongodb+srv://admin:GreenMeadows@shardingtest.zq5bn.mongodb.net 

#mongostat --host=$SECONDARY --port=$PORT --authenticationDatabase admin --ssl  --username admin --password GreenMeadows --rowcount=$NUMROWS --all --discover --json \
#	  -O='transactions.totalCommitted,transactions.commitTypes.noShards.successful,transactions.commitTypes.singleShard.successful,transactions.commitTypes.singleWriteShard.successful,transactions.commitTypes.readOnly.successful,transactions.commitTypes.twoPhaseCommit.successful,transactions.commitTypes.recoverWithToken.successful,opcounters,extra_info.user_time_us,extra_info.system_time_us'
