SECONDARY=functionaltest-shard-00-01.zq5bn.mongodb.net
TESTNAME=multiRegion1
STARTTIME=`date +"%T"`

mongostat --host=$SECONDARY --port=27017 --authenticationDatabase admin --ssl  --username admin --password GreenMeadows --all --discover --json \
	  -O='transactions.totalCommitted,transactions.commitTypes.singleShard.successful' | tee "$TESTNAME.json" | \
    mongoimport --uri mongodb+srv://admin:power_low12@tradeservice.aamtz.gcp.mongodb.net --db junk --collection $TESTNAME --type json 

