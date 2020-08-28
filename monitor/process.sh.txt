#!/bin/bash

#HOST=functionaltest-shard-00-02.zq5bn.mongodb.net
HOST=$1
PORT=27017
USERNAME=admin
PASSWORD=GreenMeadows

runMongoStat () {
#    echo "start";
    mongostat --host "$HOST" --port "$PORT" --username "$USERNAME" --password "$PASSWORD" --authenticationDatabase admin --ssl --json -O='transactions.totalCommitted' | jq --unbuffered ".[\"$HOST:$PORT\"] | .[\"transactions.totalCommitted\"] | tonumber"
}

# { read n; }< <(runMongoStat)
# echo "$n"


previous=0
while { read n; } do
    diff=$((n - previous))
    if [ $diff -gt 0 ]
    then 
	ts=`date +"%T"`
	echo "$ts, Transaction:, $diff,  : $n - $previous"
	previous=$n
    fi
done < <(runMongoStat)
