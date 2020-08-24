var numZones = 3;
var numShards = numZones;
var shardPrefix = "atlas-vc1dhn-shard-";
var dbName = "bookdbo";
var colNames = ["bookdbo.summaryCol", "bookdbo.journalCol", "bookdbo.summary", "bookdbo.transactionHistory", "bookdbo.trialBalanceSummary"];  
var accountLower = 1000000000;
var accountUpper = 9999999999;
var numInitialChunks = 3840; // (120 GB per collection / 64 MB chunks) * 2  -- we want the chunks to be half empty
var chunkSplitInterval = Math.floor((accountUpper - accountLower) / numInitialChunks);

var zones = [];

var zoneCardinality = Math.round((accountUpper - accountLower) / numZones);

var db = db.getSiblingDB(dbName);
db.dropDatabase();


var previousUpperBound = 0;
// Assign shards to zones
for (z = 0; z < numZones; z++) {
    let zoneName = "Zone" + z;
    let shardName = shardPrefix + z;
    let lowerBound = z == 0 ? accountLower : previousUpperBound;
    let upperBound = z == numZones - 1 ? accountUpper : lowerBound + zoneCardinality;

    zones.push({zoneName, lowerBound, upperBound});
    previousUpperBound = upperBound;
    
    sh.addShardToZone(shardName, zoneName);
}

printjson(zones);

// set up each shard
colNames.forEach(collection => {
    zones.forEach(zone => {
	if (collection == "bookdbo.transactionHistory") {
	    sh.updateZoneKeyRange(
		collection,
		{accountNumber : zone.lowerBound},
		{accountNumber : zone.upperBound},
		zone.zoneName
	    );
	}
	else {
	    sh.updateZoneKeyRange(
		collection,
		{account_number : zone.lowerBound},
		{account_number : zone.upperBound},
		zone.zoneName
	    );
	}
    });
});

sh.enableSharding(dbName);		 

colNames.forEach(collection => {
    if (collection == "bookdbo.transactionHistory") {
	sh.shardCollection(collection, {accountNumber : 1}, false, {numInitialChunks : numInitialChunks});
    }
    else {
	sh.shardCollection(collection, {account_number : 1}, false, {numInitialChunks: numInitialChunks});
    }
});

print("starting presplit...");
colNames.forEach(collection => {
    print("starting collection: ", collection);
    
    for (x = accountLower + Math.floor(chunkSplitInterval / 2); x < accountUpper; x = x + chunkSplitInterval) {
	if (collection == "bookdbo.transactionHistory") {
	    db.adminCommand({split : collection, middle : {accountNumber : x}});
	}
	else {
	    db.adminCommand({split : collection, middle : {account_number : x}});
	}
   }
});
