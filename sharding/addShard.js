var newShardNum = 3;
var splitZoneName = "Zone" + (newShardNum - 1);
var newZoneName = "newZone" + newShardNum;
var newShardName = "atlas-r602et-shard-" + newShardNum;
var dbName = "bookdbo";
var colNames = ["bookdbo.summaryCol", "bookdbo.journalCol", "bookdbo.summary", "bookdbo.transactionHistory", "bookdbo.trialBalanceSummary"];

// Stop the balancer
sh.stopBalancer();

//After creating a shard and add a new zone and associate shard to that new zone
//sh.addShardToZone(newShardName, newZoneName);

print("Zone to split: ", splitZoneName);
print("New zone: ", newZoneName);
print("New shard name: ", newShardName);

sh.addShardToZone(newShardName, newZoneName);
colNames.forEach(collection => {
    if (collection == (dbName + ".transactionHistory")) {
	sh.removeRangeFromZone(collection,
			       {accountNumber : 7000000000},
			       {accountNumber : 9999999999},
			       splitZoneName);
	sh.updateZoneKeyRange(collection,
			      {accountNumber : 7000000000},
			      {accountNumber : 9000000000},
			      splitZoneName);
	sh.updateZoneKeyRange(collection,
			      {accountNumber : 9000000000},
			      {accountNumber : 9999999999},
			      newZoneName);
    }
    else {
	sh.removeRangeFromZone(collection,
			       {account_number : 7000000000},
			       {account_number : 9999999999},
			       splitZoneName);
	sh.updateZoneKeyRange(collection,
			      {account_number : 7000000000},
			      {account_number : 9000000000},
			      splitZoneName);
	sh.updateZoneKeyRange(collection,
			      {account_number : 9000000000},
			      {account_number : 9999999999},
			      newZoneName);
    }
	
})

// Start the balancer
//sh.startBalancer();
