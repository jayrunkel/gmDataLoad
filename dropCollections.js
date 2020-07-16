
db.summaryCol.drop();
db.journalCol.drop();
db.summary.drop();
db.trialBalanceSummary.drop();

db.createCollection("summary");
db.summary.createIndex({account_number : 1, account_sub_type: 1});
