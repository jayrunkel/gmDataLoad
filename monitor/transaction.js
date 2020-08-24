// v4.0.0-rc0/bin/mongo --port 38000

// ****************************************************
// On a sample person collection with two documents _id 1, 2
// Insert new document, _id 3, inside session1 scope
// Understand how the find operation on these scopes change
// from startTransaction to commitTransaction
// ****************************************************

// drop and recreate person collection with 2 documents _id 1, 2
//use test;
db.person.drop();
db.person.insert({"_id": 1, "fname": "fname-1", "lname": "lname-1"});
db.person.insert({"_id": 2, "fname": "fname-2", "lname": "lname-2"});

// create session1 and a collection object using session1 and start a transaction on it
var session1 = db.getMongo().startSession();
var session1PersonColl = session1.getDatabase('test').getCollection('person');
session1.startTransaction({readConcern: {level: 'snapshot'}, writeConcern: {w: 'majority'}});

// insert a document _id 3, inside a transaction/session1
session1PersonColl.insert({"_id": 3, "fname": "fname-3", "lname": "lname-3"});
// WriteResult({ "nInserted" : 1 })

// find the documents from collection and session1
db.person.find()
// { "_id" : 1, "fname" : "fname-1", "lname" : "lname-1" }
// { "_id" : 2, "fname" : "fname-2", "lname" : "lname-2" }

// notice that the insert on session1 is only visible to it.
session1PersonColl.find()
// { "_id" : 1, "fname" : "fname-1", "lname" : "lname-1" }
// { "_id" : 2, "fname" : "fname-2", "lname" : "lname-2" }
// { "_id" : 3, "fname" : "fname-3", "lname" : "lname-3" }

// commit and end the session
session1.commitTransaction()
session1.endSession()

// show the documents after committing the transaction
db.person.find()
// { "_id" : 1, "fname" : "fname-1", "lname" : "lname-1" }
// { "_id" : 2, "fname" : "fname-2", "lname" : "lname-2" }
// { "_id" : 3, "fname" : "fname-3", "lname" : "lname-3" }
