
var pipeline = 
[{$project: {
  _id : 0
}}, {$addFields: {
  copy: 1,
}}, {$merge: {
  into: "journalCol",
}}];

db.journalCol.aggregate(pipeline, {allowDiskUse : true})
