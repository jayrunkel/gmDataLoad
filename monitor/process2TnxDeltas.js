//var col = db.getCollection("shTest08Sep2020-3");
//var colName = "shTest10Sep2020-2"
var colName = "scaleup2M-9-15-20";
var col = db.getCollection(colName);
var viewColName = colName + "MView";

col.createIndex({host: 1, localTime : 1});

var pipeline = [{$lookup: {
    from: colName,
  'let': {
    host: '$host',
    time: '$localTime'
  },
  pipeline: [
    {
      $match: {
        $expr: {
          $and: [
            {
              $eq: [
                '$host',
                '$$host'
              ]
            },
            {
              $lt: [
                '$localTime',
                '$$time'
              ]
            }
          ]
        }
      }
    },
    {
      $sort: {
        localTime: -1
      }
    },
    {
      $limit: 1
    },
    {
      $addFields: {
        'transactions.previousLocalTime': '$localTime'
      }
    },
    {
      $project: {
        _id: 0,
        transactions : 1,
        opcounters : 1
      }
    }
  ],
  as: 'deltas'
}}, {$addFields: {
  normalizedTime: {
    localTime1Sec: {
      $multiply: [
        {
          $trunc: [
            {
              $divide: [
                {
                  $toLong: '$localTime'
                },
                1000
              ]
            },
            0
          ]
        },
        1000
      ]
    },
    localTime10Sec: {
      $multiply: [
        {
          $trunc: [
            {
              $divide: [
                {
                  $toLong: '$localTime'
                },
                10000
              ]
            },
            0
          ]
        },
        10000
      ]
    }
  },
  tnxDeltas: {
    $let: {
      vars: {
        deltaObj: {
          $arrayElemAt: [
            '$deltas',
            0
          ]
        }
      },
      'in': {
        $let: {
          vars: {
            duration: {
              $subtract: [
                '$localTime',
                '$$deltaObj.transactions.previousLocalTime'
              ]
            }
          },
          'in': {
            duration: '$$duration',
            commitTypes_noShards_successful: {
              $subtract: [
                '$transactions.commitTypes.noShards.successful',
                '$$deltaObj.commitTypes_noShards_successful'
              ]
            },
            commitTypes_readOnly_successful: {
              $subtract: [
                '$transactions.commitTypes_readOnly_successful',
                '$$deltaObj.commitTypes_readOnly_successful'
              ]
            },
            commitTypes_recoverWithToken_successful: {
              $subtract: [
                '$transactions.commitTypes_recoverWithToken_successful',
                '$$deltaObj.commitTypes_recoverWithToken_successful'
              ]
            },
            commitTypes_singleShard_successful: {
              $divide: [
                {
                  $subtract: [
                    '$transactions.commitTypes.singleShard.successful',
                    '$$deltaObj.transactions.commitTypes.singleShard.successful'
                  ]
                },
                {
                  $divide: [
                    '$$duration',
                    1000
                  ]
                }
              ]
            },
            commitTypes_singleWriteShard_successful: {
              $subtract: [
                '$transactions.commitTypes_singleWriteShard_successful',
                '$$deltaObj.commitTypes_singleWriteShard_successful'
              ]
            },
            commitTypes_twoPhaseCommit_successful: {
              $subtract: [
                '$transactions.commitTypes_twoPhaseCommit_successful',
                '$$deltaObj.commitTypes_twoPhaseCommit_successful'
              ]
            },
              totalCommittedDelta: {
		  $divide : [
		      {
			  $subtract: [
			      '$transactions.totalCommitted',
			      '$$deltaObj.transactions.totalCommitted'
			  ]
		      },
		      {
			  $divide : [
			      '$$duration',
			      1000
			  ]
		      }
		  ]
	      }
           }
        }
      }
    }
  },
  opDeltas: {
    $let: {
      vars: {
        deltaObj: {
          $arrayElemAt: [
            '$deltas',
            0
          ]
        }
      },
      'in': {
        $let: {
          vars: {
            duration: {
              $subtract: [
                '$localTime',
                '$$deltaObj.transactions.previousLocalTime'
              ]
            }
          },
          'in': {
            duration: '$$duration',
            updateDelta : {
              $divide: [
                {
                  $subtract: [
                    '$opcounters.update',
                    '$$deltaObj.opcounters.update'
                  ]
                },
                {
                  $divide: [
                    '$$duration',
                    1000
                  ]
                }
              ]
            },
	    insertDelta : {
              $divide: [
                {
                  $subtract: [
                    '$opcounters.insert',
                    '$$deltaObj.opcounters.insert'
                  ]
                },
                {
                  $divide: [
                    '$$duration',
                    1000
                  ]
                }
              ]
              },
	      queryDelta : {
              $divide: [
                {
                  $subtract: [
                    '$opcounters.query',
                    '$$deltaObj.opcounters.query'
                  ]
                },
                {
                  $divide: [
                    '$$duration',
                    1000
                  ]
                }
              ]
            },
	      
          }
        }
      }
    }
  }
}}, {$addFields: {
  normalizedTime: {
    localTime1SecMS: '$normalizedTime.localTime1Sec',
    localTime1Sec: {
      $toDate: '$normalizedTime.localTime1Sec'
    },
    localTime10SecMS: '$normalizedTime.localTime10Sec',
    localTime10Sec: {
      $toDate: '$normalizedTime.localTime10Sec'
    }
  }
}},
 {
 $out : viewColName
 }];


col.aggregate(pipeline, {allowDiskUse : true, maxTimeMS : 0})
