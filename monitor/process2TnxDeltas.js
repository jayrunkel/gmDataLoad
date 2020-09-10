var col = db.getCollection("shTest08Sep2020-3");

var pipeline = [{$lookup: {
  from: 'shTest08Sep2020-3',
  'let': {
    host: '$host',
    time: '$uptimeMillis'
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
                '$uptimeMillis',
                '$$time'
              ]
            }
          ]
        }
      }
    },
    {
      $sort: {
        uptimeMillis: -1
      }
    },
    {
      $limit: 1
    },
    {
      $addFields: {
        'transactions.previousMillis': '$uptimeMillis'
      }
    },
    {
      $replaceRoot: {
        newRoot: '$transactions'
      }
    }
  ],
  as: 'tnxDeltas'
}}, {$addFields: {
  tnxDeltas: {
    $let: {
      vars: {
        tDeltas: {
          $arrayElemAt: [
            '$tnxDeltas',
            0
          ]
        }
      },
      'in': {
        $let: {
          vars: {
            duration: {
              $subtract: [
                '$uptimeMillis',
                '$$tDeltas.previousMillis'
              ]
            }
          },
          'in': {
            duration: '$$duration',
            commitTypes_noShards_successful: {
              $subtract: [
                '$transactions.commitTypes.noShards.successful',
                '$$tDeltas.commitTypes_noShards_successful'
              ]
            },
            commitTypes_readOnly_successful: {
              $subtract: [
                '$transactions.commitTypes_readOnly_successful',
                '$$tDeltas.commitTypes_readOnly_successful'
              ]
            },
            commitTypes_recoverWithToken_successful: {
              $subtract: [
                '$transactions.commitTypes_recoverWithToken_successful',
                '$$tDeltas.commitTypes_recoverWithToken_successful'
              ]
            },
            commitTypes_singleShard_successful: {
              $divide: [
                {
                  $subtract: [
                    '$transactions.commitTypes.singleShard.successful',
                    '$$tDeltas.commitTypes.singleShard.successful'
                  ]
                },
                  {
		      $divide : ['$$duration', 1000]
		  }
              ]
            },
            commitTypes_singleWriteShard_successful: {
              $subtract: [
                '$transactions.commitTypes_singleWriteShard_successful',
                '$$tDeltas.commitTypes_singleWriteShard_successful'
              ]
            },
            commitTypes_twoPhaseCommit_successful: {
              $subtract: [
                '$transactions.commitTypes_twoPhaseCommit_successful',
                '$$tDeltas.commitTypes_twoPhaseCommit_successful'
              ]
            },
            totalCommitted: {
              $subtract: [
                '$transactions.totalCommitted',
                '$$tDeltas.totalCommitted'
              ]
            }
          }
        }
      }
    }
  }
}}
,{
 $out : "Test3WithDeltasMView"
 }
 ]

		

col.aggregate(pipeline, {allowDiskUse : true})
