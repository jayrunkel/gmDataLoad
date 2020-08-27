[{$match: {
  repl: 'RTR'
}}, {$lookup: {
  from: 'sTest1Report',
  'let': {
    server: '$server',
    time: '$time'
  },
  pipeline: [
    {
      $match: {
        $expr: {
          $and: [
            {
              $eq: [
                '$server',
                '$$server'
              ]
            },
            {
              $lt: [
                '$time',
                '$$time'
              ]
            }
          ]
        }
      }
    },
    {
      $sort: {
        time: -1
      }
    },
    {
      $limit: 1
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
        commitTypes_noShards_successful: {
          $subtract: [
            '$transactions.commitTypes_noShards_successful',
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
          $subtract: [
            '$transactions.commitTypes_singleShard_successful',
            '$$tDeltas.commitTypes_singleShard_successful'
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
}}]
