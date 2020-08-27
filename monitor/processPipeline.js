[{$project: {
  _id: 0,
  servers: {
    $map: {
      input: {
        $filter: {
          input: {
            $objectToArray: '$$ROOT'
          },
          as: 'field',
          cond: {
            $ne: [
              '$$field.k',
              '_id'
            ]
          }
        }
      },
      as: 'server',
      'in': '$$server.v'
    }
  }
}}, {$unwind: {
  path: '$servers'
}}, {$addFields: {
  transactions: {
    $map: {
      input: {
        $filter: {
          input: {
            $objectToArray: '$servers'
          },
          as: 'kvPair',
          cond: {
            $eq: [
              {
                $arrayElemAt: [
                  {
                    $split: [
                      '$$kvPair.k',
                      '.'
                    ]
                  },
                  0
                ]
              },
              'transactions'
            ]
          }
        }
      },
      as: 'tPair',
      'in': {
        k: {
          $trim: {
            input: {
              $reduce: {
                input: {
                  $slice: [
                    {
                      $split: [
                        '$$tPair.k',
                        '.'
                      ]
                    },
                    1,
                    100
                  ]
                },
                initialValue: '',
                'in': {
                  $concat: [
                    '$$value',
                    '_',
                    '$$this'
                  ]
                }
              }
            },
            chars: '_'
          }
        },
        v: {
          $convert: {
            input: '$$tPair.v',
            to: 'int',
            onError: 0,
            onNull: 0
          }
        }
      }
    }
  }
}}, {$project: {
  server: {
    $arrayElemAt: [
      {
        $split: [
          '$servers.host',
          ':'
        ]
      },
      0
    ]
  },
  command: {
    $toInt: {
      $arrayElemAt: [
        {
          $split: [
            '$servers.command',
            '|'
          ]
        },
        0
      ]
    }
  },
  conn: {
    $toInt: '$servers.conn'
  },
  'delete': {
    $toInt: {
      $trim: {
        input: '$servers.delete',
        chars: '*'
      }
    }
  },
  getmore: {
    $toInt: '$servers.getmore'
  },
  url: '$servers.host',
  insert: {
    $toInt: {
      $trim: {
        input: '$servers.insert',
        chars: '*'
      }
    }
  },
  query: {
    $toInt: {
      $trim: {
        input: '$servers.query',
        chars: '*'
      }
    }
  },
  repl: '$servers.repl',
  set: '$servers.set',
  time: {
    $dateFromString: {
      dateString: {
        $concat: [
          {
            $arrayElemAt: [
              {
                $split: [
                  {
                    $dateToString: {
                      date: '$$NOW'
                    }
                  },
                  'T'
                ]
              },
              0
            ]
          },
          'T',
          '$servers.time'
        ]
      }
    }
  },
  update: {
    $convert: {
      input: '$servers.update',
      to: 'int',
      onError: 0
    }
  },
  transactions: {
    $arrayToObject: '$transactions'
  }
}}]





