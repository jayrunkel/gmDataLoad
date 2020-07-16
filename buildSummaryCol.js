var cursor = db.trialBalanceSummary.aggregate(
[{$lookup: {
  from: 'summary',
  let: {
    aNum: "$account_number",
    sType: "$account_sub_type"
  },
  pipeline: [{
    $match : {
      $expr : {
        $and : [
          { $eq : ["$account_number", "$$aNum"]},
          { $eq : ["$account_sub_type", "$$sType"]}
          ]
      }
    }
  },
    {
      $project : {
        _id : 0,
        account_number : 0,
        account_sub_type : 0,
        basket_id : 0,
        firm : 0
      }
    }
  ],
  as: "positions"
}},{$group: {
  _id: "$account_number",
  sub_types: {
    $push: "$$ROOT"
  }
}},{$addFields: {
  account_number : "$_id",
  sub_types: {
    $map: {
      input: "$sub_types",
      in: {
        account_sub_type : "$$this.account_sub_type",
        basket_id: "$$this.basket_id",
        firm: "$$this.firm",
        balanceSummary: {
          td_balance : "$$this.td_balance",
          sd_balance : "$$this.sd_balance"
        },
        positions: "$$this.positions"
      }
    }
  }
}},
 {
     $project  : {
	 _id : 0
     }
 },
 {$out : "summaryCol"}
], {"allowDiskUse" : true})



