


// Update the cusip position for the sub_type
function updatePositions(accountSubType, basketId, cusip, firm, tdBalance, sdBalance, costBasis) {

    return {
	$let : {
	    vars : {
		// extract the sub_type object from the array
		subTypeObj: { $arrayElemAt : ["$$subTypeObjs", 0]}
	    },
            in: {
		$let : {
		    vars : {
			// find the cusip position from the positions array
			positionObjs : {
			    $filter : {
				input : "$$subTypeObj.positions",
				cond : {$eq : ["$$this.cusip", cusip]}
			    }
			},
			// build the new position
			newPosition : {
			    cusip: cusip,
			    td_balance: tdBalance,
			    sd_balance: sdBalance,
			    cost_basis: costBasis
			}
		    },
			in: {
			//update the sub_type fields
			$mergeObjects : [
			    "$$subTypeObj",
			    {
				basketId: basketId,
				firm: firm,
				positions : {
				    $cond : {
					// check to see if the cusip position is already in the positions array
					if : {$eq : [{$size : "$$positionObjs"}, 1]},
					// replace the existing cusip position information with the new information
					then: {
					    $map : {
						input: "$$subTypeObj.positions",
						as: "pos",
						in: {
						    $cond : {
							if : {$eq : ["$$pos.cusip", cusip]},
							then: "$$newPosition",
							else: "$$pos"
						    }
						}
					    }
					},
					// add the new cusip position to the positions array
					else: {
					    $concatArrays : ["$$subTypeObj.positions", [ "$$newPosition" ]]
					}
				    }
				}
			    }
			]
		    }
		}
	    }
	}
    }
}

// return a new sub_type document given the values for the sub_type
function buildNewSubType(accountSubType, basketId, cusip, firm, tdBalance, sdBalance, costBasis) {

    let subType = {
        "account_sub_type": accountSubType,
        "basketId": basketId,
        "firm": firm,
	"positions": [
	    {
		"cusip" : cusip,
		"td_balance": tdBalance,
		"sd_balance": sdBalance,
		"cost_basis": costBasis
	    }
	]
    };
    return subType
}



function updateBalanceSummary(
    accountNumber,
    accountSubType,
    basketId,
    cusip,
    firm,
    tdBalance,
    sdBalance,
    costBasis) {

    let col = db.getCollection("summaryCol");

    // Single update statement performs the following logic:
    // 1. Updates the existing subtype and cusip position, if it exists
    // 2. If the subtype exists, but the cusip position does not exist, then a position subdocument for the cusip is added to the positions array
    // 3. If the subtype does not exist, then adds the subtype to the sub_types array with the cusip as the only element in the positions array

    let aggUpdateQuery = [{
				    $set : {
					sub_types : {
					    $let : {
						vars : {
						    // select the sub_type document from the sub_types array
						    subTypeObjs : {
							$ifNull : [{
							    $filter : {
								input : "$sub_types",
								cond : {$eq : ["$$this.account_sub_type", accountSubType]}
							    }
							},
							[],
						      ]
								   
						    }
						},
						in : {
						    $let : {
							vars : {
							    newSubTypeObj : {
								$cond : {
								    if : {$eq : [{$size : "$$subTypeObjs"}, 1]},
								    // if the sub_type exists, then update the cusip position
								    then : updatePositions(accountSubType, basketId, cusip, firm, tdBalance, sdBalance, costBasis),
								    // if the sub_type does not exist, build a new sub_type subdocument
								    else : buildNewSubType(accountSubType, basketId, cusip, firm, tdBalance, sdBalance, costBasis)
								}
							    }
							},
							in : {
							    $cond : {
								if : {$eq : [{$size : "$$subTypeObjs"}, 1]},
								then : {
								    // If the subtype exists, replace the new sub_type subdocument for the existing version
								    $map : {
									input: "$sub_types",
									as: "subtype",
									in: {
									    $cond : {
										if: {$eq : ["$$subtype.account_sub_type", accountSubType]},
										then: "$$newSubTypeObj",
										else: "$$subtype"
									    }
									}
								    }
								},
								// if the subtype does not exist, add the new sub_type subdocument to the array of sub_types
								else : {$concatArrays : [{$ifNull : ["$sub_types", []]}, ["$$newSubTypeObj"]]}
							    }
							}
						    }
						}
					    }
					}
				    }
    }];
    

    
    col.updateOne({_id : accountNumber}, aggUpdateQuery);
    //printjson(aggUpdateQuery);

}

//Example call
updateBalanceSummary("testDoc",
		     "MARGIN",
		     99,
		     "11111111",
		     "NFSC",
		     999,
		     99,
		     1110.00)
		     
