# Python Atlas API
import sys
import csv
from collections import OrderedDict
from collections import defaultdict
import json
import logging
import datetime
import random
import uuid
import os
import io
import subprocess
import time
import re
import multiprocessing
import pprint
import getopt
from contextlib import redirect_stdout
from bson.objectid import ObjectId
#import bbhelper as bb
from datetime import datetime
from pymongo import MongoClient
settings = {}
settings_file = "monitor_settings.json"
last_vals = {}
last_doc = {}
first_time = True

def read_json(json_file):
    result = {}
    with open(json_file) as jsonfile:
        result = json.load(jsonfile)
    return result

def process_args(arglist):
    args = {}
    for arg in arglist:
        pair = arg.split("=")
        if len(pair) == 2:
            args[pair[0].strip()] = pair[1].strip()
        else:
            args[arg] = ""
    return args

def fix_vals(curdoc):
    curdoc["conn"] = int(curdoc["conn"])
    curdoc["flushes"] = int(curdoc["flushes"])
    curdoc["getmore"] = int(curdoc["getmore"])
    #curdoc["extra_infoSystem_time_us"] = int(curdoc.pop("extra_info.system_time_us"))
    #del curdoc["extra_info.system_time_us"]
    #curdoc["extra_infoUser_time_us"] = int(curdoc["extra_info.user_time_us"])
    #del curdoc["extra_info.user_time_us"]
    cur_trans = int(curdoc["transactions.totalCommitted"])
    if "host" in curdoc:
        print(f'Host: {curdoc["host"]}')
        host = curdoc["host"]
        diff = 0
        if host in last_vals:
            diff = cur_trans - last_vals[host]
        curdoc["transactionsNet"] = diff
    
    curdoc["transactionsTotalCommitted"] = cur_trans
    del curdoc["transactions.totalCommitted"]
    #curdoc["insert"] = int(curdoc["insert"].replace("*",""))
    #curdoc["update"] = int(curdoc["update"].replace("*",""))
    #curdoc["delete"] = int(curdoc["delete"].replace("*",""))
    curdoc["query"] = int(curdoc["query"].replace("*",""))
    curdoc["version"] = "1.0"
    return(curdoc)

def clean_key(key):
    return key.replace(":","-").replace(".","_").replace("@","").replace(",","")

def run_mongostat():
    host = settings["shardsource"]["host"]
    port = settings["shardsource"]["port"]
    username = settings["shardsource"]["username"]
    password = settings["shardsource"]["password"]
    numrows = settings["mongostat_batch_size"]
    options = settings["mongostat_options"]
    cmd = ["mongostat", "--host", f'"{host}"', "--port", f'"{port}"', "--username", f'"{username}"', "--password", f'"{password}"', "--authenticationDatabase", "admin", "--ssl", "--discover", f'--rowcount={numrows}', "--json", f'-O="{options}"']
    result = run_shell(cmd)
    fixed = result.stdout
    fixed = fixed.decode()
    fixed = fixed.replace("\n{",",\n{")
    fixed = f'[{fixed}]'
    #print("Processed ------------------------------------")
    #print(fixed)
    stats = json.loads(fixed)
    return stats

def db_stats(dbclient = "none"):
    global last_doc
    global first_time
    global last_doc
    if dbclient == "none":
        user = settings["shardsource"]["username"]
        pwd = settings["shardsource"]["password"]
        s_uri = settings["shardsource"]["uri"]
        s_uri = s_uri.replace("//", f'//{user}:{pwd}@')
        print(f'Source: {s_uri}')
        dbclient = MongoClient(s_uri)
    iters = settings["batch_size"]
    batch = []
    for inc in range(iters):
        res = dbclient.admin.command("serverStatus")
        #print("--- Status Output ---")
        #print(res)
        print(".", end="", flush=True)
        del res["transportSecurity"]
        del res["metrics"]["aggStageCounters"]
        del res["$clusterTime"]
        if not first_time: #"ok" in last_doc:
            res["opcounters"]["insertDelta"] = res["opcounters"]["insert"] - last_doc["opcounters"]["insert"]
            res["opcounters"]["updateDelta"] = res["opcounters"]["update"] - last_doc["opcounters"]["update"]
            res["opcounters"]["deleteDelta"] = res["opcounters"]["delete"] - last_doc["opcounters"]["delete"]
            res["opcounters"]["queryDelta"] = res["opcounters"]["query"] - last_doc["opcounters"]["query"]
            res["opcounters"]["getmoreDelta"] = res["opcounters"]["getmore"] - last_doc["opcounters"]["getmore"]
            res["opcounters"]["commandDelta"] = res["opcounters"]["command"] - last_doc["opcounters"]["command"]
            res["transactions"]["totalCommittedDelta"] = res["transactions"]["totalCommitted"] - last_doc["transactions"]["totalCommitted"]
            res["transactions"]["totalStartedDelta"] = res["transactions"]["totalStarted"] - last_doc["transactions"]["totalStarted"]
            res["transactions"]["totalAbortedDelta"] = res["transactions"]["totalAborted"] - last_doc["transactions"]["totalAborted"]
            res["transactions"]["commitTypes"]["singleShard"]["successfulDelta"] =  res["transactions"]["commitTypes"]["singleShard"]["successful"] -  last_doc["transactions"]["commitTypes"]["singleShard"]["successful"]
        else:
            res["opcounters"]["insertDelta"] = 0
            res["opcounters"]["updateDelta"] = 0
            res["opcounters"]["deleteDelta"] = 0
            res["opcounters"]["queryDelta"] = 0
            res["opcounters"]["getmoreDelta"] = 0
            res["opcounters"]["commandDelta"] = 0
            res["transactions"]["totalCommittedDelta"] = 0
            res["transactions"]["totalStartedDelta"] = 0
            res["transactions"]["totalAbortedDelta"] = 0
            res["transactions"]["commitTypes"]["singleShard"]["successfulDelta"] = 0
            first_time = False    
        batch.append(res)
        last_doc = res
        time.sleep(1)
    print(" done")
    return batch

def rebuild_stats():
    global first_time
    global last_doc
    iters = settings["mongostat_batch_size"]
    batch = []
    query_result = mdb[testname].find({}).sort({"uptimeMillis" : 1})
    for doc in query_result:
        subdoc = OrderedDict()
        subdoc["uptimeMillis"] = doc["uptimeMillis"]
        if first_time:
            subdoc["insertDelta"] = 0
            subdoc["updateDelta"] = 0
            subdoc["deleteDelta"] = 0
            subdoc["queryDelta"] = 0
            subdoc["getmoreDelta"] = 0
            subdoc["commandDelta"] = 0
            subdoc["totalCommittedDelta"] = 0
            subdoc["totalStartedDelta"] = 0
            subdoc["totalAbortedDelta"] = 0
            first_time = False    
        else:
            subdoc["insertDelta"] = doc["opcounters"]["insert"] - last_doc["opcounters"]["insert"]
            subdoc["updateDelta"] = doc["opcounters"]["update"] - last_doc["opcounters"]["update"]
            subdoc["deleteDelta"] = doc["opcounters"]["delete"] - last_doc["opcounters"]["delete"]
            subdoc["queryDelta"] = doc["opcounters"]["query"] - last_doc["opcounters"]["query"]
            subdoc["getmoreDelta"] = doc["opcounters"]["getmore"] - last_doc["opcounters"]["getmore"]
            subdoc["commandDelta"] = doc["opcounters"]["command"] - last_doc["opcounters"]["command"]
            subdoc["totalCommittedDelta"] = doc["transactions"]["totalCommitted"] - last_doc["transactions"]["totalCommitted"]
            subdoc["totalStartedDelta"] = doc["transactions"]["totalStarted"] - last_doc["transactions"]["totalStarted"]
            subdoc["totalAbortedDelta"] = doc["transactions"]["totalAborted"] - last_doc["transactions"]["totalAborted"]

        mdb[testname].update({"_id" : doc["_id"]},{"$set" : {"calculatedStats" : subdoc} })
        batch.append(subdoc)
        last_doc = doc
    print(" done")
    return batch
 
   
def run_shell(cmd = ["ls", "-l"]):
    result = subprocess.run(cmd, capture_output=True)
    print("The exit code was: %d" % result.returncode)
    print("#--------------- STDOUT ---------------#")
    print(result.stdout)
    if result.stderr:
        print("#--------------- STDERR ---------------#")
        print(result.stderr)
    return result


#-----------------------------------------------------------#
#------------------------  MAIN ----------------------------#
if __name__ == "__main__":
    ARGS = process_args(sys.argv)
    settings = read_json(settings_file)
    uri = settings["logger"]["uri"]
    username = settings["logger"]["username"]
    password = settings["logger"]["password"]
    database = settings["logger"]["database"]
    batches = settings["batches"]
    testname = ARGS["testname"]
    #shards = settings["shardsource"]["shards"]
    uri = uri.replace("//", f'//{username}:{password}@')
    client = MongoClient(uri) #&w=majority
    mdb = client[database]
    print(f'Opening {uri}')
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "monitor":
        print(f'Performing {batches} batches')
        for iter in range(batches):
            print(f'Gathering stats {settings["mongostat_batch_size"]} times using 1 second interval')     
            result = db_stats()
            print(f'Batch {len(result)} items to do')
            mdb[testname].insert_many(result)
            #print(docs)
    elif ARGS["action"] == "recalculate":
        print("Recalculating data")
        rebuild_stats()

    else:
        print(f'{ARGS["action"]} not found')
    
            
    client.close()

'''
Sample Aggregation:
[{$match: {
  "transactions.totalCommittedDelta" : {$exists: true}
}}, {$project: {
  host : 1,
  localTime: {$dateToString: { format: "%d-%m-%Y|%H:%M:%S", date: "$localTime"}}, 
  uptimeMillis : 1,
  transactions : "$transactions.commitTypes.singleShard.successful",
  transactionsinit : "$transactions.commitTypes.singleShard.initiated",
  'netTransactions' : "$transactions.totalCommittedDelta",
  transactionsFailed: "$transactions.totalAbortedDelta",
  transactionsTotal: "$transactions.totalCommitted",
  inserts: "$opcounters.insertDelta",
  updates: "$opcounters.updateDelta",
  deletes: "$opcounter.deleteDelta",
  queries: "$opcounters.queryDelta",
  getmore: "$opcounters.getmoreDelta"

  }},
  {$sort: {uptimeMillis: -1}}]
'''