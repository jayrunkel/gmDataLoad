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
import bbhelper as bb
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



def create_people(c_item):
    p = multiprocessing.current_process()
    start_time = datetime.now()
    pinfo = f'P[{p.name}|{p.pid}]'
    bb.message_box("New Thread")
    bb.logit("Starting Process %s at %s" % (pinfo, start_time))
    bb.logit("Working on %s" % c_item)
    client = MongoClient(f'mongodb+srv://{settings["username"]}:{settings["password"]}@{settings["mdb_url"]}/test?retryWrites=true') #&w=majority
    #client = MongoClient("mongodb://localhost:27017/test")
    mdb = client[settings["database"]]
    inc = 0
    cursor = mdb.restaurants.find({"cuisine" : c_item})
    count = cursor.count()
    while inc != count:
        #Load rest
        arr = []
        curdoc = cursor[inc]
        res = mdb.people.find({"restaurant_id" : curdoc["_id"]})
        if(res.count() == 0):
            bb.logit(f'{pinfo}OP[{inc}] Update record: {curdoc["name"]}')
            for peep in curdoc["people"]:
                doc = OrderedDict()
                doc["_id"] = peep["people_id"]
                doc["restaurant_id"] = curdoc["_id"]
                doc["email"] = fake.email()
                doc["name"] = peep["name"]
                doc["age"] = random.randint(19, 70)
                doc["timestamp"] = fake.date_time_this_decade()
                arr.append(doc)
            mdb["people"].insert_many(arr)
        inc += 1
        
def load_worker(records, info = {}):
    # client init in each thread
    p = multiprocessing.current_process()
    start_time = datetime.now()
    pinfo = f'P[{p.name}|{p.pid}]'
    bb.message_box("New Thread")
    bb.logit("Starting Process %s at %s" % (pinfo, start_time))
    bb.logit("Inserting %d records" % records)
    client = MongoClient(f'mongodb+srv://{settings["username"]}:{settings["password"]}@{settings["mdb_url"]}/test?retryWrites=true') #&w=majority
    #client = MongoClient("mongodb://localhost:27017/test")
    mdb = client[settings["database"]]
    claim_json = read_json(settings["collections"]["claim"]["template"])
    for inc in range(records):
        bb.logit(f'{pinfo} Iteration: {inc}')
        #op_get("member", mdb, pinfo, inc)
        op_peopleupdate("restaurant", mdb, pinfo, inc)
        #op_get("claim", mdb, pinfo, inc)
        #op_update("claim", mdb, pinfo, inc)
        #op_add("claim", mdb, pinfo, inc, claim_json)
    bb.logit("Ending Process %s at %s" % (pinfo, datetime.now()))
    mdb.close()

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

def process_manager():
    # read settings and echo back
    bb.message_box("Load Test Run", "title")
    bb.logit(f'# Settings from: {settings_file}')
    for item in settings:
        bb.logit(f'# {item} => {settings[item]}')
    # Spawn processes
    shards = shard_map("source")
    jobs = []
    inc = 0
    for item in shards:
        bb.logit(f'Item: {item}')
        p = multiprocessing.Process(target=db_stats, args = (item,))
        jobs.append(p)
        p.start()
        inc += 1

    main_process = multiprocessing.current_process()
    bb.logit('Main process is %s %s' % (main_process.name, main_process.pid))
    for i in jobs:
        i.join()

def db_stats(shard):
    global first_time
    global last_doc
    p = multiprocessing.current_process()
    start_time = datetime.now()
    pinfo = f'P[{p.name}|{p.pid}]'
    bb.message_box('New Thread')
    bb.logit("Starting Process %s at %s" % (pinfo, start_time))
    bb.logit("Working on shard %s" % shard)
    batches = settings["batches"]
    testname = ARGS["testname"]
    l_client = db_conn("logger")
    mdb = l_client[settings["logger"]["database"]][testname]
    s_client = db_conn("source", shard)
    bb.logit(f'Gathering {batches} batches of {settings["batch_size"]} times using 1 second interval')
    first_time = True  
    for iter in range(batches):
        result = stat_batch(s_client, first_time)
        bb.logit(f'{pinfo}| Batch {len(result)} items to do')
        mdb.insert_many(result)
        first_time = False
    bb.logit(f'{pinfo}| - complete')
    

def stat_batch(conn, first_time):
    global last_doc
    iters = settings["batch_size"]
    batch = []
    for inc in range(iters):
        res = conn.admin.command("serverStatus")
        #print("--- Status Output ---")
        #print(res)
        #print(".", end="", flush=True)
        del res["transportSecurity"]
        del res["metrics"]["aggStageCounters"]
        del res["$clusterTime"]
        del res["$gleStats"]
        del res["$configServerState"]
        if not first_time:
            res["opcounters"]["insertDelta"] = res["opcounters"]["insert"] - last_doc["opcounters"]["insert"]
            res["opcounters"]["updateDelta"] = res["opcounters"]["update"] - last_doc["opcounters"]["update"]
            res["opcounters"]["deleteDelta"] = res["opcounters"]["delete"] - last_doc["opcounters"]["delete"]
            res["opcounters"]["queryDelta"] = res["opcounters"]["query"] - last_doc["opcounters"]["query"]
            res["opcounters"]["getmoreDelta"] = res["opcounters"]["getmore"] - last_doc["opcounters"]["getmore"]
            res["opcounters"]["commandDelta"] = res["opcounters"]["command"] - last_doc["opcounters"]["command"]
            res["transactions"]["totalCommittedDelta"] = res["transactions"]["totalCommitted"] - last_doc["transactions"]["totalCommitted"]
            res["transactions"]["totalStartedDelta"] = res["transactions"]["totalStarted"] - last_doc["transactions"]["totalStarted"]
            res["transactions"]["totalAbortedDelta"] = res["transactions"]["totalAborted"] - last_doc["transactions"]["totalAborted"]
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
        batch.append(res)
        last_doc = res
        time.sleep(1)
    print(" done")
    return batch

def shard_map(db):
    conn = db_conn(db)
    shards = []
    res = conn["config"]["shards"].find({})
    for item in res:
        shards.append(item["host"])
    return(shards)


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
 
def db_conn(c_type = "logger", alt_host = ""):
    uri = settings[c_type]["uri"]
    username = settings[c_type]["username"]
    password = settings[c_type]["password"]    
    if alt_host == "":
        uri = uri.replace("//", f'//{username}:{password}@')
        client = MongoClient(uri)
    else:
        client = MongoClient(host=alt_host, connect=True, username=username, password=password, ssl=True)
    bb.logit(f'Opening {uri}')
    return client
       

def run_shell(cmd = ["ls", "-l"]):
    result = subprocess.run(cmd, capture_output=True)
    print("The exit code was: %d" % result.returncode)
    print("#--------------- STDOUT ---------------#")
    print(result.stdout)
    if result.stderr:
        print("#--------------- STDERR ---------------#")
        print(result.stderr)
    return result

def clean_key(key):
    return key.replace(":","-").replace(".","_").replace("@","").replace(",","")

def run_mongostat():
    host = settings["source"]["host"]
    port = settings["source"]["port"]
    username = settings["source"]["username"]
    password = settings["source"]["password"]
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


#-----------------------------------------------------------#
#------------------------  MAIN ----------------------------#
if __name__ == "__main__":
    ARGS = process_args(sys.argv)
    settings = read_json(settings_file)
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "monitor":
        process_manager()
    elif ARGS["action"] == "recalculate":
        print("Recalculating data")
        rebuild_stats()

    else:
        print(f'{ARGS["action"]} not found')
    
            

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