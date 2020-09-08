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
import faker
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

def db_stats(dbclient = "none"):
    global first_time
    global last_doc
    if dbclient == "none":
        user = settings["source"]["username"]
        pwd = settings["source"]["password"]
        s_uri = settings["source"]["uri"]
        s_uri = s_uri.replace("//", f'//{user}:{pwd}@')
        print(f'Source: {s_uri}')
        dbclient = MongoClient(s_uri)
    iters = settings["mongostat_batch_size"]
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
            first_time = False    
        batch.append(res)
        last_doc = res
        time.sleep(1)
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
    #shards = settings["source"]["shards"]
    uri = uri.replace("//", f'//{username}:{password}@')
    client = MongoClient(uri) #&w=majority
    mdb = client[database]
    print(f'Opening {uri}')
    print(f'Performing {batches} batches')
    for iter in range(batches):
        print(f'Gathering stats {settings["mongostat_batch_size"]} times using 1 second interval')     
        result = db_stats()
        print(f'Batch {len(result)} items to do')
        mdb[testname].insert_many(result)
        #print(docs)
            
    client.close()

'''
Sample Aggregation:
[{$match: {
  "transactions.totalCommittedDelta" : {$exists: true}
}}, {$project: {
  localTime: {$dateToString: { format: "%d-%m-%Y|%H:%M:%S", date: "$localTime"}}, 
  'netTransactions' : "$transactions.totalCommittedDelta",
  transactionsFailed: "$transactions.totalAbortedDelta",
  transactionsTotal: "$transactions.totalCommitted",
  inserts: "$opcounters.insertDelta",
  updates: "$opcounters.updateDelta",
  deletes: "$opcounter.deleteDelta",
  queries: "$opcounters.queryDelta",
  getmore: "$opcounters.getmoreDelta"

  }}]
'''