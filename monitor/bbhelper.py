#  BB lib - Python Helper
from datetime import datetime
import json
import os
import sys

def init_log():
    logit("#------------- New Run ---------------#")


def logit(message, log_type = "INFO", display_only = True):
    cur_date = datetime.now().strftime("%m/%d/%Y %H:%m:%S")
    stamp = f"{cur_date}|{log_type}> "
    for line in message.splitlines():
        print(f"{stamp}{line.strip()}")

def message_box(msg, mtype = "sep"):
    tot = 80
    start = ""
    res = ""
    msg = msg[0:64] if len(msg) > 65 else msg
    ilen = tot - len(msg)
    if (mtype == "sep"):
        start = f'#{"-" * int(ilen/2)} {msg}'
        res = f'{start}{"-" * (tot - len(start) + 1)}#'
    else:
        res = f'#{"-" * tot}#\n'
        start = f'#{" " * int(ilen/2)} {msg} '
        res += f'{start}{" " * (tot - len(start) + 1)}#\n'
        res += f'#{"-" * tot}#\n'

        logit(res)
        return res

def separator(ilength = 82):
    dashy = "-" * (ilength - 2)
    print(f'#{dashy}#')


def process_args(arglist):
    args = {}
    for arg in arglist:
        pair = arg.split("=")
        if len(pair) == 2:
            args[pair[0].strip()] = pair[1].strip()
        else:
            args[arg] = ""
    return args

def read_json(json_file):
    result = {}
    with open(json_file) as jsonfile:
        result = json.load(jsonfile)
    return result
