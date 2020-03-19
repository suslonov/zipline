#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 15:59:06 2020

@author: anton
"""

import os
if  "__file__" in globals():
    os.sys.path.append(os.path.dirname(os.path.abspath(__file__)))
else:
    os.sys.path.append(os.path.abspath('.'))

import importlib
from datetime import datetime
import io
from contextlib import redirect_stdout, redirect_stderr
import pandas as pd

import zipline
import zipline_utils

algorithms_dir = "algorithms"

def zipline_launcher(alg_name, run_params, algorithm_params):
    alg = importlib.import_module(algorithms_dir+"."+alg_name)

    algorithm_context = alg.algorithm_context(algorithm_params)
    params = {**run_params, **algorithm_context.functions_dict()}

    x = zipline.run_algorithm(**params)
    more_output = algorithm_context.records()
    return x, more_output

alg_name = "VIX+Bonds+Indicators+Stops 1"
run_comment = "Test run on full data test comment"

algorithm_params = {"WINDOW_MIN": 5, "VIX_SHARE": 0.6, "VIX_OPEN_LEVEL": 15, "stop_limit": 0.075, "MA": 100}

run_params = {}
#run_params["start"] = datetime.strptime("2017-01-01", "%Y-%m-%d")
run_params["start"] = pd.Timestamp(datetime.strptime("2018-01-01", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["end"] = pd.Timestamp(datetime.strptime("2019-01-27", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["capital_base"] = 100000
run_params["bundle"] = 'mixed-data'
run_params["data_frequency"] = 'minute'

with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
    x, more_output = zipline_launcher(alg_name, run_params, algorithm_params)
    text_output = buf.getvalue()

#with open("a.txt", "w") as f:
#    f.write(text_output)

params_extractor = {"parameter1": "VIX_OPEN_LEVEL", "parameter2": "VIX_SHARE", "parameter3": "WINDOW_MIN"}
alg_return = x.algorithm_period_return.iloc[-1]
max_drawdown = x.max_drawdown.iloc[-1]
max_drawdown = max_drawdown if max_drawdown < 0 else 0
metrics = {"return (%)": alg_return, "drawdown (%)": max_drawdown, "Win/loss": zipline_utils.metrics_winloss(x, ['TLT'])}

zipline_utils.save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x)

print(metrics)


 
