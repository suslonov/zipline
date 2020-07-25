#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

algorithm_params = {"WINDOW_MIN": 5, "VIX_SHARE": 0.6, "VIX_OPEN_LEVEL": 15, "stop_limit": 0.075, "MA": 100, 
                    "SPREAD_ORDERS_D": 1, "SPREAD_ORDERS_ALL": 1}
alg_name = "VIX+Bonds+Indicators+Stops 2"
run_params = {}
run_params["start"] = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["end"] = pd.Timestamp(datetime.strptime("2020-07-01", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["capital_base"] = 100000
run_params["bundle"] = 'mixed-data'
run_params["data_frequency"] = 'minute'
params_extractor = {"parameter1": "VIX_OPEN_LEVEL", "parameter2": "VIX_SHARE", "parameter3": "WINDOW_MIN"}

run_comment = str(algorithm_params["SPREAD_ORDERS_D"]) + " daily order(s), " + " fire orders anyway " + str(algorithm_params["SPREAD_ORDERS_ALL"])
with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
    x, more_output = zipline_launcher(alg_name, run_params, algorithm_params)
    text_output = buf.getvalue()
alg_return = x.algorithm_period_return.iloc[-1]
max_drawdown = x.max_drawdown.iloc[-1]
max_drawdown = max_drawdown if max_drawdown < 0 else 0
metrics = {"return (%)": alg_return, "drawdown (%)": max_drawdown, "Win/loss": zipline_utils.metrics_winloss(x, ['TLT'])}

zipline_utils.save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x)
print(metrics)


# f = open("a.txt", "w")
# f.write(text_output) 
# f.close()
