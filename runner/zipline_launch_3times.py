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
import time
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
params_extractor = {"parameter1": "VIX_OPEN_LEVEL", "parameter2": "VIX_SHARE", "parameter3": "WINDOW_MIN"}

run_comment = "Run on 2019 year"
algorithm_params = {"WINDOW_MIN": 5, "VIX_SHARE": 0.6, "VIX_OPEN_LEVEL": 15, "stop_limit": 0.075, "MA": 100}

run_params = {}
run_params["start"] = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["end"] = pd.Timestamp(datetime.strptime("2020-01-27", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["capital_base"] = 1000000
run_params["bundle"] = 'mixed-data'

with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
    x, more_output = zipline_launcher(alg_name, run_params, algorithm_params)
    text_output = buf.getvalue()

alg_return = x.algorithm_period_return.iloc[-1]
max_drawdown = x.max_drawdown.iloc[-1]
max_drawdown = max_drawdown if max_drawdown < 0 else 0
metrics = {"return (%)": alg_return, "drawdown (%)": max_drawdown, "Win/loss": zipline_utils.metrics_winloss(x, ['TLT'])}

zipline_utils.save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x)
print(metrics)
time.sleep(10)
####################################

run_comment = "Run on 2 years VIX_OPEN_LEVEL=20"
algorithm_params = {"WINDOW_MIN": 5, "VIX_SHARE": 0.6, "VIX_OPEN_LEVEL": 20, "stop_limit": 0.075, "MA": 100}

run_params = {}
run_params["start"] = pd.Timestamp(datetime.strptime("2012-03-01", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["end"] = pd.Timestamp(datetime.strptime("2014-12-31", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["capital_base"] = 1000000
run_params["bundle"] = 'mixed-data'

with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
    x, more_output = zipline_launcher(alg_name, run_params, algorithm_params)
    text_output = buf.getvalue()

alg_return = x.algorithm_period_return.iloc[-1]
max_drawdown = x.max_drawdown.iloc[-1]
max_drawdown = max_drawdown if max_drawdown < 0 else 0
metrics = {"return (%)": alg_return, "drawdown (%)": max_drawdown, "Win/loss": zipline_utils.metrics_winloss(x, ['TLT'])}

zipline_utils.save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x)
print(metrics)
time.sleep(10)
#####################################

run_comment = "Run on 7 years"
algorithm_params = {"WINDOW_MIN": 5, "VIX_SHARE": 0.6, "VIX_OPEN_LEVEL": 15, "stop_limit": 0.075, "MA": 100}

run_params = {}
run_params["start"] = pd.Timestamp(datetime.strptime("2012-03-01", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["end"] = pd.Timestamp(datetime.strptime("2019-12-31", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["capital_base"] = 1000000
run_params["bundle"] = 'mixed-data'

with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
    x, more_output = zipline_launcher(alg_name, run_params, algorithm_params)
    text_output = buf.getvalue()

alg_return = x.algorithm_period_return.iloc[-1]
max_drawdown = x.max_drawdown.iloc[-1]
max_drawdown = max_drawdown if max_drawdown < 0 else 0
metrics = {"return (%)": alg_return, "drawdown (%)": max_drawdown, "Win/loss": zipline_utils.metrics_winloss(x, ['TLT'])}

zipline_utils.save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x)
print(metrics)


