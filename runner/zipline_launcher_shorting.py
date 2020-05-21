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

alg_name = "Shorting ETF pairs"
run_comment = "backtests monthly"

algorithm_params = {'leverage': -1, 'sim': -1, }
algorithm_params["etfs_long"] = ["SPXS", "FAS", "JDST", "LABU", "DRN", "FNGD", "TNA", "TECL", "EDC"]
algorithm_params["etfs_short"] = ["TMV", "FAZ", "JNUG", "LABD", "DRV", "FNGU", "TZA", "TECS", "EDZ"]

run_params = {}
run_params["capital_base"] = 20000
run_params["bundle"] = 'mixed-data'
run_params["data_frequency"] = 'minute'

run_params["start"] = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
run_params["end"] = pd.Timestamp(datetime.strptime("2020-04-08", "%Y-%m-%d")).tz_localize(tz='US/Eastern')

# 2010-01-01 - 2020-04-08
# 2019-01-01 - 2020-04-08
# 2019-01-01 - 2020-04-06
# 2019-01-01 - 2020-04-03
# 2019-01-01 - 2020-04-02
# 2019-01-01 - 2020-04-01
# 2019-01-01 - 2020-03-31

with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
    x, more_output = zipline_launcher(alg_name, run_params, algorithm_params)
    text_output = buf.getvalue()

params_extractor = {"parameter1": "leverage", "parameter2": "etfs_long", "parameter3": "etfs_short"}
alg_return = x.algorithm_period_return.iloc[-1]
max_drawdown = x.max_drawdown.iloc[-1]
max_drawdown = max_drawdown if max_drawdown < 0 else 0
metrics = {"return (%)": alg_return, "drawdown (%)": max_drawdown}
# metrics = {"return (%)": alg_return, "drawdown (%)": max_drawdown, "Win/loss": zipline_utils.metrics_winloss(x, ['TLT'])}

zipline_utils.save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x)

print(metrics)
