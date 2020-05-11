#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import importlib
import pandas as pd
from datetime import datetime
from zipline.data import bundles
from zipline.data.data_portal import DataPortal
from trading_calendars import get_calendar
from zipline.utils.run_algo import load_extensions

if  "__file__" in globals():
    os.sys.path.append(os.path.dirname(os.path.abspath(__file__)))
else:
    os.sys.path.append(os.path.abspath('.'))
import zipline_utils
import remote_server

class FakeContext():
    def __init__(self):
        self.not_zipline_run = True
        
class MyDailyData(DataPortal):
    def history(self, symbol, field, period, freq):
        if type(field) == list:
            return dict(zip(field, [self.get_history_window([symbol], self.end_dt, period, freq, fld, 'daily').iloc[:, 0] for fld in field]))
        else:
            return self.get_history_window([symbol], self.end_dt, period, freq, field, 'daily').iloc[:, 0]

    def current(self, symbol, field):
        return self.get_history_window([symbol], self.end_dt, 1, '1d', field, 'daily').iloc[0, 0] # TODO

environ = os.environ
bundle = 'mixed-data'
bundle_timestamp = None
market_data = 0
alg_name = "VIX+Bonds+Indicators+Stops 1"
signals_comment = "test run"
algorithm_params = {"WINDOW_MIN": 5, "VIX_SHARE": 0.6, "VIX_OPEN_LEVEL": 15, "stop_limit": 0.075, "MA": 100}

if len(sys.argv) > 1:
    input_date = pd.Timestamp(datetime.strptime(sys.argv[1], "%Y-%m-%d")).tz_localize(tz='US/Eastern')
else:
    input_date = pd.Timestamp(datetime.today()).tz_localize(tz='US/Eastern')

def get_signals_from_data(input_date):
    load_extensions(True, (), True, environ)
    bundle_data = bundles.load(
        bundle,
        environ,
        bundle_timestamp,
    )
    
    first_trading_day = bundle_data.equity_minute_bar_reader.first_trading_day
    trading_calendar = get_calendar('XNYS')
    
    data = MyDailyData(
        bundle_data.asset_finder,
        trading_calendar=trading_calendar,
        first_trading_day=first_trading_day,
        equity_minute_reader=bundle_data.equity_minute_bar_reader,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader,
    )
    data.end_dt = max(trading_calendar.schedule.loc[trading_calendar.schedule.index <= input_date].index)
    
    algorithms_dir = "algorithms"
    alg = importlib.import_module(algorithms_dir+"."+alg_name)
    algorithm_context = alg.algorithm_context(algorithm_params)
    
    context = FakeContext()
    context.bundle_data = bundle_data
    algorithm_context.initialize(context)
    
    algorithm_context.get_history_data_for_rebalance(context, data)
    
    # get add_market_data_for_rebalance
    # add_market_data_for_rebalance(context, data)
    # market_data = 1
    
    signals = algorithm_context.signals_for_rebalance(context, data)
    
    params_extractor = {"parameter1": "VIX_OPEN_LEVEL", "parameter2": "VIX_SHARE", "parameter3": "WINDOW_MIN"}
    signals_extractor = {"Short_UVXY_SIGNAL1": "simple", "Short_UVXY_SIGNAL2": "simple", "Close_UVXY_SIGNAL1": "simple", "Close_UVXY_SIGNAL2": "simple",
                         "VIX_HedgeLeverage": "simple", "WVF": "pandas", "SmoothedWVF1": "array", "SmoothedWVF2": "array"}
    
    zipline_utils.save_signals_to_db(alg_name, input_date, signals_comment, algorithm_params, signals, market_data, params_extractor, signals_extractor)
    
    server, port = remote_server.open_remote_port()
    zipline_utils.save_signals_to_db(alg_name, input_date, signals_comment, algorithm_params, signals, market_data, params_extractor, signals_extractor, port)
    remote_server.close_remote_port(server)

get_signals_from_data(pd.Timestamp(datetime.strptime('2019-1-1', "%Y-%m-%d")))

# get_signals_from_data(input_date)

get_signals_from_data(pd.Timestamp(datetime.strptime('2020-5-8', "%Y-%m-%d")))
