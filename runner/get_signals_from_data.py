#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import importlib
import pandas as pd
from datetime import datetime
import threading
import time
import pytz

from zipline.data import bundles
from zipline.data.data_portal import DataPortal
from trading_calendars import get_calendar
from zipline.utils.run_algo import load_extensions

if  "__file__" in globals():
    os.sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
else:
    os.sys.path.append(os.path.abspath('.'))
    os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import zipline_utils
import remote_server
import IBconnector

class FakeContext():
    def __init__(self):
        self.not_zipline_run = True

class MyDailyData(DataPortal):
    def one_symbol_history(self, equity, field, period, freq):
        h = self.get_history_window([equity], self.end_dt, period, freq, field, 'daily').iloc[:, 0]
        if equity.symbol in self.current_day_data:
            c = self.current_day_data[equity.symbol]
            if not c is None:
                h[self.input_date.tz_localize(None).tz_localize('UTC')] = c[field].iloc[0]
        return h

    def history(self, equity, field, period, freq):
        if type(field) == list:
            return dict(zip(field, [self.one_symbol_history(equity, fld, period, freq) for fld in field]))
        else:
            return self.one_symbol_history(equity, field, period, freq)

    def current(self, equity, field):
        return self.one_symbol_history(equity, field, 1, '1d').iloc[-1]
        # return self.get_history_window([equity], self.end_dt, 1, '1d', field, 'daily').iloc[0, 0] # TODO

eastern = pytz.timezone('US/Eastern')
jerusalem = pytz.timezone('Asia/Jerusalem')
start_session = "09:30"
end_session = "15:59"

def connect_once_to_ib_get_data(symbol_list, input_date):
    ib = IBconnector.IBManager()
    if ib.connect() == 0:
        SystemExit(0)
    ib.run()

    current_day_data = {}
    for symbol in symbol_list:
        req_contract = ib.contract(symbol, exchange='SMART')
        req_id = ib.reqHistoricalData(req_contract, durationStr='1 D', barSizeSetting='1 min',  whatToShow='TRADES')

        while not ib.historical_data_finished(req_id):
            time.sleep(1)
    
        historical_data = ib.get_historical_data(req_id)    
        if len(historical_data) == 0:
            current_day_data[symbol] = None
            continue
    
        historical_data['ex_dividend'] = 0.0
        historical_data['split_ratio'] = 1.0
        historical_data["Date"] = historical_data.apply(lambda row: pd.Timestamp(jerusalem.localize(row._date).astimezone(eastern)), axis = 1)
        historical_data.set_index(pd.DatetimeIndex(historical_data["Date"]), inplace=True)
        aa = historical_data.loc[historical_data.index < input_date + pd.Timedelta('1 day')]
        aa = aa.between_time(start_session, end_session)
        aa = aa.loc[aa.index == max(aa.index)]
        current_day_data[symbol] = aa
        ib.del_historical_data(req_id)

    ib.disconnect()
    return current_day_data

def get_signals_from_data(input_date, market_data=False):
    environ = os.environ
    bundle = 'mixed-data'
    bundle_timestamp = None
    alg_name = "VIX+Bonds+Indicators+Stops 1"
    signals_comment = "test run"
    algorithm_params = {"WINDOW_MIN": 5, "VIX_SHARE": 0.6, "VIX_OPEN_LEVEL": 15, "stop_limit": 0.075, "MA": 100}
    algorithm_symbols = ["UVXY", "SPY"]
    
    load_extensions(True, (), True, environ)
    bundle_data = bundles.load(
        bundle,
        environ,
        bundle_timestamp,
    )
    
    first_trading_day = bundle_data.equity_minute_bar_reader.first_trading_day
    trading_calendar = get_calendar('XNYS')
    
    algorithms_dir = "algorithms"
    alg = importlib.import_module(algorithms_dir+"."+alg_name)
    algorithm_context = alg.algorithm_context(algorithm_params)
    
    params_extractor = {"parameter1": "VIX_OPEN_LEVEL", "parameter2": "VIX_SHARE", "parameter3": "WINDOW_MIN"}
    signals_extractor = {"Short_UVXY_SIGNAL1": "simple", "Short_UVXY_SIGNAL2": "simple", "Close_UVXY_SIGNAL1": "simple", "Close_UVXY_SIGNAL2": "simple",
                         "VIX_HedgeLeverage": "simple", "WVF": "pandas", "SmoothedWVF1": "array", "SmoothedWVF2": "array"}
    
    data = MyDailyData(
        bundle_data.asset_finder,
        trading_calendar=trading_calendar,
        first_trading_day=first_trading_day,
        equity_minute_reader=bundle_data.equity_minute_bar_reader,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader,
    )
    # data.end_dt = max(trading_calendar.schedule.loc[trading_calendar.schedule.index <= input_date].index)
    data.end_dt = min([bundle_data.equity_daily_bar_reader.sessions[-1], input_date])
    data.input_date = input_date
    data.current_day_data = {}

    if market_data:    
        data.current_day_data = connect_once_to_ib_get_data(algorithm_symbols, input_date)
    else:
        data.current_day_data = {}
    
    market_data = len([i for i in data.current_day_data.values() if not i is None]) != 0
    
    context = FakeContext()
    context.bundle_data = bundle_data
    algorithm_context.initialize(context)
    
    algorithm_context.get_history_data_for_rebalance(context, data)
    
    signals = algorithm_context.signals_for_rebalance(context, data)
    
    zipline_utils.save_signals_to_db(alg_name, input_date, signals_comment, algorithm_params, signals, market_data, params_extractor, signals_extractor)
    
    server, port = remote_server.open_remote_port()
    zipline_utils.save_signals_to_db(alg_name, input_date, signals_comment, algorithm_params, signals, market_data, params_extractor, signals_extractor, port)
    remote_server.close_remote_port(server)

def main():
    # sys.argv.append('0')
    # sys.argv.append('2020-05-15')
    market_data = False
    if len(sys.argv) > 1:
        if sys.argv[1] == '1' or sys.argv[2].lower() == 'true':
            market_data = True
    if len(sys.argv) > 2:
        input_date = pd.Timestamp(datetime.strptime(sys.argv[2], "%Y-%m-%d")).tz_localize(tz='Asia/Jerusalem').tz_convert(tz='US/Eastern')
    else:
        input_date = pd.Timestamp(datetime.today()).tz_localize(tz='Asia/Jerusalem').tz_convert(tz='US/Eastern')

    get_signals_from_data(input_date, market_data)


if __name__ == '__main__':
    main()

