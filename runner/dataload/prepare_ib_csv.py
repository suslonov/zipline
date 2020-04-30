#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
import time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
from ibapi.contract import Contract

eastern = pytz.timezone('US/Eastern')
jerusalem = pytz.timezone('Asia/Jerusalem')
start_session = "09:30"
end_session = "16:00"
output_names = ['Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio']

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data_to_load = pd.DataFrame(columns=['_date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio'])

    def historicalData(self, reqId, bar):
        self.data_to_load.loc[len(self.data_to_load)] = [datetime.fromtimestamp(int(bar.date)), bar.open, bar.high, bar.low, bar.close, bar.volume, 0.0, 1.0]

def run_loop():
    app.run()

app = IBapi()

app.connect('127.0.0.1', 4002, 123)  #Gateway  paper trading
if not app.isConnected():
    app.connect('127.0.0.1', 7497, 123)  #TWS  paper trading
if not app.isConnected():
    print("exit: no connection")
    SystemExit(0)

api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

symbol_list = sys.argv[2].split(',')

for (req_id, symbol) in enumerate(symbol_list):
    symbol = str.strip(symbol)
    a = pd.read_csv(sys.argv[1] + '/' + symbol + '.csv', parse_dates=['Date'])
    a.set_index(pd.DatetimeIndex(a["Date"]), inplace=True)
    app.data_to_load = pd.DataFrame(columns=['_date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio'])
    
    if not a.empty:
        end_date = max(a.index).tz_localize(tz='US/Eastern').date()
        now_date = pd.to_datetime('now', utc=True).tz_convert(tz=eastern).date()
        days_to_request = (now_date - end_date).days
        if days_to_request > 5:
            days_to_request = (days_to_request//7 + 1) * 5 + 1
        else:
            days_to_request += 1
    else:
        days_to_request = 100
    
    req_contract = Contract()
    req_contract.symbol = symbol
    req_contract.secType = 'STK'
    req_contract.exchange = 'SMART'
    req_contract.currency = 'USD'

    data_len = 0
    app.reqHistoricalData(req_id, req_contract, '', str(days_to_request) + ' D', '1 min', 'TRADES', 0, 2, False, [])

    time.sleep(10 + days_to_request)
    while data_len < len(app.data_to_load):
        data_len = len(app.data_to_load)
        time.sleep(1)
    print(symbol, len(app.data_to_load))
    app.data_to_load["Date"] = app.data_to_load.apply(lambda row: pd.Timestamp(jerusalem.localize(row._date).astimezone(eastern)), axis = 1)
    app.data_to_load.set_index(pd.DatetimeIndex(app.data_to_load["Date"]), inplace=True)

    # aa = app.data_to_load.loc[app.data_to_load.index < pd.Timestamp(now_date - pd.Timedelta('1 days') - pd.Timedelta('1 days'))]
    aa = app.data_to_load.loc[app.data_to_load.index < pd.Timestamp(now_date)]
    aa = aa.loc[aa.index > pd.Timestamp(end_date + pd.Timedelta('1 days'))]
    aa["Date"] = aa.apply(lambda row: row.Date.astimezone(pytz.utc).tz_localize(None), axis = 1)
    aa["Date"] += pd.Timedelta(minutes=1)  # shift to be the same as Quantopian
    aa = aa.between_time(start_session, end_session).sort_values(by="Date")
    aa.set_index(pd.DatetimeIndex(aa["Date"]), inplace=True)

    ab = a.append(aa)
    ab.to_csv(sys.argv[1] + '/' + symbol + '.csv', columns=output_names, index=False)

app.disconnect()
