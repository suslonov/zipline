#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
if  "__file__" in globals():
    os.sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
else:
    os.sys.path.append(os.path.abspath('.'))
    os.sys.path.append(os.path.dirname(os.path.abspath('.')))

import sys
import pandas as pd
import pytz
from datetime import datetime
import time
import IBconnector

eastern = pytz.timezone('US/Eastern')
jerusalem = pytz.timezone('Asia/Jerusalem')
start_session = "09:30"
end_session = "15:59"
output_names = ['Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio']

ib = IBconnector.IBManager()

if ib.connect() == 0:
    SystemExit(0)

ib.run()

symbol_list = sys.argv[2].split(',')

for symbol in symbol_list:
    symbol = str.strip(symbol)
    a = pd.read_csv(sys.argv[1] + '/' + symbol + '.csv', parse_dates=['Date'])
    a.set_index(pd.DatetimeIndex(a["Date"]), inplace=True)
    
    end_date = max(a.index).tz_localize(tz='US/Eastern').date()
    now_date = pd.to_datetime('now', utc=True).tz_convert(tz=eastern).date()
    if not a.empty:
        days_to_request = (now_date - end_date).days
        if days_to_request > 5:
            days_to_request = (days_to_request//7 + 1) * 5 + 1
        else:
            days_to_request += 1
    else:
        days_to_request = 100
        
    print("now ", now_date, " last loaded date ", end_date, " requesting ", days_to_request, " days")
    
    req_contract = ib.contract(symbol, exchange='SMART')

    req_id = ib.reqHistoricalData(req_contract, durationStr=str(days_to_request) + ' D', barSizeSetting='1 min',  whatToShow='TRADES')

    while not ib.historical_data_finished(req_id):
        time.sleep(1)

    historical_data = ib.get_historical_data(req_id)    
    print(symbol, len(historical_data))
    if len(historical_data) == 0:
        continue

    historical_data['ex_dividend'] = 0.0
    historical_data['split_ratio'] = 1.0
    historical_data["Date"] = historical_data.apply(lambda row: pd.Timestamp(jerusalem.localize(row._date).astimezone(eastern)), axis = 1)
    historical_data.set_index(pd.DatetimeIndex(historical_data["Date"]), inplace=True)
    aa = historical_data.loc[historical_data.index < pd.Timestamp(now_date)]
    aa = aa.loc[aa.index > pd.Timestamp(end_date + pd.Timedelta('1 day'))]

    if len(aa) == 0:
        continue

    aa["Date"] = aa.apply(lambda row: row.Date.astimezone(pytz.utc).tz_localize(None), axis = 1)
    aa["Date"] += pd.Timedelta(minutes=1)  # shift to be the same as Quantopian
    aa = aa.between_time(start_session, end_session).sort_values(by="Date")
    aa.set_index(pd.DatetimeIndex(aa["Date"]), inplace=True)

    ab = a.append(aa)
    ab.to_csv(sys.argv[1] + '/' + symbol + '.csv', columns=output_names, index=False)
    ib.del_historical_data(req_id)

ib.disconnect()
