#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import pandas as pd
import datetime


csv_names = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividend', 'Split']
csv_rename = {'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume', 'Dividend': 'ex_dividend', 'Split': 'split_ratio'}
pa = pd.read_csv(sys.argv[1], parse_dates=['Date'])
pb = pa.set_index(keys=['Date'], drop=True).rename(columns=csv_rename).drop(columns=["Adj_Open", "Adj_High", "Adj_Low", "Adj_Close" ,"Adj_Volume"]).sort_index()

if sys.argv[2]:
    start_session=pd.Timestamp(datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")).tz_localize(tz='US/Eastern')
    pb = pb.loc[pb.index >= start_session]
if sys.argv[3]:
    end_session=pd.Timestamp(datetime.datetime.strptime(sys.argv[3], "%Y-%m-%d")).tz_localize(tz='US/Eastern')
    pb = pb.loc[pb.index <= end_session]

pb.to_csv(sys.argv[1])

