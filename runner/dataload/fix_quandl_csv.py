#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import pandas as pd

csv_names = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividend', 'Split']
csv_rename = {'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume', 'Dividend': 'ex_dividend', 'Split': 'split_ratio'}
pa = pd.read_csv(sys.argv[1], parse_dates=['Date'])
pb = pa.set_index(keys=['Date'], drop=True).rename(columns=csv_rename).drop(columns=["Adj_Open", "Adj_High", "Adj_Low", "Adj_Close" ,"Adj_Volume"]).sort_index()

pb.to_csv(sys.argv[1])

