#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import quandl
quandl.ApiConfig.api_key = os.environ['QUANDL_API_KEY']
quandl.bulkdownload('EOD', filename="~/.zipline/EOD_all/q.zip")

import os
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import csv

csv_names = ['Symbol', 'Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio', 'adj_open', 'adj_high', 'adj_low', 'adj_close', 'adj_volume']
csvfile = "~/.zipline/EOD_all/EOD_20200623.csv"

symbols = ['TVIX', 'TQQQ', 'UBT', 'UST', 'SWAN', 'BTAL', 'IBUY', 'MOM', 'AOK', 'ARKG', 'CLIX', 'IAUF', 'OGIG', 
           'DFND', 'BSV', 'EDV', 'PHDG', 'AAPL', 'COST', 'EBAY', 'EUO', 'GLD', 'JNJ', 'MSFT', 'QQQ', 'SPXL', 
           'TIP', 'TMF', 'UGLD', 'WMT', 'XLP', 'XLV', 'YCS', 'ZIV', 'TLT', 'SPXS', 'TMV', 'FAS', 'DOES', 'JDST', 
           'JNUG', 'GOOD', 'LABD', 'DRN', 'DRV', 'FNGD', 'FNGU', 'TNA', 'TZA', 'TECL', 'TECS', 'EDC', 'EDZ', ]

skiprows = 0
step = 1000000
q = []
   
pa = pd.read_csv(csvfile, skiprows=skiprows, nrows=step, header=None, names = csv_names, parse_dates=['Date'])
    
while len(pa) != 0:
    symbol = pa.Symbol.iloc[0]
    pa_current = pa.loc[pa.Symbol == symbol]
    pa = pa.loc[pa.Symbol != symbol]
    if len(pa) == 0:
        skiprows = skiprows + step
        pa = pd.read_csv(csvfile, skiprows=skiprows, nrows=step, header=None, names = csv_names, parse_dates=['Date'])
        pa_current = pa_current.append(pa.loc[pa.Symbol == symbol])
        pa = pa.loc[pa.Symbol != symbol]

    pa_current.set_index(keys=['Date'], drop=True, inplace=True)
    if symbol in symbols:
        q.append((symbol, pa_current))

pa = None

out_csvdir = os.environ["HOME"]+"/.zipline/EOD_all/for_yair/"
for (s, a) in q:
    # a.drop(columns=["Symbol"], inplace=True)
    a.to_csv(out_csvdir + s + ".csv")

start_year = datetime.datetime.today().date() - relativedelta(years=1)
start_quarter = datetime.datetime.today().date() - relativedelta(months=3)

res = []
for (s, a) in q:
    a["dollar_volume"] = a["close"] * a["volume"]
    res.append((s,
                a.loc[a.index >= pd.Timestamp(start_year)]['volume'].mean(),
                a.loc[a.index >= pd.Timestamp(start_year)]['dollar_volume'].mean(),
                a.loc[a.index >= pd.Timestamp(start_quarter)]['volume'].mean(),
                a.loc[a.index >= pd.Timestamp(start_quarter)]['dollar_volume'].mean(),))

with open(out_csvdir + "res.csv", 'w+') as csvfile:
    csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
    csvline = ["symbol", "avg_volume_year", "avg_volume_year_usd", "avg_volume_quarter", "avg_volume_quarter_usd"]
    csvwriter.writerow(csvline)
    for r in res:
        csvwriter.writerow(r)
