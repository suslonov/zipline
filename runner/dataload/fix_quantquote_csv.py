#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 19:37:03 2020

@author: anton
"""

import sys
import os
import pandas as pd
import numpy as np
import pytz
import datetime

csv_names = ['_date', '_time', 'open', 'high', 'low', 'close', 'volume', 'split_ratio', 'earnings', 'ex_dividend']
csv_types = {'_date':str, '_time':str, 'open':np.float64, 'high':np.float64, 'low':np.float64, 'close':np.float64, 'volume':np.float64, 'split_ratio':np.float64, 'earnings':np.float64, 'ex_dividend':np.float64}
output_names = ['Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio']
csv_split_names = ['Split_date', 'n', 'd']

eastern = pytz.timezone('US/Eastern')
start_session = datetime.time(9, 30, 0)
end_session = datetime.time(16, 0, 0)

a = pd.read_csv(sys.argv[1]+'/table_'+sys.argv[4].lower()+'.csv', names=csv_names, dtype=csv_types)
       
if not a.empty:
    s = pd.read_csv(sys.argv[3]+'/'+sys.argv[4].upper()+'.csv', names=csv_split_names, parse_dates=['Split_date'])

    a["_datetimeEST"] = a.apply(lambda row: pd.Timestamp(datetime.datetime.strptime(row._date[:4]+'-'
                                                                           +row._date[4:6]+'-'
                                                                           +row._date[-2:]+' '
                                                                           +row._time[-4:-2]+':'
                                                                           +row._time[-2:],
                                                                           "%Y-%m-%d %H:%M")).tz_localize(tz='US/Eastern'), axis = 1)

    a.set_index(pd.DatetimeIndex(a["_datetimeEST"]), inplace=True)
    a["Date"] = a["_datetimeEST"].dt.tz_convert(tz=pytz.utc)
    a["Date"] += pd.Timedelta(minutes=1)  # shift to be the same as Quantopian
    pb = a.between_time(start_session, end_session).sort_values(by="Date")
    pb["Date_only"] = pb["Date"].dt.normalize().tz_convert(tz=pytz.utc)
    
    if not s.empty:
        s.sort_values('Split_date', axis=0, ascending=False, inplace=True)
        s['factor'] = s.n.cumprod()/s.d.cumprod()
        s.sort_values('Split_date', axis=0, ascending=True, inplace=True)
        s.set_index('Split_date', inplace=True)
        
        pb['Split_date'] = pb.apply(lambda row: s.loc[s.index == min(s.loc[s.index >= row['Date_only']].index, default=pd.Timestamp(2100,1,1))].index[0], axis = 1)
        pb = pb.join(s, how='left', on=['Split_date'])
        
        pb['open'] = pb['open'] * pb['factor']
        pb['high'] = pb['high'] * pb['factor']
        pb['low'] = pb['low'] * pb['factor']
        pb['close'] = pb['close'] * pb['factor']
        pb['volume'] = pb['volume'] * pb['factor']
        pb['split_ratio'] = np.where(pb['Split_date'] == pb['Date_only'], pb['factor'], 1.0)
    else:
        pb['split_ratio'] = 1.0
    
    pb.to_csv(sys.argv[2]+'/'+sys.argv[4].upper()+'.csv', columns=output_names, index=False)

