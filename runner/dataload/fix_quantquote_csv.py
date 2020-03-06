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
from datetime import datetime, timedelta

csv_names = ['_date', '_time', 'open', 'high', 'low', 'close', 'volume', 'split_ratio', 'earnings', 'ex_dividend']
csv_types = {'_date':str, '_time':str, 'open':np.float64, 'high':np.float64, 'low':np.float64, 'close':np.float64, 'volume':np.float64, 'split_ratio':np.float64, 'earnings':np.float64, 'ex_dividend':np.float64}
output_names = ['Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio']
eastern = pytz.timezone('US/Eastern')

#pa = None
#for d in os.walk(sys.argv[1]):
#    if d[0] != sys.argv[1]:
a = pd.read_csv(sys.argv[1]+'/table_'+sys.argv[3].lower()+'.csv', names=csv_names, dtype=csv_types)
       
if not a.empty:
    a["_datetime"] = a.apply(lambda row: eastern.localize(datetime(
            int(row._date[:4]), 
            int(row._date[4:6]), 
            int(row._date[-2:]), 
            int(row._time[-4:-2]), 
            int(row._time[-2:]), 0)), axis = 1)
    a["Date"] = a.apply(lambda row: row._datetime.strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
    #a["time"] = a.apply(lambda row: row._datetime.strftime('%H:%M:%S'), axis = 1)
    #                int(row._time[-2:]), 0)).astimezone(pytz.utc), axis = 1)
    #        if pa is None:
    #            pa = a
    #        else:
    #            pa = pa.append(a)
    #        print(d[0])
    pb = a.sort_values(by="_datetime")
    pb.to_csv(sys.argv[2]+'/'+sys.argv[3].upper()+'.csv', columns=output_names, index=False)

