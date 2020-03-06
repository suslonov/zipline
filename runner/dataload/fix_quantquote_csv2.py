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
import queue
import threading

csv_names = ['_date', '_time', 'open', 'high', 'low', 'close', 'volume', 'split_ratio', 'earnings', 'ex_dividend']
csv_types = {'_date':str, '_time':str, 'open':np.float64, 'high':np.float64, 'low':np.float64, 'close':np.float64, 'volume':np.float64, 'split_ratio':np.float64, 'earnings':np.float64, 'ex_dividend':np.float64}
output_names = ['Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio']
eastern = pytz.timezone('US/Eastern')
num_worker_threads = 48

q_in = queue.Queue()
q_out = queue.Queue()

def worker(n):
    while True:
        print("Thread: ", n, " queue: ", q_in.qsize())
        item = q_in.get()
        if item is None:
            break
        a = pd.read_csv(item, names=csv_names, dtype=csv_types)
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
            q_out.put(a)
        q_in.task_done()

q_in = queue.Queue()
for d in os.walk(sys.argv[1]):
    if d[0] != sys.argv[1]:
        q_in.put(d[0]+'/table_'+'UVXY'.lower()+'.csv')

#a = pd.read_csv(sys.argv[1]+'/table_'+sys.argv[3].lower()+'.csv', names=csv_names, dtype=csv_types)        
       
threads = []
for i in range(num_worker_threads):
    t = threading.Thread(target=worker, args=(i,))
    t.start()
    threads.append(t)

q_in.join()
for i in range(num_worker_threads):
    q_in.put(None)
for t in threads:
    t.join()

pa = None
while not q_out.empty():
    if pa is None:
        pa = q_out.get()
    else:
        pa = pa.append(q_out.get())

print("finishing ...")
pb = pa.sort_values(by="_datetime")
pb.to_csv(sys.argv[2]+'/'+sys.argv[3].upper()+'.csv', columns=output_names, index=False)

