#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
if  "__file__" in globals():
    os.sys.path.append(os.path.dirname(os.path.abspath(__file__)))
else:
    os.sys.path.append(os.path.abspath('.'))

from time import sleep
from datetime import datetime
import threading
from app_clock import ClockMessages, RealtimeClock
import pandas as pd
import queue

import get_signals_from_data

q = queue.Queue()

session_start = pd.Timestamp(pd.to_datetime('now').replace(hour=9, minute=30, second=0)).tz_localize(tz='US/Eastern')
# session_start.tz_convert(tz='Asia/Jerusalem')
stop_at = session_start + pd.Timedelta('1 hour')

schedulled_event_list = []
schedulled_event_list.append((pd.Timedelta('1 minute') * 15, "Signals15"))

clock1 = RealtimeClock(1, stop_at, session_start, schedulled_event_list, q)

def run_loop():
    clock1.event_loop()

clock_thread = threading.Thread(target=run_loop, daemon=True)
clock_thread.start()
print("started at: " + str(datetime.now()))

while True: # TODO multithread processing
    if not q.empty():
        current_event = q.get()
        print(current_event)
        if current_event[1] == ClockMessages.FINISH:
            break
        if current_event[1] == ClockMessages.EVENT:
            if current_event[2] == "Signals15":
                print("get_signals_from_data at: " + str(current_event[0]))
                get_signals_from_data.get_signals_from_data(current_event[0].tz_convert(tz='US/Eastern'), True)
                continue
    sleep(1)

print("finished at: " + str(datetime.now()))
    
# q.join()
# clock_thread.join()
