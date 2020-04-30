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

q = queue.Queue()
stop_at = pd.to_datetime('now', utc=True) + pd.Timedelta('1 minute') * 5

# pd.Timestamp(datetime.strptime("2019-01-01 14:00:01", "%Y-%m-%d %H:%M:%S"))
schedulled_event_list = []
schedulled_event_list.append((pd.to_datetime('now', utc=True) + pd.Timedelta('1 second') * 100, "EventA"))
schedulled_event_list.append((pd.to_datetime('now', utc=True) + pd.Timedelta('1 second') * 120, "EventB"))
schedulled_event_list.append((pd.to_datetime('now', utc=True) + pd.Timedelta('1 second') * 200, "EventC"))
schedulled_event_list.append((pd.to_datetime('now', utc=True) + pd.Timedelta('1 second') * 200, "EventD"))
schedulled_event_list.append((pd.to_datetime('now', utc=True) + pd.Timedelta('1 second') * 250, "EventE"))

clock1 = RealtimeClock(1, stop_at, schedulled_event_list, q)

def run_loop():
    clock1.event_loop()

clock_thread = threading.Thread(target=run_loop, daemon=True)
clock_thread.start()

while True:
    if not q.empty():
        current_event = q.get()
        print(current_event)
        if current_event[1] == ClockMessages.FINISH:
            break
    sleep(1)
    
# q.join()
# clock_thread.join()
