#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
if  "__file__" in globals():
    os.sys.path.append(os.path.dirname(os.path.abspath(__file__)))
else:
    os.sys.path.append(os.path.abspath('.'))

from time import sleep
import threading
from app_clock import ClockMessages, RealtimeClock
import pandas as pd
import queue

q = queue.Queue()
stop_at = pd.to_datetime('now', utc=True) + pd.Timedelta('1 minute') * 5

schedulled_event_list = []
schedulled_event_list.append(( - pd.Timedelta('1 second') * 120, "Calculate"))
schedulled_event_list.append(( - pd.Timedelta('1 second') * 100, "EventB1"))
schedulled_event_list.append(( - pd.Timedelta('1 second') * 100, "EventB2"))
schedulled_event_list.append(( - pd.Timedelta('1 second') * 100, "EventB3"))
schedulled_event_list.append(( - pd.Timedelta('1 second') * 100, "EventB4"))
schedulled_event_list.append((0, "EventC"))
schedulled_event_list.append((pd.Timedelta('1 second') * 80, "Rebalance"))
schedulled_event_list.append((pd.Timedelta('1 second') * 60, "EventE1"))
schedulled_event_list.append((pd.Timedelta('1 second') * 60, "EventE2"))


session_start = pd.to_datetime('now', utc=True) + pd.Timedelta('1 minute') * 3

clock1 = RealtimeClock(1, stop_at, session_start, schedulled_event_list, q)

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
        if current_event[1] == ClockMessages.EVENT:
            if current_event[2] == "Calculate":
                print("run calc")
                continue
            if current_event[2] == "Rebalance":
                print("run Rebalance")
                continue
    sleep(1)
    
# q.join()
# clock_thread.join()
