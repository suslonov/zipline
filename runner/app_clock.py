#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
from enum import Enum

import pandas as pd

class ClockMessages:
    BAR = 0
    EVENT = 1
    FINISH = 2

class RealtimeClock(object):

    def __init__(self,
                 clock_id,
                 stop_at,
                 schedulled_event_list,
                 clock_queue):

        self.clock_id = clock_id
        self.stop_at = stop_at
        self.clock_queue = clock_queue
        self._run_clock = True
        self.schedulled_event_list = schedulled_event_list.copy()
        schedulled_event_list.sort(key = lambda x: x[1])

    def event_loop(self):
        last_emit = None
        current_time = pd.to_datetime('now', utc=True)
        while self._run_clock and current_time <= self.stop_at:
            current_time = pd.to_datetime('now', utc=True)
            if last_emit is None or (current_time - last_emit >= pd.Timedelta('1 minute')):
                self.clock_queue.put((current_time, ClockMessages.BAR))
                last_emit = current_time
            while self.schedulled_event_list and current_time >= self.schedulled_event_list[0][0]:
                self.clock_queue.put((current_time, ClockMessages.EVENT, self.schedulled_event_list[0][1]))
                del(self.schedulled_event_list[0])
            sleep(1)
        self.clock_queue.put((current_time, ClockMessages.FINISH))
