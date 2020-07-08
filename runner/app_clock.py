#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from time import sleep
from enum import Enum

import pandas as pd

class ClockMessages(Enum):
    BAR = 0
    EVENT = 1
    FINISH = 2

class RealtimeClock(object):

    def __init__(self,
                 clock_id,
                 stop_at,
                 session_start,
                 scheduled_event_list,
                 clock_queue):

        self.clock_id = clock_id
        self.stop_at = stop_at
        self.session_start = session_start
        self.clock_queue = clock_queue
        self._run_clock = True
        self.scheduled_event_list = scheduled_event_list.copy()
        self.scheduled_event_list.sort(key = lambda x: x[0])

    def event_loop(self):
        last_emit = None
        current_time = pd.to_datetime('now', utc=True)
        while self.scheduled_event_list:
            if current_time >= self.session_start + self.scheduled_event_list[0][0]:
                del(self.scheduled_event_list[0])
            else:
                break
        while self._run_clock and current_time <= self.stop_at:
            current_time = pd.to_datetime('now', utc=True)
            if last_emit is None or (current_time - last_emit >= pd.Timedelta('1 minute')):
                self.clock_queue.put((current_time, ClockMessages.BAR))
                last_emit = current_time
            while self.scheduled_event_list and current_time >= ((self.session_start
                        + self.scheduled_event_list[0][0]) if self.scheduled_event_list[0][0] else self.session_start):
                self.clock_queue.put((current_time, ClockMessages.EVENT, self.scheduled_event_list[0][1]))
                del(self.scheduled_event_list[0])
            sleep(1)
        self.clock_queue.put((current_time, ClockMessages.FINISH))
