# -*- coding: utf-8 -*-

# https://www.zipline.io/appendix.html#zipline.run_algorithm

from zipline.api import order, record, symbol, get_datetime
from datetime import datetime
import zipline
import pandas as pd

symb = 'UVXY'
symb2 = 'SPY'
start=datetime.strptime("2020-1-2", "%Y-%m-%d")
end=datetime.strptime("2020-1-3", "%Y-%m-%d")

class algorithm_context():

    def __init__(self, start, end):
        self.start = start
        self.end = end
    
    def initialize(self, context):
        context.assets = symbol(symb)
        pass

    def handle_data(self, context, data):
#        order(symbol(symb), 10)
        record(UVXY=data.current(symbol(symb), 'price')) 
        print(get_datetime(),
              str(data.history(symbol(symb), 'close', 1, '1m').values[0]),
              str(data.history(symbol(symb), 'close', 1, '1d').values[0]),
              str(data.history(symbol(symb2), 'close', 1, '1d').values[0]))
    
    def before_trading_start(self, context, data):
        # Create a window
        self.window_1 = data.history(context.assets, 'price', int((self.end-self.start).total_seconds()/3600/24), '1d')
        print(str(data.history(symbol(symb), 'close', 1, '1d').values[0]))
    
a = algorithm_context(start, end)

x = zipline.run_algorithm(start = start, end = end, data_frequency = 'minute', initialize=a.initialize, handle_data=a.handle_data, before_trading_start=a.before_trading_start, capital_base=1000000, bundle = 'mixed-data')

len(a.window_1)

