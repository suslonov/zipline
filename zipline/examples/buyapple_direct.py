#!/usr/bin/env python
#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from zipline.api import order, record, symbol
from zipline.finance import commission, slippage
import zipline
from datetime import datetime
import pandas as pd

symb = 'AAPL'
start=pd.Timestamp(datetime.strptime("2020-1-2", "%Y-%m-%d")).tz_localize(tz='US/Eastern')
end=pd.Timestamp(datetime.strptime("2020-1-3", "%Y-%m-%d")).tz_localize(tz='US/Eastern')

class algorithm_context():

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def initialize(self, context):
        context.asset = symbol(symb)
    
        # Explicitly set the commission/slippage to the "old" value until we can
        # rebuild example data.
        # github.com/quantopian/zipline/blob/master/tests/resources/
        # rebuild_example_data#L105
        context.set_commission(commission.PerShare(cost=.0075, min_trade_cost=1.0))
        context.set_slippage(slippage.VolumeShareSlippage())


    def handle_data(self, context, data):
        order(context.asset, 10)
        record(AAPL=data.current(context.asset, 'price'))


    # Note: this function can be removed if running
    # this algorithm on quantopian.com
    def analyze(self, context=None, results=None):
        import matplotlib.pyplot as plt
        # Plot the portfolio and asset data.
        ax1 = plt.subplot(211)
        results.portfolio_value.plot(ax=ax1)
        ax1.set_ylabel('Portfolio value (USD)')
        ax2 = plt.subplot(212, sharex=ax1)
        results.AAPL.plot(ax=ax2)
        ax2.set_ylabel('AAPL price (USD)')

        # Show the plot.
        plt.gcf().set_size_inches(18, 8)
        plt.show()

a = algorithm_context(start, end)

x = zipline.run_algorithm(start = start, 
                          end = end, 
                          data_frequency = 'daily', 
                          initialize=a.initialize, 
                          handle_data=a.handle_data, 
                          analyze=a.analyze, 
                          capital_base=1000000)



