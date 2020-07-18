#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
https://algotrading101.com/learn/interactive-brokers-python-api-native-guide/
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time
from datetime import datetime
import pandas as pd

from ibapi.contract import Contract
from ibapi.order import *

ports = {4001: "Live IB gateway", 7496: "Live IB TWS", 4002: "Paper trading IB gateway", 7497: "Paper trading IB TWS"}

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.historical_data = {}

    def tickPrice(self, reqId, tickType, price, attrib):
        # if tickType == 2 and reqId == 1:
            # print('The current ask price is: ', price)
        print(f'tickPrice reqId: {reqId} tickType: {tickType} price: {price}')

    def tickSize(self, reqId, tickType, size):
        # if tickType == 2 and reqId == 1:
            # print('The current ask price is: ', price)
        print(f'tickSize reqId: {reqId} tickType: {tickType} size: {size}')

    def historicalData(self, reqId, bar):
        self.historical_data[reqId]["data"].loc[len(self.historical_data[reqId]["data"])] = \
            [datetime.fromtimestamp(int(bar.date)), bar.open, bar.high, bar.low, bar.close, bar.volume]
        
    def historicalDataEnd(self, reqId, start, end):
        super().historicalDataEnd(reqId, start, end)
        self.historical_data[reqId]["finished"] = True

    def historicalDataUpdate(self, reqId, bar):
        print("HistoricalDataUpdate. ReqId:", reqId, "BarData.", bar)

    def nextValidId(self, orderId: int):     
        super().nextValidId(orderId)
        self.nextorderId = orderId
        print('The next valid order id is: ', self.nextorderId)

    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice) 

    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action, order.orderType, order.totalQuantity, orderState.status)

    def execDetails(self, reqId, contract, execution):
        print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)
        
    def updateMktDepth(self, reqId, position, operation, side, price, size):
        print(f'reqId: {reqId} position: {position} operation: {operation} side: {side} price: {price}, size: {size}')

    def mktDepthExchanges(self,	depthMktDataDescriptions):
        print(depthMktDataDescriptions)

class IBManager:
    def __init__(self):
        self.app = IBapi()
        self.reqId = 0

    def run_loop(self):
        self.app.run()
        
    def connect(self, port = None):
        if port:
            self.app.connect('127.0.0.1', port, 123)
            if self.app.isConnected():
                print("connected to:" + str(port) + " = " + ports[port])
                return port
        for port in ports:
            self.app.connect('127.0.0.1', port, 123)
            if self.app.isConnected():
                print("connected to:" + str(port) + " = " + ports[port])
                return port
        else:
            print("exit: no connection")
            return 0

    def run(self):
        self.api_thread = threading.Thread(target=self.run_loop, daemon=True)
        self.api_thread.start()
        time.sleep(1) #Sleep interval to allow time for connection to server

    def disconnect(self):
        self.app.disconnect()
        
    def contract(self, symbol, secType='STK', currency='USD', exchange='ISLAND', primaryExchange='ISLAND'):
        c = Contract()
        c.symbol = symbol
        c.secType = secType 
        c.currency = currency
        c.exchange = exchange
        c.primaryExchange = primaryExchange
        return c

    def next_reqId(self):
        self.reqId += 1
        return self.reqId

    def reqMktData(self, contract, reqId = None, genericTickList = '', snapshot=False, regulatorySnaphsot=False, mktDataOptions=[]):
        if not reqId:
            reqId = self.next_reqId()
        self.app.reqMktData(reqId, contract, genericTickList, snapshot, regulatorySnaphsot, mktDataOptions)
# add processing        
        return reqId

    def cancelMktData(self, reqId):
        self.app.cancelMktData(reqId)

    def reqHistoricalData(self, contract, reqId = None, endDateTime='', durationStr='2 D', barSizeSetting='1 hour', whatToShow='BID', useRTH=0, formatDate=2, keepUpToDate=False, chartOptions=[]):
        if not reqId:
            reqId = self.next_reqId()
        data_to_load = pd.DataFrame(columns=['_date', 'open', 'high', 'low', 'close', 'volume'])
        self.app.historical_data[reqId] = {"data": data_to_load, "finished": False}
        self.app.reqHistoricalData(reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)
        return reqId
    
    def historical_data_finished(self, reqId):
        return self.app.historical_data[reqId]["finished"]

    def get_historical_data(self, reqId):
        return self.app.historical_data[reqId]["data"]

    def del_historical_data(self, reqId):
        del(self.app.historical_data[reqId])

"""
app.nextorderId = None


while True:
    if isinstance(app.nextorderId, int):
        print('connected')
        break
    else:
        print('waiting for connection')
        time.sleep(1)


"""