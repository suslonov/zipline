#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://www.zipline.io/appendix.html#zipline.run_algorithm
#https://www.quantopian.com/posts/guide-for-porting-your-algorithms-to-a-local-zipline-research-environment

from zipline.api import *
from zipline.pipeline.filters import Q3000US  
from zipline.pipeline.factors import Returns  
from zipline.algorithm import attach_pipeline, pipeline_output  
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data import Fundamentals as F  
import zipline.optimize as opt  

from datetime import datetime
import zipline


# ----------------------------------------------------------------------------  
QTU = Q3000US(); MKT = symbol('SPY'); BONDS = symbols('TLT', 'IEF'); MIN = 1;
MOM = 60; EXCL = 2; N = 5; N_Q = 60; MA_F = 10; MA_S = 80; LEV = 1.0;  
# MOM in [126, 60, 30]
# ----------------------------------------------------------------------------  
def initialize(context):  
    set_slippage(slippage.FixedSlippage(spread = 0.0))  
    schedule_function(trade, date_rules.week_start(), time_rules.market_open(minutes = 1))  
    m = QTU  
    ltd_to_eq = F.long_term_debt_equity_ratio.latest.rank(mask = m)  
    quality = (ltd_to_eq)  
    m &= quality.top(N_Q)  
    momentum = Returns(window_length = EXCL + MOM, mask = m) - Returns(window_length = EXCL, mask = m)  
    m &= momentum.top(N)  
    attach_pipeline(Pipeline(screen = m), 'pipeline')

def trade(context, data):  
    TF = data.history(MKT,'close', MA_F, '1d').mean() > data.history(MKT,'close', MA_S, '1d').mean()  
    stocks = pipeline_output('pipeline').index  
    wt = {}; wt_bnd = LEV; wt_stk = LEV/len(stocks) if len(stocks) > 0 else 0  
    for sec in context.portfolio.positions:  
        if sec not in stocks and sec not in BONDS: wt[sec] = 0  
        elif sec in stocks and not TF: wt[sec] = wt_stk; wt_bnd -= wt[sec]  
    for sec in stocks:  
        if TF:  wt[sec] = wt_stk; wt_bnd -= wt[sec]  
    for sec in BONDS: wt[sec] = wt_bnd / len(BONDS)

    order_optimal_portfolio(opt.TargetWeights(wt), [opt.MaxGrossExposure(LEV)])

def before_trading_start(context, data):  
    record(leverage = context.account.leverage)  
    longs = shorts = 0  
    for position in context.portfolio.positions.values():  
        if position.amount > 0: longs += 1  
        elif position.amount < 0: shorts += 1  
    record(long_count = longs, short_count = shorts)
    
start=datetime.strptime("2017-12-1", "%Y-%m-%d")
end=datetime.strptime("2018-1-1", "%Y-%m-%d")

zipline.run_algorithm(start = start, end = end, initialize = initialize, before_trading_start = before_trading_start, capital_base=1000000)

