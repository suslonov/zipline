#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 07:23:26 2020

@author: anton
"""

import MySQLdb
import pickle
import zlib
import json
import pandas as pd


db_host="127.0.0.1"
db_user="zipline"
db_passwd="zipline_pass"
db_name="zipline_runs"

def save_run_to_db(alg_name, run_comment, text_output, run_params, algorithm_params, metrics, params_extractor, x = None):

    s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
    s2 = """INSERT INTO algorithms_table (name, extractor) VALUES (%s, "%s")"""
    s3 = """INSERT INTO saved_runs_table (algorithm_id, run_parameters, algorithm_parameters, metrics, is_xdata, run_comment) VALUES (%s, "%s", "%s", "%s", "%s", "%s")"""
    s4 = """INSERT INTO xdata_table (saved_run_id, xdata1, xdata2) VALUES (%s, _binary "%s", _binary "%s")"""
    s5 = """INSERT INTO text_output_table (saved_run_id, text_output) VALUES (%s, _binary "%s")"""
#    x_columns1 = ('algo_volatility', 'algorithm_period_return', 'alpha',
#       'benchmark_period_return', 'benchmark_volatility', 'beta',
#       'capital_used', 'ending_cash', 'ending_exposure', 'ending_value',
#       'excess_return', 'gross_leverage', 'long_exposure', 'long_value',
#       'longs_count', 'max_drawdown', 'max_leverage', 'net_leverage',
#       'period_close', 'period_label', 'period_open', 'pnl', 'portfolio_value',
#       'returns', 'sharpe', 'short_exposure', 'short_value',
#       'shorts_count', 'sortino', 'starting_cash', 'starting_exposure',
#       'starting_value', 'trading_days',
#       'treasury_period_return', 'universe_size')
    x_columns2 = ('orders', 'positions', 'transactions')
    x_columns1 = tuple(set(x.columns)- set(x_columns2))
    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
    mycur = db.cursor()
    
    i = mycur.execute(s1, (alg_name, ))
    if i == 0:
        mycur.execute(s2, (alg_name, json.dumps(params_extractor)))
        mycur.execute("SELECT LAST_INSERT_ID();")
        algorithm_id = mycur.fetchall()[0][0]
    else:
        algorithm_id = mycur.fetchall()[0][0]
  
    if x is None:
        mycur.execute(s3, (algorithm_id, json.dumps(run_params, default=str), json.dumps(algorithm_params, default=str), json.dumps(metrics, default=str), 0, run_comment))
    else:
        mycur.execute(s3, (algorithm_id, json.dumps(run_params, default=str), json.dumps(algorithm_params, default=str), json.dumps(metrics, default=str), 1, run_comment))
        mycur.execute("SELECT LAST_INSERT_ID();")
        saved_run_id = mycur.fetchall()[0][0]
        db.commit()
        x1 = x.loc[:, x_columns1]
        x2 = x.loc[:, x_columns2]
        mycur.execute(s4, (saved_run_id, zlib.compress(pickle.dumps(x1)), zlib.compress(pickle.dumps(x2))))
        
    mycur.execute(s5, (saved_run_id, zlib.compress(text_output.encode())))
    db.commit()
    db.close()

def load_algs_from_db(alg_name = None, algorithm_id = None):
    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
    mycur = db.cursor()
    if alg_name:
        s1 = """SELECT * FROM algorithms_table WHERE name = %s"""
        mycur.execute(s1, (alg_name, ))
    elif algorithm_id:
        s1 = """SELECT * FROM algorithms_table WHERE algorithm_id = %s"""
        mycur.execute(s1, (algorithm_id, ))
    else:
        s1 = """SELECT * FROM algorithms_table ORDER BY name"""
        mycur.execute(s1)
    l = list(mycur.fetchall())
    db.close()
    return l

def load_runs_from_db(alg_name = None, algorithm_id = None, saved_run_id = None):
    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
    mycur = db.cursor()
    
    if saved_run_id:
        s2 = """SELECT * FROM saved_runs_table WHERE saved_run_id = %s"""
        mycur.execute(s2, (saved_run_id, ))
        l = list(mycur.fetchall())
        db.close()
        return l
    
    if not algorithm_id and alg_name:
        s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
        i = mycur.execute(s1, (alg_name, ))
        if i == 0:
            db.close()
            return []
        else:
            algorithm_id = mycur.fetchall()[0][0]
    
    s2 = """SELECT * FROM saved_runs_table WHERE algorithm_id = %s"""
    mycur.execute(s2, (algorithm_id, ))
    l = list(mycur.fetchall())
    db.close()
    return l

def load_xdata1_from_db(saved_run_id):
    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
    mycur = db.cursor()
    s1 = """SELECT xdata1 FROM xdata_table WHERE saved_run_id = %s"""
    i = mycur.execute(s1, (saved_run_id, ))
    if i == 0:
        db.close()
        return None
    else:
        x1 = pickle.loads(zlib.decompress(mycur.fetchall()[0][0][1:-1]))
        db.close()
        return x1

def load_xdata2_from_db(saved_run_id):
    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
    mycur = db.cursor()
    s1 = """SELECT xdata2 FROM xdata_table WHERE saved_run_id = %s"""
    i = mycur.execute(s1, (saved_run_id, ))
    if i == 0:
        db.close()
        return None
    else:
        x2 = pickle.loads(zlib.decompress(mycur.fetchall()[0][0][1:-1]))
        db.close()
        return x2

def load_text_output_from_db(saved_run_id):
    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
    mycur = db.cursor()
    s1 = """SELECT text_output FROM text_output_table WHERE saved_run_id = %s"""
    i = mycur.execute(s1, (saved_run_id, ))
    if i == 0:
        db.close()
        return None
    else:
        text_output = zlib.decompress(mycur.fetchall()[0][0][1:-1]).decode("utf-8")
        db.close()
        return text_output

def clean_db(alg_name = None, algorithm_id = None, saved_run_id = None):
    db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
    mycur = db.cursor()
    if not algorithm_id and alg_name:
        s1 = """SELECT algorithm_id FROM algorithms_table WHERE name = %s"""
        i = mycur.execute(s1, (alg_name, ))
        if i == 0:
            db.close()
            return
        else:
            algorithm_id = mycur.fetchall()[0][0]
    
    if saved_run_id:
        s3 = """DELETE FROM xdata_table WHERE saved_run_id = %s"""
        s4 = """DELETE FROM text_output_table WHERE saved_run_id = %s"""
        s5 = """DELETE FROM saved_runs_table WHERE saved_run_id = %s"""
        mycur.execute(s3, (saved_run_id, ))
        mycur.execute(s5, (saved_run_id, ))
        mycur.execute(s4, (saved_run_id, ))
    elif algorithm_id:
        s2 = """DELETE FROM algorithms_table WHERE algorithm_id = %s"""
        s3 = """DELETE xdata_table FROM xdata_table INNER JOIN saved_runs_table ON saved_runs_table.saved_run_id = xdata_table.saved_run_id WHERE saved_runs_table.algorithm_id = %s"""
        s5 = """DELETE text_output_table FROM text_output_table INNER JOIN saved_runs_table ON saved_runs_table.saved_run_id = text_output_table.saved_run_id WHERE saved_runs_table.algorithm_id = %s"""
        s4 = """DELETE FROM saved_runs_table WHERE algorithm_id = %s"""
        mycur.execute(s2, (algorithm_id, ))
        mycur.execute(s3, (algorithm_id, ))
        mycur.execute(s5, (algorithm_id, ))
        mycur.execute(s4, (algorithm_id, ))
    else:
        s2 = """DELETE FROM algorithms_table"""
        s3 = """DELETE FROM xdata_table"""
        s5 = """DELETE FROM text_output_table"""
        s4 = """DELETE FROM saved_runs_table"""
        mycur.execute(s2)
        mycur.execute(s3)
        mycur.execute(s5)
        mycur.execute(s4)

    db.commit()
    db.close()

def slice_EOD_data(csvfile = "~/.zipline/EOD_all/EOD_20200129.csv", outputdir = "~/.zipline/EOD_all/daily"):

    skiprows = 0
    sid = 0
    step = 1000000 #0
   
    csv_names = ['Symbol', 'Date', 'open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio', 'Adj. Open', 'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume']
    pa = pd.read_csv(csvfile, skiprows=skiprows, nrows=step, header=None, names = csv_names, parse_dates=['Date'])
    
    while len(pa) != 0:
        sid += 1
        symbol = pa.Symbol.iloc[0]
        pa_current = pa.loc[pa.Symbol == symbol]
        pa = pa.loc[pa.Symbol != symbol]
        if len(pa) == 0:
            skiprows = skiprows + step
            if sid < 500000:
                print('read next ' + str(step) + ' rows')
                pa = pd.read_csv(csvfile, skiprows=skiprows, nrows=step, header=None, names = csv_names, parse_dates=['Date'])
                pa_current = pa_current.append(pa.loc[pa.Symbol == symbol])
                pa = pa.loc[pa.Symbol != symbol]

#        start_date = pa_current.Date.iloc[0]
#        end_date = pa_current.Date.iloc[-1]
#        ac_date = end_date + pd.Timedelta(days=1)
#        metadata.loc[sid] = start_date, end_date, ac_date, symbol
        
        pa_current.set_index(keys=['Date'], drop=True, inplace=True)
        pa_data = pa_current.drop(columns=["Symbol", 'Adj. Open', 'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume'])

#        sessions = calendar.sessions_in_range(start_session, end_session)
#        pa_data = pa_data.reindex(
#            sessions.tz_localize(None),
#            copy=False,
#            fill_value=0.0,
#        ).fillna(0.0)

        pa_data.to_csv(outputdir+"/"+symbol+".csv")


def metrics_winloss(x, exclude):
    xx = x.loc[x['transactions'].apply(lambda t: len(t)) > 0].transactions
    plist = []
    for d in xx:
        for o in d:
            if o['sid'].symbol in exclude:
                continue    
            p = [p for p in plist if p[0] == o['sid'].symbol]
            if p:
#                print(p)
                if not p[0][3]:
                    p[0][3].append((o['amount'], o['price']))
                else:
                    if p[0][3][0][0] * o['amount'] > 0:
                        p[0][3].append((o['amount'], o['price']))
                    else:
                        num = 0
                        pos = 0.0
                        while abs(num) < abs(o['amount']):
#                            print(p[0][3][0], num, pos, o['amount'])
                            if abs(num + p[0][3][0][0]) >= abs(o['amount']):
                                amount = p[0][3][0][0] + o['amount'] + num
                                price = p[0][3][0][1]
                                num += p[0][3][0][0]
                                pos -= p[0][3][0][0] * p[0][3][0][1]
                                pos = -o['amount'] * (o['price'] + pos / num)
                                del p[0][3][0]
                                if amount != 0:
                                    p[0][3].insert(0, (amount, price))
                                break
                            else:
                                num += p[0][3][0][0]
                                pos -= p[0][3][0][0] * p[0][3][0][1]
                                del  p[0][3][0]
                                if not p[0][3]:
                                    break

#                        print(num, pos)
                        if abs(num) == abs(o['amount']):
                            if pos > 0:
                                p[0][1] += 1
                            else:
                                p[0][2] += 1
                        elif abs(num) < abs(o['amount']):
                            p[0][3].append((o['amount'] + num, o['price']))
                            if (num > 0 and pos/num < o['price']) or (num < 0 and -pos/num > o['price']):
                                p[0][1] += 1
                            else:
                                p[0][2] += 1
                        else:
                            if (num > 0 and pos > 0) or (num < 0 and pos < 0):
                                p[0][1] += 1
                            else:
                                p[0][2] += 1
#                        print(p[0])
                    
            else:
                plist.append([o['sid'].symbol, 0, 0, [(o['amount'], o['price'])]])
    
#    print(plist)
    win = 0
    loss = 0
    for p in plist:
        num = 0
        pos = 0.0
        if p[3]:
            for o in p[3]:
                num += o[0]
                pos += o[0] * o[1]
            pr = [q['last_sale_price'] for q in x.positions[-1] if p[0] == q['sid'].symbol][0]
            if (num > 0 and pos < pr * num) or (num < 0 and pos > pr*num):
                p[1] +=1
            else:
                p[2] +=1
        win += p[1]
        loss += p[2]
    if loss != 0:
        return win/loss
    else:
        return "no loss"
