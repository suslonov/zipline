#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module for building a complete daily dataset from Quandl's EOD dataset.
"""
import pandas as pd
from numpy import empty

from zipline.utils.calendars import register_calendar_alias
from zipline.data.bundles import core as bundles

def quandl_csv_bundle_ingest(csvfile):
        return CSVQUANDLBundle(csvfile).ingest

class CSVQUANDLBundle:
    def __init__(self, csvfile=None):
        self.csvfile = csvfile

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        quandl_csv_bundle(environ,
                      asset_db_writer,
                      minute_bar_writer,
                      daily_bar_writer,
                      adjustment_writer,
                      calendar,
                      start_session,
                      end_session,
                      cache,
                      show_progress,
                      output_dir,
                      self.csvfile)

def _update_splits(splits, asset_id, raw_data):
    split_ratios = raw_data.split_ratio
    df = pd.DataFrame({'ratio': 1 / split_ratios[split_ratios != 1]})
    df.index.name = 'effective_date'
    df.reset_index(inplace=True)
    df['sid'] = asset_id
    splits.append(df)


def _update_dividends(dividends, asset_id, raw_data):
    divs = raw_data.ex_dividend
    df = pd.DataFrame({'amount': divs[divs != 0]})
    df.index.name = 'ex_date'
    df.reset_index(inplace=True)
    df['sid'] = asset_id
    # we do not have this data in the EOD dataset
    df['record_date'] = df['declared_date'] = df['pay_date'] = pd.NaT
    dividends.append(df)


def gen_symbol_data(csvfile,
                    start_session,
                    end_session,
                    calendar,
                    symbol_map,
                    metadata,
                    splits,
                    dividends): 

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

        symbol_map.append(symbol)
        start_date = pa_current.Date.iloc[0]
        end_date = pa_current.Date.iloc[-1]
        ac_date = end_date + pd.Timedelta(days=1)
        metadata.loc[sid] = start_date, end_date, ac_date, symbol
        
        pa_current.set_index(keys=['Date'], drop=True, inplace=True)
        pa_data = pa_current.drop(columns=["Symbol", 'Adj. Open', 'Adj. High', 'Adj. Low', 'Adj. Close', 'Adj. Volume'])

        sessions = calendar.sessions_in_range(start_session, end_session)
        pa_data = pa_data.reindex(
            sessions.tz_localize(None),
            copy=False,
            fill_value=0.0,
        ).fillna(0.0)

        _update_splits(splits, sid, pa_data)
        _update_dividends(dividends, sid, pa_data)

        yield sid, pa_data

@bundles.register('quandl-eod-csv')
def quandl_csv_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir,
                  csvfile):
    """Build a zipline data bundle from the Quandl EOD dataset.
    """

    # data we will collect in `gen_symbol_data`
    dtype = [('start_date', 'datetime64[ns]'),
             ('end_date', 'datetime64[ns]'),
             ('auto_close_date', 'datetime64[ns]'),
             ('symbol', 'object')]
    metadata = pd.DataFrame(empty(0, dtype=dtype))
    symbol_map = []
    splits = []
    dividends = []

    daily_bar_writer.write(
        gen_symbol_data(csvfile,
              start_session,
              end_session,
              calendar,
              symbol_map,
              metadata,
              splits,
              dividends,
        ),
        show_progress=show_progress,
    )

    metadata['exchange'] = "QUANDL_EOD"

    metadata.to_csv('~/.zipline/metadata.csv')

#    asset_db_writer.write(metadata)

#    adjustment_writer.write(
#        splits=pd.concat(splits, ignore_index=True),
#        dividends=pd.concat(dividends, ignore_index=True),
#    )

register_calendar_alias("QUANDL_EOD", "NYSE")
