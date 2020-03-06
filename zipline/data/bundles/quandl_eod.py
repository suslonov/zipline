#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module for building a complete daily dataset from Quandl's EOD dataset.
"""
from io import BytesIO
from itertools import count
from time import time, sleep

from click import progressbar
from logbook import Logger
import pandas as pd
import requests
from six.moves.urllib.parse import urlencode
import urllib.error

from zipline.utils.calendars import register_calendar_alias
from zipline.utils.cli import maybe_show_progress

from zipline.data.bundles import core as bundles

log = Logger(__name__)
seconds_per_call = (pd.Timedelta('10 minutes') / 5000).total_seconds()
# Invalid symbols that quandl has had in its metadata:
excluded_symbols = frozenset({'TEST123456789', 'ZZK'})


def _fetch_raw_metadata(api_key, cache, retries, environ):
    """Generator that yields each page of data from the metadata endpoint
    as a dataframe.
    """
    for page_number in count(1):
        key = 'metadata-page-%d' % page_number
        if key in cache:
            raw = cache[key]
        else:
            for _ in range(retries):
                try:
                    raw = pd.read_csv(
                        format_metadata_url(api_key, page_number),
                        parse_dates=[
                            'oldest_available_date',
                            'newest_available_date',
                        ],
                        usecols=[
                            'dataset_code',
                            'name',
                            'oldest_available_date',
                            'newest_available_date',
                        ],
                    )
                    break
                except urllib.error.HTTPError as e:
                    if e.code == 416:
                        raw = pd.DataFrame([])
                        break
            else:
                raise ValueError(
                    'Failed to download metadata page %d after %d'
                    ' attempts.' % (page_number, retries),
                )

            cache[key] = raw

        if raw.empty:
            # use the empty dataframe to signal completion
            break
        yield raw


def fetch_symbol_metadata_frame(api_key,
                                cache,
                                retries=5,
                                environ=None,
                                show_progress=False):
    """
    Download Quandl symbol metadata.
    Parameters
    ----------
    api_key : str
        The quandl api key to use. If this is None then no api key will be
        sent.
    cache : DataFrameCache
        The cache to use for persisting the intermediate data.
    retries : int, optional
        The number of times to retry each request before failing.
    environ : mapping[str -> str], optional
        The environment to use to find the zipline home. By default this
        is ``os.environ``.
    show_progress : bool, optional
        Show a progress bar for the download of this data.
    Returns
    -------
    metadata_frame : pd.DataFrame
        A dataframe with the following columns:
          symbol: the asset's symbol
          name: the full name of the asset
          start_date: the first date of data for this asset
          end_date: the last date of data for this asset
          auto_close_date: end_date + one day
          exchange: the exchange for the asset; this is always 'quandl'
        The index of the dataframe will be used for symbol->sid mappings but
        otherwise does not have specific meaning.
    """
    raw_iter = _fetch_raw_metadata(api_key, cache, retries, environ)

    def item_show_func(_, _it=iter(count())):
        'Downloading page: %d' % next(_it)

    with maybe_show_progress(raw_iter,
                             show_progress,
                             item_show_func=item_show_func,
                             label='Downloading EOD metadata: ') as blocks:
        data = pd.concat(blocks, ignore_index=True).rename(columns={
            'dataset_code': 'symbol',
            'name': 'asset_name',
            'oldest_available_date': 'start_date',
            'newest_available_date': 'end_date',
        }).sort_values('symbol')

    data = data[~data.symbol.isin(excluded_symbols)]
    # cut out all the other stuff in the name column
    # we need to escape the paren because it is actually splitting on a regex
    data.asset_name = data.asset_name.str.split(r' \(', 1).str.get(0)
    data['exchange'] = 'QUANDL_EOD'

    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)

    return data


def format_metadata_url(api_key, page_number):
    """Build the query RL for the quandl EOD metadata.
    """
    query_params = [
        ('per_page', '100'),
        ('sort_by', 'id'),
        ('page', str(page_number)),
        ('database_code', 'EOD'),]
    if api_key is not None:
        query_params = [('api_key', api_key)] + query_params
    return (
        'https://www.quandl.com/api/v3/datasets.csv?' + urlencode(query_params)
    )


def format_eod_url(api_key, symbol, start_date, end_date):
    """
    Build a query URL for a quandl EOD dataset.
    """
    query_params = [
        ('start_date', start_date.strftime('%Y-%m-%d')),
        ('end_date', end_date.strftime('%Y-%m-%d')),
        ('order', 'asc'),
    ]
    if api_key is not None:
        query_params = [('api_key', api_key)] + query_params

    return (
        "https://www.quandl.com/api/v3/datasets/EOD/"
        "{symbol}.csv?{query}".format(
            symbol=symbol,
            query=urlencode(query_params),
        )
    )


def fetch_single_equity(api_key,
                        symbol,
                        start_date,
                        end_date,
                        retries=5):
    """
    Download data for a single equity.
    """
    for _ in range(retries):
        try:
            return pd.read_csv(
                format_eod_url(api_key, symbol, start_date, end_date),
                parse_dates=['Date'],
                index_col='Date',
                usecols=[
                    'Date',
                    'Open',
                    'High',
                    'Low',
                    'Close',
                    'Volume',
                    'Dividend',
                    'Split',
                    'Adj_Open',
                    'Adj_High',
                    'Adj_Low',
                    'Adj_Close',
                    'Adj_Volume',
                ],
                na_values=['NA'],
            ).rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Date': 'date',
                'Dividend': 'ex_dividend',
                'Split': 'split_ratio',
            })[['open', 'high', 'low', 'close', 'volume', 'ex_dividend', 'split_ratio']]
        except Exception:
            log.exception("Exception raised reading Quandl data. Retrying.")
    else:
        raise ValueError(
            "Failed to download data for %r after %d attempts." % (
                symbol, retries
            )
        )


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


def gen_symbol_data(api_key,
                    cache,
                    symbol_map,
                    calendar,
                    start_session,
                    end_session,
                    splits,
                    dividends,
                    retries):
    for asset_id, symbol in symbol_map.items():
        start_time = time()
        if symbol in cache:
            # see if we have this data cached.
            raw_data = cache[symbol]
            should_sleep = False
        else:
            # we need to fetch the data and then write it to our cache
            raw_data = cache[symbol] = fetch_single_equity(
                api_key,
                symbol,
                start_date=start_session,
                end_date=end_session,
            )
            should_sleep = True

        _update_splits(splits, asset_id, raw_data)
        _update_dividends(dividends, asset_id, raw_data)

        sessions = calendar.sessions_in_range(start_session, end_session)

        raw_data = raw_data.reindex(
            sessions.tz_localize(None),
            copy=False,
        ).fillna(0.0)
        yield asset_id, raw_data

        if should_sleep:
            remaining = seconds_per_call - time() - start_time
            if remaining > 0:
                sleep(remaining)


@bundles.register('quandl-eod')
def quandl_bundle(environ,
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
    """Build a zipline data bundle from the Quandl EOD dataset.
    """
    api_key = environ.get('QUANDL_API_KEY')
    metadata = fetch_symbol_metadata_frame(
        api_key,
        cache=cache,
        show_progress=show_progress,
    )
    symbol_map = metadata.symbol

    # data we will collect in `gen_symbol_data`
    splits = []
    dividends = []

    asset_db_writer.write(metadata)
    daily_bar_writer.write(
        gen_symbol_data(
            api_key,
            cache,
            symbol_map,
            calendar,
            start_session,
            end_session,
            splits,
            dividends,
            environ.get('QUANDL_DOWNLOAD_ATTEMPTS', 5),
        ),
        show_progress=show_progress,
    )
    adjustment_writer.write(
        splits=pd.concat(splits, ignore_index=True),
        dividends=pd.concat(dividends, ignore_index=True),
    )


def download_with_progress(url, chunk_size, **progress_kwargs):
    """
    Download streaming data from a URL, printing progress information to the
    terminal.
    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.
    chunk_size : int
        Number of bytes to read at a time from requests.
    **progress_kwargs
        Forwarded to click.progressbar.
    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers['content-length'])
    data = BytesIO()
    with progressbar(length=total_size, **progress_kwargs) as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            data.write(chunk)
            pbar.update(len(chunk))

    data.seek(0)
    return data


def download_without_progress(url):
    """
    Download data from a URL, returning a BytesIO containing the loaded data.
    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.
    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    return BytesIO(resp.content)


register_calendar_alias("QUANDL_EOD", "NYSE")
