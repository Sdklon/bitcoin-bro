#!/usr/bin/env python

"""
  This script is largely based on https://github.com/binance/binance-public-data/tree/master/python.
  It downloads the historical data from the publicly available Binance API
  Refer to notebooks/1.0-Downloading-Data.ipynb for an usage example
"""
import os, sys
import pandas as pd
import urllib.request
from datetime import * 
from pathlib import Path
from binance.spot import Spot

YEARS = ['2017', '2018', '2019', '2020', '2021', '2022']
INTERVALS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo"]
DAILY_INTERVALS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
TRADING_TYPE = ["spot", "um", "cm"]
MONTHS = list(range(1,13))
PERIOD_START_DATE = '2020-01-01'
BASE_URL = 'https://data.binance.vision/'
START_DATE = date(int(YEARS[0]), MONTHS[0], 1)
END_DATE = datetime.date(datetime.now())

BINANCE_CLIENT = Spot()
print(BINANCE_CLIENT.time())

def get_realtime_klines(start_time, ticker="BTCUSDT", interval="1m"):
    client = BINANCE_CLIENT
        
    # Get all klines today up to the latest recorded minute
    realtime_klines = []
    try:
        # API limits max 1000 klines in response
        new_klines = None

        # To handle the case where more than 1000 minutes have elapsed in the day already
        while new_klines is None or len(new_klines) > 0:
            new_klines = client.klines(ticker, interval, startTime=start_time, limit=1000)
            if len(new_klines) > 0:
                realtime_klines.extend(new_klines)
                start_time = new_klines[-1][6] + 1
            
    except Exception as e:
        print(f"Exception getting realtime klines from Binance API: {e}")
        
    return realtime_klines

def get_destination_dir(file_url, folder=None):
  store_directory = os.environ.get('STORE_DIRECTORY')
  if folder:
    store_directory = folder
  if not store_directory:
    store_directory = os.path.dirname(os.path.realpath(__file__))
  return os.path.join(store_directory, file_url)

def get_download_url(file_url):
  return "{}{}".format(BASE_URL, file_url)

def download_file(base_path, file_name, date_range=None, folder=None):
  download_path = "{}{}".format(base_path, file_name)
  if folder:
    base_path = os.path.join(folder, base_path)
#   # Note: So that date_range does not become part of the save_path, so we can prevent saving previously downloaded dates
#   if date_range:
#     date_range = date_range.replace(" ","_")
#     base_path = os.path.join(base_path, date_range)
  save_path = get_destination_dir(os.path.join(base_path, file_name), folder)
  

  if os.path.exists(save_path):
    # print("\nfile already exists! {}".format(save_path))
    return
  
  # make the directory
  if not os.path.exists(base_path):
    Path(get_destination_dir(base_path)).mkdir(parents=True, exist_ok=True)

  try:
    download_url = get_download_url(download_path)
    dl_file = urllib.request.urlopen(download_url)
    length = dl_file.getheader('content-length')
    if length:
      length = int(length)
      blocksize = max(4096,length//100)

    with open(save_path, 'wb') as out_file:
      dl_progress = 0
      print("\nFile Download: {}".format(save_path))
      while True:
        buf = dl_file.read(blocksize)   
        if not buf:
          break
        dl_progress += len(buf)
        out_file.write(buf)
        done = int(50 * dl_progress / length)
        sys.stdout.write("\r[%s%s]" % ('#' * done, '.' * (50-done)) )    
        sys.stdout.flush()

  except urllib.error.HTTPError:
    print("\nFile not found: {}".format(download_url))
    pass

def convert_to_date_object(d):
  year, month, day = [int(x) for x in d.split('-')]
  date_obj = date(year, month, day)
  return date_obj

def get_path(trading_type, market_data_type, time_period, symbol, interval=None):
  trading_type_path = 'data/spot'
  if trading_type != 'spot':
    trading_type_path = f'data/futures/{trading_type}'
  if interval is not None:
    path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/'
  else:
    path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/'
  return path

def download_historical_daily_klines(trading_type, symbols, num_symbols, intervals, start_date, end_date, folder, checksum=0):
  current = 0
  date_range = None

  if start_date and end_date:
    date_range = start_date + " " + end_date

  if not start_date:
    start_date = START_DATE
  else:
    start_date = convert_to_date_object(start_date)

  if not end_date:
    end_date = END_DATE
  else:
    end_date = convert_to_date_object(end_date)

  #Get valid intervals for daily
  intervals = list(set(intervals) & set(DAILY_INTERVALS))
  print("Found {} symbols".format(num_symbols))

  # All Dates
  period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(PERIOD_START_DATE)
  dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
  dates = [date.strftime("%Y-%m-%d") for date in dates]

  for symbol in symbols:
    print("[{}/{}] - start download daily {} klines ".format(current+1, num_symbols, symbol))
    for interval in intervals:
      for date in dates:
        current_date = convert_to_date_object(date)
        if current_date >= start_date and current_date <= end_date:
          path = get_path(trading_type, "klines", "daily", symbol, interval)
          file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date)
          download_file(path, file_name, date_range, folder)

          if checksum == 1:
            checksum_path = get_path(trading_type, "klines", "daily", symbol, interval)
            checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)
            download_file(checksum_path, checksum_file_name, date_range, folder)

    current += 1

def generate_latest_historical_df(trading_type, 
                                  ticker_symbol, 
                                  interval, 
                                  start_date, 
                                  end_date, 
                                  historical_data_dir, 
                                  historical_files_dir, 
                                  historical_df_path,
                                  raw_df_headers,
                                  write_csv=True,
                                 ):
    
    download_historical_daily_klines(trading_type, 
                                     [ticker_symbol], 
                                     1, 
                                     [interval], 
                                     start_date, 
                                     end_date, 
                                     historical_data_dir)

    # Read all files in BINANCE_HISTORICAL_FILES_DIR
    # files = sorted([str(path) for path in historical_files_dir.glob('**/*') if path.is_file()])
    files = [f"{historical_files_dir}/{ticker_symbol}-{interval}-{ts.strftime('%Y-%m-%d')}.zip" for ts in list(pd.date_range(start=start_date, end=end_date))]
    df_list = [pd.read_csv(path, names=raw_df_headers) for path in files]
    historical_df = pd.concat(df_list, axis=0, ignore_index=True)

    if write_csv:
      historical_df.to_csv(historical_df_path, index=False)
    
    return historical_df