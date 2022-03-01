# -*- coding: utf-8 -*-
"""
Created on Sun Jan  2 21:17:25 2022

@author: Lukas
"""

import numpy as np
import pandas as pd
import json
import datetime as dt
import os
import ccxt
import plotly.graph_objects as go
import time
import matplotlib.pyplot as plt
from scipy import stats
import pytz
from dateutil.tz import tzutc, tzlocal
from copy import deepcopy


# read a list of all available exchanges from the package
exchg_list = ccxt.exchanges

# define a dictionary of selected, most common currencies to investigate
curr_names = ["Bitcoin Cash", "Bitcoin", "Ethereum", "Litecoin", "Ripple"]
curr_codes = ["BCH", "BTC", "ETH", "LTC", "XRP"]
curr = dict(zip(curr_codes, curr_names))

# create list of exchanges to investigate by index into overall list -  this may be adapted
cert_exchg_idx_3 = [62,85]
cert_exchg_list = [exchg_list[j] for j in range(0,len(exchg_list)) if j in cert_exchg_idx_3]



# save resulting article list as json file
def save_data_to_json_file(my_list, filename):
    # create json string
    json_string = json.dumps(my_list, indent = 4)
    # open, write to, and close json file
    json_file = open(filename, "w")
    json_file.write(json_string)
    json_file.close() 
    return None

# read data from json file
def read_data_from_json_file (fn):
    # open, load from, and close json file
    # at this point we ensure manually that files exist, "try... except..."  may be added later 
    f = open(fn)
    d = json.load(f)
    f.close()
    return d


# read symbols of selected exchanges from file into dictionary
# the file may be edited manually or updated automatically later on
def read_in_filtered_symbols_from_file(fn):
    # define full path of file
    full_fn = os.path.join(os.getcwd(),fn)
    # read in json file
    fs = read_data_from_json_file(full_fn)
    return fs

# the data structure "fetched_symbols" will be used to control which OHLCV data will be downloaded from the web
fetched_symbols = {}
fetched_symbols = read_in_filtered_symbols_from_file("filtered symbols of certified exchanges 4.json")


# read OHLCV data from the web
def fetch_ohlcv_from_web (exchg_str, curr, sym, tf, dt_from, dt_to, limit):
    # exchg_str : exchange to read,  curr : currency to check,  sym : symbol to read
    # tf : timeframe (e.g. 1m for 1 minute), dt_from, dt_to : start/end date of period to read
    # limit : parameter for reading OHLCV data
    
    # start with empty list
    od = []
    # connect to actual exchange
    act_exchg = eval ('ccxt.%s ()' % exchg_str)
    # read market data for this exchange from the web
    act_markets = act_exchg.load_markets()    
    # read available currencies of this exchange 
    act_currs = act_exchg.currencies
    # determine whether market/symbol is active in this exchange  (list will contain only one element)
    market_active = [v["active"] for (k,v) in iter(act_markets.items()) if k == sym][0]   
    # determine whether currency is active in this market
    curr_active =   [v["active"] for (k,v) in iter(act_currs.items()) if k == curr][0]
    if market_active and curr_active:
        # check if OHLCV data is available in this exchange
        if act_exchg.has["fetchOHLCV"]:
            # convert datetimes to timestamps
            dt_base = dt.datetime(1970, 1, 1)  
            act_t   = (dt_from - dt_base).total_seconds() * 1000
            act_end = (dt_to   - dt_base).total_seconds() * 1000
            # loop to read chunks of OHLCV data
            while act_t < act_end:
                # waiting time to avoid getting banned from the exchange due to too frequent requests
                # time.sleep wants seconds, no milliseconds
                time.sleep (act_exchg.rateLimit / 1000)   
                # fetch OHLCV data
                ohlcv_data = act_exchg.fetch_ohlcv(symbol = sym, timeframe = tf, 
                                                   since = act_t, limit = limit)
                # message to indicate progress of fetching
                print ("%16d   %16s  %4d" % ((act_end - act_t)/(1000*limit*60), 
                                             dt.datetime.fromtimestamp(act_t/1000.0).astimezone(tzutc()), 
                                             len(ohlcv_data)))
                # as long is there is still data to fetch
                if len(ohlcv_data):
                    # read timestamp of last data chunk and add delta_t => new start timestamp for next loop cycle
                    act_t = ohlcv_data[-1][0] + (ohlcv_data[-1][0] - ohlcv_data[-2][0])  
                    # add data chunk to list
                    od += ohlcv_data
                else:
                    break
        # there was no data to fetch
        else: print("    No OHLCV data to fetch in exchange" + exchg_str)
    # market and/or currency was not active
    else:
        if not market_active: print("    Market ", sym, " not active !")
        else: print("    Currency ", curr, " not active !")
    # returns a list of lists
    return od


def read_in_ohlcv_data_from_web (fs, tf, from_str, to_str, limit, target_dir, fetch_str, save_it = False):
    # fs: dict of exchanges / symbols, tf: timeframe for OHLCV data, 
    # from_str, to_str: date strings for start and end of reading period
    # limit : parameter for reading OHLCV data
    # taret_dir, fetch_str, save_it: parameters for saving data to json files 

    # list of exchanges to read
    exchg_list = fs.keys()
    # start with empty dictionary  -  a two-staged dict of dicts for exchanges and their symbols
    # first level: exchanges
    fo = dict.fromkeys(exchg_list, {})
    # loop over exchanges to read
    for i,exchg_str in enumerate(exchg_list):
        print("%3d Exchange %s" % (i, exchg_str))
        # read symbol list of the actual exchange 
        sym_list = fs[exchg_str]["filtered symbols"]
        # create list of currencies from symbol list - will be used for second level of dictionary
        curr_str_list = [sym[0:sym.find("/")] for sym in sym_list]
        # start with empty dictionary for symbols
        fo[exchg_str] = dict.fromkeys(curr_str_list, [])

        # loop over all currencies in current exchange
        for j,curr_str in enumerate(curr_str_list):
            # select symbol from list
            sym = sym_list[j]
            print("  %2d Exchange %s - Currency %s - Symbol %s" % (j, exchg_str, curr_str, sym))
            # convert date strings to datetime objects
            p = "%Y-%m-%d %H_%M_%S"
            dt_from = dt.datetime.strptime(from_str, p)
            dt_to   = dt.datetime.strptime(to_str, p)
            # call fetch function and add result (= list of lists) to dictionary 
            fo[exchg_str][curr_str] = fetch_ohlcv_from_web(exchg_str, curr_str, 
                                                           sym, tf, dt_from, dt_to, limit)

            # create json filename from parameters
            fn = fetch_str + "  ohlcv  " + tf + "  " + from_str + "  " + to_str + "  " + \
                exchg_str + "  " + sym.replace("/","-") + ".json"
            # create full path for json file
            full_fn = os.path.join(os.getcwd(), target_dir, fn)
            if save_it:
                # save json file
                save_data_to_json_file(fo[exchg_str][curr_str], full_fn)
                print("      saved: ", sym)          
    # return dict of dicts
    return fo

        
# convert any local time to UTC time - all data will be based upon UTC time
def make_utctime(t):
    t1 = dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S").astimezone(tzlocal())
    t1 = t1.replace(tzinfo = tzutc())
    t2 = str(t1.strftime("%Y-%m-%d %H_%M_%S"))
    # return a datetime object and a string
    return t1, t2

 
### main routine to fetch data from the web

# determine subdirectory to save OHLCV data to  (dir is in current working directiory) 
target_dir = "ohlcv data 220101-220131 1m"
# define start and end of OHLCV reading period 
start = "2022-01-01 00:00:00"
end   = "2022-02-01 00:00:00"
start_utc, start_str = make_utctime(start)
end_utc, end_str = make_utctime(end)
# check plausibility of dates
dates_ok = (end_utc > start_utc)
# determine time of reading 
fetch_str = str(dt.datetime.now().strftime("%Y-%m-%d %H_%M_%S"))

# start with an empty dict of dicts with exchanges on first level and markets/symbols on second level
fetched_ohlcvs = {}
if dates_ok:
    # read OHLCV data 
    fetched_ohlcvs = read_in_ohlcv_data_from_web(fetched_symbols, "1m", start_str, end_str, 360,
                                                  target_dir, fetch_str, save_it = True)
else:
    print("ERROR: End date before Start date !  -  Check Start and End date !")
    
    
### dict of dicts with fetched OHLCV data will be converted into dataframe for further analysis 

# column headers for dataframe
column_headers = ["EPOCH", "Open", "High", "Low", "Close", "Volume", "Timestamp", 
                  "Date", "Year", "Month", "Day", "Hour", "Minute", "Second", 
                  "WeekDay", "YearDay"]

# conversion function
def convert_ohlcv_to_df(fo):
    print("\nConverting OHLCV data into dataframe:")
    # start with empty data structure
    dfo = {} 
    # loop over all exchanges
    for exchg in fo:
        dfo[exchg] = {}
        # loop over all markets/symbols
        for sym in fo[exchg]:
            # check of there are markets/symbols to convert at all 
            if len(fo[exchg][sym]) > 0:
                print(str(exchg), " - ", str(sym))
                # use temporary list of dataframes before concatenating them into one single dataframe
                tmp_dfs = []
                # loop over all rows of list in dictionary 
                for k in range(0,len(fo[exchg][sym])):
                    tmp_epoch = fo[exchg][sym][k][0]
                    tmp_open  = fo[exchg][sym][k][1]
                    tmp_high  = fo[exchg][sym][k][2]
                    tmp_low   = fo[exchg][sym][k][3]
                    tmp_close = fo[exchg][sym][k][4]
                    tmp_vol   = fo[exchg][sym][k][5]
                    tmp_ts    = dt.datetime.fromtimestamp(tmp_epoch // 1000, dt.timezone.utc) 
                    (Y,M,D,h,m,s,wd,yd,dst) = tmp_ts.timetuple()
                    # convert to dataframe
                    tmp_df = pd.DataFrame([[tmp_epoch, tmp_open, tmp_high, tmp_low, tmp_close, tmp_vol,
                                            tmp_ts, dt.date(Y,M,D), Y, M, D, h, m, s, wd, yd]],
                                          columns = column_headers)
                    # add to list
                    tmp_dfs.append(tmp_df)
                    # indicate progress on screen
                    if k % 100 == 0: print(".", end = "")
                print(" ")
                # concatenate list of dataframes into a single one and add it to the new dict of dicts
                dfo[exchg][sym] = pd.concat(tmp_dfs, ignore_index = True)
            else:
                # add an empty dataframe to the new dict of dicts
                dfo[exchg][sym] = pd.DataFrame([])
    # return dict of dicts of dataframes (one dataframe for each pair of exchange and market/symbol)
    return dfo    

# call conversion function wit dict of dicts of OHLCV data fetched from the web
df_ohlcvs = {} 
df_ohlcvs = convert_ohlcv_to_df(fetched_ohlcvs)      


# create subdirectory if not existing yet and save a dataframe to its csv file
def save_df_to_csv_file(df, full_csv_dir, full_fn):
    os.makedirs(full_csv_dir, exist_ok = True)
    df.to_csv(full_fn)
    

# save dict of dicts of dataframes to csv files
# one csv file for each pair of exchange and market/symbol
def save_df_ohlcv_data_to_csv_files(dfo, csv_dir):
    print("\nSaving dataframes to csv files in subdirectory ", csv_dir)
    # loop over exchanges
    for exchg in dfo:
        print("  Exchange: ", str(exchg))
        # loop over markets/symbols
        for sym in dfo[exchg]:
            print("    Currency: ",sym)
            # if dataframe not empty
            if len(dfo[exchg][sym]) > 0:
                # determine full path to csv file
                fn = "df_ohlcv  " + str(exchg) + "  " + sym.replace("/","-") + ".csv"
                full_fn = os.path.join(os.getcwd(), csv_dir, fn)
                full_csv_dir = os.path.join(os.getcwd(), csv_dir)
                # call save function to save dataframe to path
                save_df_to_csv_file(dfo[exchg][sym], full_csv_dir, full_fn)

    
# call main save function for dict of dicts with all dataframes                   
save_df_ohlcv_data_to_csv_files(df_ohlcvs, "df_ohlcvs")           






