# -*- coding: utf-8 -*-
"""
Created on Sun Jan  2 21:17:25 2022

@author: Lukas
"""

import pandas as pd
import json
import datetime as dt
import os
import ccxt
import time
from dateutil.tz import tzutc, tzlocal


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



# read OHLCV data from the web
def fetch_ohlcv_from_web (exchg_str, curr, sym, tf, dt_from, dt_to, limit):
    # exchg_str : exchange to read,  curr : currency to check,  sym : symbol to read
    # tf : timeframe (e.g. 1m for 1 minute), dt_from, dt_to : start/end date of period to read
    # limit : parameter for reading OHLCV data
    
    # start with empty list
    od = []
    could_fetch = False
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
                # as long is there is still data to fetch
                if len(ohlcv_data):
                    # message to indicate progress of fetching
                    print ("%16d   %16s  %4d" % ((act_end - act_t)/(1000*limit*60), 
                                                 dt.datetime.fromtimestamp(act_t/1000.0).astimezone(tzutc()), 
                                                 len(ohlcv_data)))
                    could_fetch = True
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
    return od, could_fetch


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
        print("%3d Exchange %s  %s" % (i, exchg_str, tf))
        # read symbol list of the actual exchange 
        sym_list = fs[exchg_str]["filtered symbols"]
        # create list of currencies from symbol list - will be used for second level of dictionary
        curr_str_list = [sym[0:sym.find("/")] for sym in sym_list]
        # start with empty dictionary for symbols
        fo[exchg_str] = dict.fromkeys(curr_str_list, [])

        # loop over all currencies in current exchange
        for j,curr_str in enumerate(curr_str_list):
            could_fetch = False
            # select symbol from list
            sym = sym_list[j]
            print("  %2d Exchange %s - Currency %s - Symbol %s" % (j, exchg_str, curr_str, sym))
            # convert date strings to datetime objects
            p = "%Y-%m-%d %H_%M_%S"
            dt_from = dt.datetime.strptime(from_str, p)
            dt_to   = dt.datetime.strptime(to_str, p)
            # call fetch function and add result (= list of lists) to dictionary 
            fo[exchg_str][curr_str], could_fetch = fetch_ohlcv_from_web(exchg_str, curr_str, 
                                                                        sym, tf, dt_from, dt_to, limit)

            if could_fetch:
                if save_it:
                    # create json filename from parameters
                    fn = fetch_str + "  ohlcv  " + tf + "  " + from_str + "  " + to_str + "  " + \
                        exchg_str + "  " + sym.replace("/","-") + ".json"
                    # create full path for json file
                    full_fn = os.path.join(os.getcwd(), target_dir, fn)
                    # save json file                
                    save_data_to_json_file(fo[exchg_str][curr_str], full_fn)
                    print("      saved: ", sym)  
            else: print("      could not fetch data")
    # return dict of dicts
    return fo, could_fetch

  
      
# convert any local time to UTC time - all data will be based upon UTC time
def make_utctime(t):
    t1 = dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S").astimezone(tzlocal())
    t1 = t1.replace(tzinfo = tzutc())
    t2 = str(t1.strftime("%Y-%m-%d %H_%M_%S"))
    # return a datetime object and a string
    return t1, t2

    
    
### dict of dicts with fetched OHLCV data will be converted into dataframe for further analysis 

# conversion function
def convert_ohlcv_to_df(fo):
    # column headers for dataframe
    column_headers = ["EPOCH", "Open", "High", "Low", "Close", "Volume", "Timestamp", 
                      "Date", "Year", "Month", "Day", "Hour", "Minute", "Second", 
                      "WeekDay", "YearDay"]
    print("\nConverting OHLCV data into dataframe:")
    # start with empty data structure
    dfo = {} 
    # loop over all exchanges
    for exchg in fo:
        dfo[exchg] = {}
        print("  Exchange: ", str(exchg))
        # loop over all markets/symbols
        for sym in fo[exchg]:
            # check of there are markets/symbols to convert at all 
            print("    Currency: ",sym, end="")
            if len(fo[exchg][sym]) > 0:
                print(" ")
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
                print(" - no data to convert")
    # return dict of dicts of dataframes (one dataframe for each pair of exchange and market/symbol)
    return dfo    
  


# create subdirectory if not existing yet and save a dataframe to its csv file
def save_df_to_csv_file(df, full_csv_dir, full_fn):
    os.makedirs(full_csv_dir, exist_ok = True)
    df.to_csv(full_fn)
    

# save dict of dicts of dataframes to csv files
# one csv file for each pair of exchange and market/symbol
def save_df_ohlcv_data_to_csv_files(dfo, csv_dir, tf, start_str, end_str):
    print("\nSaving dataframes to csv files in subdirectory ", csv_dir)
    # loop over exchanges
    for exchg in dfo:
        print("  Exchange: ", str(exchg))
        # loop over markets/symbols
        for sym in dfo[exchg]:
            print("    Currency: ",sym, end="")
            # if dataframe not empty
            if len(dfo[exchg][sym]) > 0:
                # determine full path to csv file
                fn = "df_ohlcv  " + str(exchg) + "  " + sym.replace("/","-") + \
                     "  " + tf + "  " + start_str + "  " + end_str + ".csv"
                full_fn = os.path.join(os.getcwd(), csv_dir, fn)
                full_csv_dir = os.path.join(os.getcwd(), csv_dir)
                # call save function to save dataframe to path
                save_df_to_csv_file(dfo[exchg][sym], full_csv_dir, full_fn)
                print(" - saved to csv")
            else: print(" - no data to save")



### main routine

# read a list of all available exchanges from the package
exchg_list = ccxt.exchanges

# define a dictionary of selected, most common currencies to investigate
curr_names = ["Bitcoin Cash", "Bitcoin", "Ethereum", "Litecoin", "Ripple"]
curr_codes = ["BCH", "BTC", "ETH", "LTC", "XRP"]
curr = dict(zip(curr_codes, curr_names))

# create list of exchanges to investigate by index into overall list -  this may be adapted
cert_exchg_idx_3 = [62]
cert_exchg_list = [exchg_list[j] for j in range(0,len(exchg_list)) if j in cert_exchg_idx_3]

# the data structure "fetched_symbols" will be used to control which OHLCV data will be downloaded from the web
fetched_symbols = {}
fetched_symbols = read_in_filtered_symbols_from_file("filtered symbols of certified exchanges 5.json")



### main routine to fetch data from the web

# determine subdirectory to save OHLCV data to  (dir is in current working directiory) 
target_dir_json = "ohlcv data - json"
target_dir_csv = "df_ohlcvs - csv"

# define start and end of OHLCV reading period   -  1h data
times = ["2022-03-01", "2022-02-01", "2022-01-01",
          "2021-12-01", "2021-11-01", "2021-10-01", "2021-09-01", "2021-08-01", "2021-07-01",
          "2021-06-01", "2021-05-01", "2021-04-01", "2021-03-01", "2021-02-01", "2021-01-01",
          "2020-12-01", "2020-11-01", "2020-10-01", "2020-09-01", "2020-08-01", "2020-07-01",
          "2020-06-01", "2020-05-01", "2020-04-01", "2020-03-01", "2020-02-01", "2020-01-01",
          "2019-12-01", "2019-11-01", "2019-10-01", "2019-09-01", "2019-08-01", "2019-07-01",
          "2019-06-01", "2019-05-01", "2019-04-01", "2019-03-01", "2019-02-01", "2019-01-01",
          "2018-12-01", "2018-11-01", "2018-10-01", "2018-09-01", "2018-08-01", "2018-07-01",
          "2018-06-01", "2018-05-01", "2018-04-01", "2018-03-01", "2018-02-01", "2018-01-01",
          "2017-12-01", "2017-11-01", "2017-10-01", "2017-09-01", "2017-08-01", "2017-07-01",
          "2017-06-01", "2017-05-01", "2017-04-01", "2017-03-01", "2017-02-01", "2017-01-01",
          "2016-12-01", "2016-11-01", "2016-10-01", "2016-09-01", "2016-08-01", "2016-07-01",
          "2016-06-01", "2016-05-01", "2016-04-01", "2016-03-01", "2016-02-01", "2016-01-01",
          "2015-12-01", "2015-11-01", "2015-10-01", "2015-09-01", "2015-08-01", "2015-07-01",
          "2015-06-01", "2015-05-01", "2015-04-01", "2015-03-01", "2015-02-01", "2015-01-01"
          ]
# define start and end of OHLCV reading period   -  5 min data   (uncomment if neessary)
# times = ["2022-03-01", "2022-02-01", "2022-01-01",
#          "2021-12-01", "2021-11-01", "2021-10-01", "2021-09-01", "2021-08-01", "2021-07-01",
#          "2021-06-01", "2021-05-01", "2021-04-01"]

# timeframes trying to read
timeframes = ["5m", "15m", "30m", "1h"]
limits = [288, 96, 48, 24]
# timeframes = ["1h"]
# limits = [24]


for i,t in enumerate(times):
    if i > 0:     # skip first element of list
        for j,tf in enumerate(timeframes):
            start = times[i]   + " 00:00:00"
            end =   times[i-1] + " 00:00:00"
            start_utc, start_str = make_utctime(start)
            end_utc, end_str = make_utctime(end)
            # check plausibility of dates
            dates_ok = (end_utc > start_utc)
            # determine time of reading 
            fetch_str = str(dt.datetime.now().strftime("%Y-%m-%d %H_%M_%S"))

            # start with an empty dict of dicts with exchanges on first level and markets/symbols on second level
            could_fetch_data = False
            fetched_ohlcvs = {}
            if dates_ok:
                # read OHLCV data 
                fetched_ohlcvs, could_fetch_data = \
                    read_in_ohlcv_data_from_web(fetched_symbols, tf, start_str, end_str, limits[j],
                                                target_dir_json, fetch_str, save_it = True)
            else:
                print("ERROR: End date before Start date !  -  Check Start and End date !")

            if could_fetch_data:
                # call conversion function wit dict of dicts of OHLCV data fetched from the web
                df_ohlcvs = {} 
                df_ohlcvs = convert_ohlcv_to_df(fetched_ohlcvs)    
                    
                # call main save function for dict of dicts with all dataframes                   
                save_df_ohlcv_data_to_csv_files(df_ohlcvs, target_dir_csv, tf, start_str, end_str)           
                break







