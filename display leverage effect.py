# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

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

# list of exchanges - this is currently limited to exchange gateio from the ccxt package
# gateio delivers 1-minute OHLCV data, which is not the case for many other exchanges in the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]

     
# read dict of dicts of dataframes from csv files
def read_df_ohlcv_data_to_csv_files(exchg_list, curr_list, csv_dir):
    # exchg_list:  list of exchanges, curr_list: list of currencies, csv_dir: subdirectory with csv files

    print("\nReading dataframes from csv files in subdirectory ", csv_dir)
    # start with empty two-staged dict of dicts - first level: exchanges
    dfo = dict.fromkeys(exchg_list, {})
    # loop over exchanges
    for exchg_str in exchg_list:
        print("Exchange %s" % exchg_str)
        # start with empty dictionary for symbols
        dfo[exchg_str] = dict.fromkeys(curr_list, [])
        # loop over all currencies in current exchange
        for curr_str in curr_list:
            print("  Exchange %s - Currency %s" % (exchg_str, curr_str))
            # determine full path to csv file
            fn = "df_ohlcv  " + exchg_str + "  " + curr_str + ".csv"
            full_fn = os.path.join(os.getcwd(), csv_dir, fn)
            # read dataframe from its csv file, one csv file for each pair of exchange and market/symbol
            # at this point we manually ensure the file exists,  "try...except" may be added later
            dfo[exchg_str][curr_str] = pd.read_csv(full_fn)
    return dfo

    
# call main read function for dict of dicts with all dataframes                   
df_ohlcvs_test = read_df_ohlcv_data_to_csv_files(cert_exchg_list, curr_list, "df_ohlcvs") 


# Calculate Intraday volatility, Daily returns and some statistic and store in new dataframe 
def calculate_df_ohlcvs_stats_lev(dfo):
    # dfo: dict of dicts of dataframes to calculate statistics for
    
    print("\nCalulating statistics for Intraday volatility, etc. ...")
    # start with empty two-staged dict of dicts - first level: exchanges
    df_st = {} 
    # loop over exchanges
    for exchg in dfo:
        print("Exchange: ",exchg)
        # start with empty dictionary for symbols/currencies - second level
        df_st[exchg] = {}
        # loop over symbols
        for sym in dfo[exchg]:
            # check of there are markets/symbols to convert at all 
            if len(dfo[exchg][sym]) > 0:
                print("  ",sym)
                # create a temporary copy of the input dataframe of the actual exchange/symbo pair
                tmp = dfo[exchg][sym].copy(deep=True)
                # insert new column with index data to be used for grouping of Volume and Price values 
                # here:  every full hour  -  new column is named "DateHour"
                tmp.insert(loc = tmp.shape[1], 
                           column = "DateHour", 
                           value = pd.Series([dt.datetime(tmp["Year"][k], tmp["Month"][k], 
                                                          tmp["Day"][k], tmp["Hour"][k])
                                                          .astimezone(tzlocal())
                                                          .replace(tzinfo = tzutc())
                                              for k in range(0,tmp.shape[0])
                                             ]
                                            )
                           )
                # create new dataframe with statistical data for the actual exchange/symbo pair
                df_st[exchg][sym] = {"data": pd.DataFrame({"DateHour":[], "I_vol":[], 
                                                           "Ret_abs":[], "Ret_pct":[]}), 
                                     "corr": {"coeff": 0, "p-value": 0}}
                # fill new dataframe with grouped data
                # index column DateHour
                df_st[exchg][sym]["data"]["DateHour"]  = tmp.groupby(["DateHour"])["DateHour"].nth(0)                
                # Intraday Volatility: standard deviation of grouped Volume values
                df_st[exchg][sym]["data"]["I_vol"]     = tmp.groupby(["DateHour"])["Volume"].agg([np.std])
                # Absolute Daily returns from first Open value and last Close value
                df_st[exchg][sym]["data"]["Ret_abs"]   = tmp.groupby(["DateHour"])["Close"].nth(-1) - \
                                                         tmp.groupby(["DateHour"])["Open"].nth(0)
                # Relative Daily Returns from absolute daily returns                                         
                df_st[exchg][sym]["data"]["Ret_pct"]   = df_st[exchg][sym]["data"]["Ret_abs"].pct_change()                                         
                # calculate correlation coefficient and related p-value and add to new dataframe
                c,p = stats.pearsonr(df_st[exchg][sym]["data"]["I_vol"], 
                                     df_st[exchg][sym]["data"]["Ret_abs"])
                df_st[exchg][sym]["corr"]["coeff"] = c
                df_st[exchg][sym]["corr"]["p-value"] = p                                                      
            # if no data then add emoty dataframe
            else:
                df_st[exchg][sym] = pd.DataFrame([])
    # return dict of dicts of dataframes with statistical data
    return df_st

df_ohlcvs_stats_lev = {}
df_ohlcvs_stats_lev = calculate_df_ohlcvs_stats_lev(df_ohlcvs_test)



def plot_leverage_effect(dta, plt_wdth, title_str, xlabel_str, ylabel_str, share_x, share_y):
    # dta : dataframes of currencies of actual exchange, 
    # plt_wdth: parameter for plot layout
    # title_str, xlabel_str, ylabel_str: parametrized plot labels
    # share_x, share_y: to control whether to use same scale on x-axis resp. y-axis
    
    # define number of plots in one row  -  all currencies of an exchange side by side 
    plot_cols = len(dta.keys())
    # definition of plot layout
    fig, axes = plt.subplots(ncols = plot_cols, figsize = (plt_wdth, 4), 
                             sharex = share_x, sharey = share_y, tight_layout = True)
    # loop over currencies of actual exchange
    for j, sym in enumerate(list(dta.keys())):
        # select subplot
        ax = axes[j]
        # display scatterplot of percent change of Daily returns over Intraday volatility 
        # drop 1st row of dataframe as pct_change will be NA there
        ax.scatter(dta[sym]["data"].dropna(axis=0)["I_vol"], 
                   dta[sym]["data"].dropna(axis=0)["Ret_pct"], 2)
        # add correlation coefficient and p-value to plot title 
        c = "%.3f" % dta[sym]["corr"]["coeff"]
        p = "%.8f" % dta[sym]["corr"]["p-value"]
        ax.set_title(title_str + sym + "\nCorr.-coeff.: " + c + "  p-value: " + p)
        ax.set_xlabel(xlabel_str)
        if j == 0: ax.set_ylabel(ylabel_str)
        
                
def scatterplot_exchanges(df_st, share_x, share_y):
    # loop over all exchanges
    for i,exchg in enumerate(list(df_st.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(df_st[exchg][k]) > 0  for k in iter(df_st[exchg].keys())]):
            # call function to create scatterplots for the actual exchange
            plot_leverage_effect(df_st[exchg], 24, 
                                 "Leverage effect: " + exchg + " - ", 
                                 "Intrahour volatility", "Daily returns", share_x, share_y)



# main routine to calculate and display scatterplots showing Leverage effect
# parameters: dict of dicts of dataframes, use same scale on x-axis resp. y-axis                            

print("\nCalculating data and plotting may take a few seconds, please be patient !")
scatterplot_exchanges(df_ohlcvs_stats_lev, False, False)
# scatterplot_exchanges(df_ohlcvs_stats, True, False)
