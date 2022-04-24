# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

# functions to create various timeline plots

# for figures 2.1 and 2.2

import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited ot exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]



# Plot Volume and Close values over time, each time in a single plot 
def timeline_plots(exchg, exchg_str):
    # create list of datetimes for x-axis
    my_line_style=["b-","r-","g-","c-","m-"]
    p = "%Y-%m-%d %H:%M:%S%z"
    sym = list(exchg.keys())[0]
    ts = []
    for i in range(len(exchg[sym]["Timestamp"])):
        ts.append(dt.datetime.strptime(exchg[sym]["Timestamp"][i], p))
        
    # Plot Close values for all symbols in a single plot
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(24,12))
    for i,sym in enumerate(exchg.keys()):
        ax.plot(ts, exchg[sym]["Close"], my_line_style[i], label=sym)
    ax.set_title("Price in USD", fontsize=24)
    ax.tick_params(axis='both', labelsize=20)
    ax.set_xlabel('Date', fontsize=20)  # Add an x-label to the axes.
    ax.set_ylabel('Price in USD', fontsize=20)  # Add a y-label to the axes.
    ax.set_yscale('log')
    ax.legend()
    ax.xaxis.set_major_locator(mdates.YearLocator(month=1, day=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=([1,4,7,10])))
    ax.grid(visible=True, axis="both")
    plt.show()
    
    # Plot Volume values for all symbols in a single plot
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(24,12))
    for i,sym in enumerate(exchg.keys()):
        ax.plot(ts, exchg[sym]["Volume"], my_line_style[i], label=sym, alpha=0.5)
    ax.set_title("Exchange " + exchg_str + "  -  Volumes")
    ax.title.set_size(24)
    ax.tick_params(axis='both', labelsize=20)
    ax.set_xlabel('Date', fontsize=20)  # Add an x-label to the axes.
    ax.set_ylabel('Volume', fontsize=20)  # Add a y-label to the axes.
    # ax.set_yscale('log')
    ax.legend()
    ax.xaxis.set_major_locator(mdates.YearLocator(month=1, day=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=([1,4,7,10])))
    ax.grid(visible=True, axis="both")
    plt.show()
          
    
 
# Plot Volume values over time in individual plots per symbol 
def timeline_plots_by_volumes(exchg, exchg_str):
    # create list of datetimes for x-axis
    my_line_style=["b-","r-","g-","c-","m-","k-"]
    p = "%Y-%m-%d %H:%M:%S%z"
    sym = list(exchg.keys())[0]
    ts = exchg[sym].groupby(["Date"])["Timestamp"].nth(0)
    for i in range(len(ts)):
        ts.iloc[i] = dt.datetime.strptime(ts.iloc[i], p)
    
    fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(24,18), tight_layout = True)
    for i,sym in enumerate(exchg.keys()):
        # for each symbol, aggregate Volume information per day into temporary dataframe
        vol = exchg[sym].groupby(["Date"])["Volume"].agg([np.sum])
        pd_temp = pd.concat([ts, vol], axis = 1, ignore_index=True)
        pd_temp.columns = ["Time", "Volume"]
        # and plot it
        axes[i].plot(pd_temp["Time"], pd_temp["Volume"], my_line_style[i], label=sym)
        axes[i].set_title(sym, fontsize=24)
        axes[i].tick_params(axis='both', labelsize=20)
        axes[i].set_xlabel('Date', fontsize=20)  # Add an x-label to the axes.
        axes[i].set_ylabel('Volume', fontsize=20)  # Add a y-label to the axes[i]es.
        axes[i].set_yscale('log')
        axes[i].legend(fontsize=16)
        axes[i].xaxis.set_major_locator(mdates.YearLocator(month=1, day=1))
        axes[i].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        axes[i].xaxis.set_minor_locator(mdates.MonthLocator(bymonth=([1,4,7,10])))
        axes[i].grid(visible=True, axis="both")
    plt.show() 



# Plot Close values over time in individual plots per symbol 
def timeline_plots_by_close_price(exchg, exchg_str):
    # create list of datetimes for x-axis    
    my_line_style=["b-","r-","g-","c-","m-","k-"]
    p = "%Y-%m-%d %H:%M:%S%z"
    sym = list(exchg.keys())[0]
    ts = exchg[sym].groupby(["Date"])["Timestamp"].nth(0)
    for i in range(len(ts)):
        ts.iloc[i] = dt.datetime.strptime(ts.iloc[i], p)
    
    fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(24,18), tight_layout = True)
    for i,sym in enumerate(exchg.keys()):
        # for each symbol, aggregate Close information per day into temporary dataframe
        cls_pr = exchg[sym].groupby(["Date"])["Close"].nth(-1)
        pd_temp = pd.concat([ts, cls_pr], axis = 1, ignore_index=True)
        pd_temp.columns = ["Time", "Close"]
        # and plot it        
        axes[i].plot(pd_temp["Time"], pd_temp["Close"], my_line_style[i], label=sym)
        axes[i].set_title(sym, fontsize=24)
        axes[i].tick_params(axis='both', labelsize=20)
        axes[i].set_xlabel('Date', fontsize=20)  # Add an x-label to the axes.
        axes[i].set_ylabel('Close', fontsize=20)  # Add a y-label to the axes[i]es.
        axes[i].set_yscale('log')
        axes[i].legend(fontsize=16)
        axes[i].xaxis.set_major_locator(mdates.YearLocator(month=1, day=1))
        axes[i].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        axes[i].xaxis.set_minor_locator(mdates.MonthLocator(bymonth=([1,4,7,10])))
        axes[i].grid(visible=True, axis="both")
    plt.show() 


# Plot Volume values over time in individual plots per symbol and per year
def timeline_plots_volumes_by_year(exchg, exchg_str):
    my_colors=["b","r","g","c","m","y"]
    # create temporary dictionary
    volumes = dict.fromkeys(exchg.keys())    
    # and fill it for each symbol key by pivoting original data of that symbol
    # Row index is Date,  Column Index is Year
    for sym in exchg.keys():
        volumes[sym] = exchg[sym].pivot(index=["Month", "Day", "Hour"], columns="Year", values="Volume")    
    # setup subplot grid
    fig, axes = plt.subplots(nrows=6, ncols=5, figsize=(24,18), sharex=True, tight_layout=True)
    for i,sym in enumerate(exchg.keys()):                 
        axes[0,i].set_title(sym + " Volume")
        axes[0,i].title.set_size(16)
        for j,v in enumerate(list(volumes[sym].columns)):
            axes[j,i].plot(list(volumes[sym][v]), 
                           color=my_colors[j], label=v, alpha=0.5)  
            axes[j,i].tick_params(axis='both', labelsize=12)
            if i==0: axes[j,i].set_ylabel('Volume', fontsize=12)  # Add a y-label to the axes.
            axes[j,i].grid(visible=True, axis="both")
            axes[j,i].legend()
        axes[j,i].set_xlabel('Hour within year', fontsize=12)  # Add an x-label to the axes.
        
   
   
# call main read function for dict of dicts with all dataframes      
start_date = "2017-11-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "1h"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                            tf, start_date, end_date) 

# create all timeline plots, here only for one exchange: gateio
for exchg in df_ohlcvs.keys():
    print("Exchange: ", exchg)
    timeline_plots(df_ohlcvs[exchg], exchg)
    timeline_plots_by_volumes(df_ohlcvs[exchg], exchg)
    timeline_plots_by_close_price(df_ohlcvs[exchg], exchg)
    timeline_plots_volumes_by_year(df_ohlcvs[exchg], exchg)
    
    
    

    

    

