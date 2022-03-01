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

# list of exchanges - this is currently limited ot exchange gateio from the ccxt package
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



def boxplot_by_time(exchg_dta, grp_by, data_to_grp, plt_wdth, title_str, xlabel_str, ylabel_str, share_y):
    # exchg_dta : dataframes of currencies of actual exchange, 
    # grp_by: variable in df to use its unique values for x-ticks of boxplot
    # data_to_grp:  variable to investigate for seasonality
    # plt_wdth: parameter for plot layout
    # title_str, xlabel_str, ylabel_str: parametrized plot labels
    # share_y: to control whether to use same scale on y-axis
        
    # define number of plots in one row  -  all currencies of an exchange side by side     
    plot_cols = len(exchg_dta.keys())
    # definition of plot layout
    fig, axes = plt.subplots(ncols = plot_cols, figsize = (plt_wdth, 4), 
                             sharey = share_y,  tight_layout = True)
    # loop over currencies of actual exchange
    for j, sym in enumerate(list(exchg_dta.keys())):
        # unique x-ticks from dataframe
        grp_by_set = set(exchg_dta[sym][grp_by])
        # calculation of plot layout
        set_len = len(exchg_dta[sym][data_to_grp])
        this_col = len(grp_by_set)
        this_row = set_len // this_col
        # initialize empty array to transfer data for boxplots into
        this_data = np.zeros((this_row, this_col), dtype = float)
        # loop over groups of data from dataframe column to investigate
        # one array column keeps data for one box  
        for c,g in enumerate(grp_by_set):    
            this_data[:,c] = [exchg_dta[sym][data_to_grp][k] 
                              for k in range(0, set_len) if exchg_dta[sym][grp_by][k] == g]
        # select subplot
        ax = axes[j]
        # display boxplot using data from array above
        ax.boxplot(this_data, 
                   labels = grp_by_set, 
                   patch_artist=True, showfliers=False,
                   medianprops={"color": "white", "linewidth": 1.5},
                   boxprops={"facecolor": "C0", "edgecolor": "white", "linewidth": 0.5},
                   whiskerprops={"color": "C0", "linewidth": 0.5},
                   capprops={"color": "C0", "linewidth": 0.5})
        ax.set_title(title_str + sym)
        ax.set_xlabel(xlabel_str)
        if j == 0: ax.set_ylabel(ylabel_str)
        
                    
def boxplot_exchanges(dfo, to_show, share_y):
    # loop over all exchanges
    for i,exchg in enumerate(list(dfo.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(dfo[exchg][k]) > 0  for k in iter(dfo[exchg].keys())]):
            # call fucntion to create boxplots for the actual exchange 
            boxplot_by_time (dfo[exchg], "Hour", to_show, 24,
                             "Exchange: "+ exchg + " - Symbol: ", 
                             "Hour of day", to_show, share_y)
   

# main routine to calculate and display boxplots showing seasonality of data     
# parameters: dict of dicts of dataframes, variable to investigate, use same scale on y-axis
print("\nProcessing and plotting a larger amount of data may take a few seconds, please be patient !")
boxplot_exchanges(df_ohlcvs_test, "Volume", False)
boxplot_exchanges(df_ohlcvs_test, "Close", False)           
