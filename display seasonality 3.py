# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

# functions to create various boxplots to investigate seasonality

# for figures 3.1 and 3.2

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import date

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited ot exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]


# function to pivot original data into columns of hourly data from 0:00 to 23:00
def calculate_hourly_data(exchg, grp_by, data_to_grp):
    df_st = {} 
    for sym in exchg:
        df_st[sym] = exchg[sym].pivot(index=["Year","Month", "Day"], 
                                      columns=grp_by, 
                                      values=data_to_grp)    
    return df_st



# function to pivot original data into columns of weekly data from Mon to Sun  (0-6) 
# in case of volumes, hourly volumes will be aggregated to daily volumes
def calculate_weekday_data(exchg, grp_by, data_to_grp):
    df_st = {} 
    for sym in exchg:
        # create a temporary copy of the dataframe and add temporary columns to enable pivoting per weekday
        tmp = exchg[sym].copy(deep=True)       
        tmp.insert(loc = exchg[sym].columns.get_loc("YearDay"), 
                   column = "Iso_Cal_Week", 
                   value = [date(exchg[sym]["Year"][k], 
                                 exchg[sym]["Month"][k], 
                                 exchg[sym]["Day"][k]).isocalendar()[1]  
                            for k in range(0, exchg[sym].shape[0])]
                   )  
        tmp.insert(loc = exchg[sym].columns.get_loc("YearDay"), 
                   column = "Iso_Cal_Year", 
                   value = [date(exchg[sym]["Year"][k], 
                                 exchg[sym]["Month"][k], 
                                 exchg[sym]["Day"][k]).isocalendar()[0]  
                            for k in range(0, exchg[sym].shape[0])]
                   ) 
        if data_to_grp == "Volume":
            df_st[sym] = pd.pivot_table(tmp, values=data_to_grp, 
                                        index=["Iso_Cal_Year","Iso_Cal_Week"], columns=grp_by,
                                        aggfunc=np.sum) 
        if data_to_grp == "Close":
            df_st[sym] = pd.pivot_table(tmp, values=data_to_grp, 
                                        index=["Iso_Cal_Year","Iso_Cal_Week"], columns=grp_by,
                                        aggfunc=np.mean) 
    return df_st


# function to create boxplots of Volumes resp. Close values over hours resp. weekdays
def boxplot_by_time(exchg_dta, grp_by, data_to_grp, plt_wdth, xlabel_str, ylabel_str, share_y, b_set_xlabel):
    # Parameters:
    # exchg_dta: original data per exchange (here: only gateio)
    # grp_by: key for grouping,  here either Hour or Weeday
    # data_to_grp: data that will be grouped,  here either Volume or Close
    plot_cols = len(exchg_dta.keys())
    fig, axes = plt.subplots(ncols = plot_cols, figsize = (plt_wdth, 4), 
                             sharey = share_y,  tight_layout = True)
    # select appropriate grouping function
    if grp_by == "Hour": 
        this_data = calculate_hourly_data(exchg_dta, grp_by, data_to_grp)
    if grp_by == "WeekDay":
        this_data = calculate_weekday_data(exchg_dta, grp_by, data_to_grp)
    # for each symbol, create a subplot 
    for j, sym in enumerate(list(exchg_dta.keys())):
        ax = axes[j]
        # adjust lables on x-axis to avoid overlapping
        if grp_by == "Hour": 
            grp_by_set_labels = this_data[sym].columns[::2]
            tmp_blanks = [""] * len(grp_by_set_labels)
            grp_by_set_labels = list(zip(grp_by_set_labels, tmp_blanks))
            grp_by_set_labels = list(np.array(grp_by_set_labels).flatten())
        else:
            grp_by_set_labels = this_data[sym].columns
        # create boxplot        
        ax.boxplot(this_data[sym].dropna(), 
                   labels = grp_by_set_labels, 
                   patch_artist=True, showfliers=False,
                   medianprops={"color": "white", "linewidth": 1.5},
                   boxprops={"facecolor": "C0", "edgecolor": "white", "linewidth": 0.5},
                   whiskerprops={"color": "C0", "linewidth": 0.5},
                   capprops={"color": "C0", "linewidth": 0.5})
        ax.set_title(sym, fontsize=24)
        ax.tick_params(axis='both', labelsize=16)
        # adjust symbols for thousands separator and decimal point for large values or values with fractals
        current_values = ax.get_yticks()
        if (sym == "XRP") and (data_to_grp == "Close"):
            ax.set_yticklabels(['{:.2f}'.format(x).replace(".",",") for x in current_values])
        else:
            ax.set_yticklabels(['{:,.0f}'.format(x).replace(",",".") for x in current_values])
        # adjust axis labels            
        if b_set_xlabel: ax.set_xlabel(xlabel_str, fontsize=16)
        if j == 0: ax.set_ylabel(ylabel_str, fontsize=16)
    return this_data



# function to create sets of different boxplots for Volume/Close values over Hours/Weekdays                    
def boxplot_exchanges(dfo):
    # loop over all exchanges
    for i,exchg in enumerate(list(dfo.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(dfo[exchg][k]) > 0  for k in iter(dfo[exchg].keys())]):
            # call fucntion to create boxplots for the actual exchange 
            df1 = boxplot_by_time (dfo[exchg], "Hour", "Volume", 30, "Hour of day", "Volume", False, False)
            df2 = boxplot_by_time (dfo[exchg], "Hour", "Close",  30, "Hour of day", "Price",  False, True)
            df3 = boxplot_by_time (dfo[exchg], "WeekDay", "Volume", 30, "Day of week", "Volume", False, False)
            df4 = boxplot_by_time (dfo[exchg], "WeekDay", "Close",  30, "Day of week", "Price",  False, True)
    return df1, df2, df3, df4   # returned dataframes are for checking purposes only


    
# call main read function for dict of dicts with all dataframes      
start_date = "2021-03-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "1h"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                            tf, start_date, end_date) 

# main routine to calculate and display boxplots showing seasonality of data     
# parameters: dict of dicts of dataframes
print("\nProcessing and plotting a larger amount of data may take a few seconds, please be patient !")
df1, df2, df3, df4 = boxplot_exchanges(df_ohlcvs)
