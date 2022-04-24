# -*- coding: utf-8 -*-
"""
Created on Thu Apr  7 22:31:13 2022

@author: Lukas
"""

# functions to create autocorrelation plots for investigation of volatility clustering

# for figure 3.7, 3.8, 3.9

import numpy as np
import matplotlib.pyplot as plt
import statsmodels.tsa.api as smt
import math

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited ot exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]


# function to enhance original dataframe with returns and Log returns
def calculate_returns(dfo):
    # loop over all exchanges
    for i,exchg in enumerate(list(dfo.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(dfo[exchg][k]) > 0  for k in iter(dfo[exchg].keys())]):
            for j, sym in enumerate(list(dfo[exchg].keys())):
                # add new columns
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Ret", 
                                        value = dfo[exchg][sym]["Close"] - dfo[exchg][sym]["Open"]
                                        )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Ret_1", 
                                        value = dfo[exchg][sym]["Close"]
                                        )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Ret_2", 
                                        value = dfo[exchg][sym]["Close"].shift(1)
                                        )       
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Quot", 
                                        value = dfo[exchg][sym]["Ret_1"] / dfo[exchg][sym]["Ret_2"]
                                        )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Ret_Log", 
                                        value = 0
                                        )      
                # calculate Log returns    (simple method instead of issue with np.log and lack of time)                
                for i,q in enumerate(dfo[exchg][sym]["Quot"]):
                    if math.isnan(q) or (q == 0): dfo[exchg][sym]["Ret_Log"][i] = float("nan") 
                    else: dfo[exchg][sym]["Ret_Log"][i] = math.log(q) 
    return dfo


   
    
# call main read function for dict of dicts with all dataframes      
start_date = "2021-03-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "1h"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                            tf, start_date, end_date) 

# calculate log returns as input for autocorrelation
df_ohlcvs = calculate_returns(df_ohlcvs)

# parameters for autocorrelation plots
N_LAGS = 50
SIGNIFICANCE_LEVEL = 0.05


# function to create autocorrelation plots for squared returns and absolute returns for each symbol
def plot_autocorrelation_2(df, exchg, sym):
    fig, ax = plt.subplots(2, 1, figsize=(16, 8), tight_layout = True)
    # create acf plot for squared returns
    smt.graphics.plot_acf(df["Ret_Log"]**2, 
                          lags=N_LAGS, 
                          alpha=SIGNIFICANCE_LEVEL, 
                          use_vlines=True,
                          title="",
                          ax = ax[0])
    xmin, xmax, ymin, ymax = ax[0].axis()
    ax[0].axis([xmin, xmax, 0, 0.5]) 
    ax[0].set_title(sym + " - Squared Returns", fontsize = 32, loc="left")
    ax[0].set_xlabel("Lag in hours", fontsize = 28)
    ax[0].set_ylabel("ACF", fontsize = 28)
    ax[0].tick_params(axis='both', labelsize=28)

    # create acf plot for absolute returns
    smt.graphics.plot_acf(np.abs(df["Ret_Log"]), 
                          lags=N_LAGS, 
                          alpha=SIGNIFICANCE_LEVEL, 
                          use_vlines=True,
                          title="",
                          ax = ax[1])
    xmin, xmax, ymin, ymax = ax[1].axis()
    ax[1].axis([xmin, xmax, 0, 0.5]) 
    ax[1].set_title(sym + " - Absolute Returns", fontsize = 32, loc="left")
    ax[1].set_xlabel("Lag in hours", fontsize = 28)
    ax[1].set_ylabel("ACF", fontsize = 28)
    ax[1].tick_params(axis='both', labelsize=28)
    plt.show()
    
    
# create autocorrelation plots for all exchanges and all symbos in each exchange
df = df_ohlcvs
for exchg in df:
    for sym in df[exchg]:
        plot_autocorrelation_2(df[exchg][sym].dropna(how = "any"), exchg, sym)




    
