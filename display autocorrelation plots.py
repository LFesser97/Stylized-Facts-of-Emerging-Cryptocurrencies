# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 22:38:29 2022

@author: Lukas
"""

# functions to create basic autocorrelation plots

# for figure 3.5

import statsmodels.tsa.api as smt
import matplotlib.pyplot as plt
import math

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited to exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]



# function to enhance original dataframe with Log returns
def calculate_log_returns(dfo):
    # loop over all exchanges
    for exchg in list(dfo.keys()):
        # check there is data for all currencies in the actual exchange
        if all([len(dfo[exchg][k]) > 0  for k in iter(dfo[exchg].keys())]):
            for j, sym in enumerate(list(dfo[exchg].keys())):
                # add new columns
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                       column = "Ret", 
                                       value = dfo[exchg][sym]["Close"] - dfo[exchg][sym]["Open"]
                                       )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Quot", 
                                        value = dfo[exchg][sym]["Close"] / dfo[exchg][sym]["Close"].shift(1)
                                        )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Ret_log", 
                                        value = 0
                                        )      
                # calculate Log returns    (simple method instead of issue with np.log and lack of time)
                for i,q in enumerate(dfo[exchg][sym]["Quot"]):
                    if math.isnan(q) or (q == 0): dfo[exchg][sym]["Ret_log"][i] = float("nan") 
                    else: dfo[exchg][sym]["Ret_log"][i] = math.log(q)                              
    return dfo
                         

# function to create autocorrelation plot for all symbols of an exchange
def plot_acf(exchg_dta, n_lags, signif_level, plt_wdth, plt_hght, exchg, start_date, end_date, tf):
    # exchg_dta : dataframes of currencies of actual exchange, 
    # grp_by: variable in df to use its unique values for x-ticks of boxplot
    # data_to_grp:  variable to investigate for seasonality
        
    # define number of plots in one row  -  all currencies of an exchange side by side     
    plot_cols = len(exchg_dta.keys())
    # definition of plot layout
    fig, axes = plt.subplots(ncols = plot_cols, nrows = 1, figsize = (plt_wdth, 8), tight_layout = True)
    # loop over currencies of actual exchange
    for j, sym in enumerate(list(exchg_dta.keys())):
        ax = axes[j]
        # create autocorrelation plot
        acf = smt.graphics.plot_acf(exchg_dta[sym]["Ret_log"].dropna(), 
                                    ax = ax, 
                                    lags = n_lags, 
                                    alpha = signif_level,
                                    title = sym)
        xmin, xmax, ymin, ymax = ax.axis()
        ax.axis([xmin, xmax, -0.05, 0.05]) 
        ax.set_xlabel("Lag in hours", size = 20)
        if j == 0: 
            ax.set_ylabel("ACF", size = 20)
        ax.title.set_size(24)
        ax.tick_params(axis='both', labelsize=20)
        ax.grid(visible=True, which='major', color='#303030',linewidth='0.5',  linestyle=':')

           
# function to create autocorrelation plots for all exchanges  (here:  only gateio)              
def plot_autocorrelations(dfo, n_lags, signif_level, plt_wdth, plt_hght, start_date, end_date, tf):
    # loop over all exchanges
    for i,exchg in enumerate(list(dfo.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(dfo[exchg][k]) > 0  for k in iter(dfo[exchg].keys())]):
            # call function to create autocorrelation plots for the actual exchange 
            plot_acf(dfo[exchg], n_lags, signif_level, plt_wdth, plt_hght, exchg, start_date, end_date, tf)
    

    
# call main read function for dict of dicts with all dataframes      
start_date = "2021-03-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "1h"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                            tf, start_date, end_date) 

# calculate log returns as input for autocorrelation
df_ohlcvs = calculate_log_returns(df_ohlcvs)



# main routine to calculate and display autocorrelation plots
# parameters: dict of dicts of dataframes, autocorrelation parameters, plot layout parameters
# print("\nProcessing and plotting a larger amount of data may take a few seconds, please be patient !")
n_lags = 24
if tf == "5m": n_lags *=12   #  adjust n_lags to check autocorrelation always for a complete day
plot_autocorrelations(df_ohlcvs, n_lags, 0.05, 24, 8, start_date, end_date, tf)




