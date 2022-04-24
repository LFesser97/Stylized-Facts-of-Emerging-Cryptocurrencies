# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

# various functions to calculate and plot volatility measures and correlations to investigate leverage effect

# for figures 3.4, 3.8, A5, A1-A4, A6-A10


import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from scipy import stats
from statsmodels.tsa.stattools import adfuller
import math

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited to exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]


# adjust the value of this variable if you want to calculate and plot correlations based on lag values other than 0
# here: lag = 1,2,3, and 7
my_lag = 0


# Augmented Dickey-Fuller test
def adf_test(x):
    indices = ['Test Statistic', 'p-value', '# of Lags Used', '# of Observations Used']
    adf_test = adfuller(x, autolag='AIC')
    results = pd.Series(adf_test[0:4], index=indices, dtype = float)
    for key, value in adf_test[4].items():
        results[f'Critical Value ({key})'] = value
    return results


# Rolling Correlation coefficient
def corr_coeff_rolling (I, R, t, m):
    c = np.zeros(len(I), dtype=float)
    p = np.zeros(len(I), dtype=float)
    if t < min(len(I), len(R)):    
        if len(I) == len(R):   
            for k in range(0,len(I)-m):
                if k >= t-1:
                    c[k], p[k]= stats.pearsonr(I[k-(t-1):k+1], R[k-(t-1):k+1])    
                else:
                    c[k], p[k] = (np.nan, np.nan)
        else:
            print("Series for I_vol and Ret have different length")
    else:
        print("Length parameter for rolling window longer than series")
    return c,p



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
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                       column = "Ret", 
                                       value = dfo[exchg][sym]["Close"] - dfo[exchg][sym]["Open"]
                                       )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                       column = "Ret_pct", 
                                       value = dfo[exchg][sym]["Ret"].pct_change()
                                       )  
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Quot", 
                                        value = dfo[exchg][sym]["Close"] / dfo[exchg][sym]["Close"].shift(1)
                                        )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                        column = "Ret_log", 
                                        value = 0
                                        )      
                for i,q in enumerate(dfo[exchg][sym]["Quot"]):
                    if math.isnan(q) or (q == 0): dfo[exchg][sym]["Ret_log"][i] = float("nan") 
                    else: dfo[exchg][sym]["Ret_log"][i] = math.log(q)                 
                # dfo[exchg][sym]["Ret_log"] = np.log(dfo[exchg][sym]["Ret_log"])    
                
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                       column = "Ret_sqrd", 
                                       value = np.square(dfo[exchg][sym]["Ret_log"])
                                       )   
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                       column = "Price_span", 
                                       value = dfo[exchg][sym]["High"] - dfo[exchg][sym]["Low"]
                                       )  
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                       column = "Price_avg", 
                                       value = (dfo[exchg][sym]["Close"] + dfo[exchg][sym]["Open"]) / 2
                                       )  
                dfo[exchg][sym].insert(loc = dfo[exchg][sym].columns.get_loc("Volume"), 
                                       column = "Var_coeff", 
                                       value = dfo[exchg][sym]["Price_span"] / dfo[exchg][sym]["Price_avg"]
                                       )  
                
                # create a temporary copy of the input dataframe of the actual exchange/symbo pair
                tmp = dfo[exchg][sym].copy(deep=True)       
                
                # create new dataframe with statistical data for the actual exchange/symbo pair
                df_st[exchg][sym] = {"data": pd.DataFrame({"Timestamp":[], "Date":[], 
                                                           "I_vol":[], "I_vol_pct": [], "I_vol_rel": [],
                                                           "Ret":[], "Ret_pct":[], "Quot":[], "Ret_log":[], 
                                                           "RV":[]}), 
                                     "corr": {"coeff": 0, "p-value": 0, "l1": 0, "l2": 0},
                                     "corr_series_30": {"c": [], "p": [], "l1": 0, "l2": 0},
                                     "corr_series_180": {"c": [], "p": [], "l1": 0, "l2": 0},
                                     "adf": pd.Series([], dtype = float),
                                     "adf_log": pd.Series([], dtype = float),
                                     "corr_RV": {"coeff": 0, "p-value": 0, "l1": 0, "l2": 0},
                                     "corr_series_RV_30": {"c": [], "p": [], "l1": 0, "l2": 0},
                                     "corr_series_RV_180": {"c": [], "p": [], "l1": 0, "l2": 0},
                                     "adf_RV": pd.Series([], dtype = float),
                                     "adf_log_RV": pd.Series([], dtype = float)}
                # fill new dataframe with grouped data
                # index column Date
                df_st[exchg][sym]["data"]["Date"] = tmp.groupby(["Date"])["Date"].nth(0)   
                # Datetime
                df_st[exchg][sym]["data"]["Timestamp"] = tmp.groupby(["Date"])["Timestamp"].nth(0)                   
                # Absolute Daily returns from first Open value and last Close value
                df_st[exchg][sym]["data"]["Ret"] = tmp.groupby(["Date"])["Close"].nth(-1) - tmp.groupby(["Date"])["Open"].nth(0)
                # Relative Daily Returns from absolute daily returns                                         
                df_st[exchg][sym]["data"]["Ret_pct"] = df_st[exchg][sym]["data"]["Ret"].pct_change() 
                # Log Daily Returns from absolute daily returns
                df_st[exchg][sym]["data"]["Quot"] = tmp.groupby(["Date"])["Close"].nth(-1)/tmp.groupby(["Date"])["Open"].nth(0)
                for i,q in enumerate(df_st[exchg][sym]["data"]["Quot"]):
                    if math.isnan(q) or (q == 0): df_st[exchg][sym]["data"]["Ret_log"][i] = float("nan") 
                    else: df_st[exchg][sym]["data"]["Ret_log"][i] = math.log(q)   
                # Intraday Squared returns: alternative measure for volatility, see also Annex A.2
                df_st[exchg][sym]["data"]["RV"]        = np.sqrt(tmp.groupby(["Date"])["Ret_sqrd"].agg([np.sum]))
                # Intraday Volatility: standard deviation of grouped return values
                df_st[exchg][sym]["data"]["I_vol"]     = tmp.groupby(["Date"])["Ret"].agg([np.std])
                # Intraday Volatility percentage: standard deviation of grouped percentage return values
                df_st[exchg][sym]["data"]["I_vol_pct"] = tmp.groupby(["Date"])["Ret_pct"].agg([np.std])
                ##### Intraday Volatility relative: standard deviation of grouped values of variation coefficients
                df_st[exchg][sym]["data"]["I_vol_rel"] = tmp.groupby(["Date"])["Var_coeff"].agg([np.std])
                
                # calculate correlation coefficient and related p-value and add to new dataframe
                if my_lag <= 0:
                    temp1 = df_st[exchg][sym]["data"]["I_vol"]
                    temp2 = df_st[exchg][sym]["data"]["Ret_log"]
                else:
                    temp1 = df_st[exchg][sym]["data"]["I_vol"][:-my_lag]
                    temp2 = df_st[exchg][sym]["data"]["Ret_log"].shift(-my_lag).dropna()
                c,p = stats.pearsonr(temp1, temp2)
                df_st[exchg][sym]["corr"]["coeff"] = c
                df_st[exchg][sym]["corr"]["p-value"] = p   
                df_st[exchg][sym]["corr"]["l1"] = len(temp1)   
                df_st[exchg][sym]["corr"]["l2"] = len(temp2)   
                                                      
                # calculate correlation coefficient and p-value in rolling window - 30 days
                df_st[exchg][sym]["corr_series_30"]["c"], df_st[exchg][sym]["corr_series_30"]["p"] = \
                    corr_coeff_rolling(temp1, temp2, 30, my_lag)
                df_st[exchg][sym]["corr_series_30"]["l1"] = len(temp1)
                df_st[exchg][sym]["corr_series_30"]["l2"] = len(temp2)
                # calculate correlation coefficient and p-value in rolling window - 180 days
                df_st[exchg][sym]["corr_series_180"]["c"], df_st[exchg][sym]["corr_series_180"]["p"] = \
                    corr_coeff_rolling(temp1, temp2, 180, my_lag)
                df_st[exchg][sym]["corr_series_180"]["l1"] = len(temp1)
                df_st[exchg][sym]["corr_series_180"]["l2"] = len(temp2)

                # run augmented Dickey-Fuller test
                df_st[exchg][sym]["adf"]     = adf_test(dfo[exchg][sym]["Close"])
                df_st[exchg][sym]["adf_log"] = adf_test(dfo[exchg][sym]["Ret_log"].dropna())
                
                # calculate correlation coefficient and related p-value of RV and add to new dataframe
                if my_lag <= 0:
                    temp1 = df_st[exchg][sym]["data"]["RV"]
                    temp2 = df_st[exchg][sym]["data"]["Ret_log"]
                else:
                    temp1 = df_st[exchg][sym]["data"]["RV"][:-my_lag]
                    temp2 = df_st[exchg][sym]["data"]["Ret_log"].shift(-my_lag).dropna()              
                c,p = stats.pearsonr(temp1, temp2)
                df_st[exchg][sym]["corr_RV"]["coeff"] = c
                df_st[exchg][sym]["corr_RV"]["p-value"] = p   
                df_st[exchg][sym]["corr_RV"]["l1"] = len(temp1)   
                df_st[exchg][sym]["corr_RV"]["l2"] = len(temp2)   
                                                      
                # calculate correlation coefficient and p-value in rolling window - 30 days
                df_st[exchg][sym]["corr_series_RV_30"]["c"], df_st[exchg][sym]["corr_series_RV_30"]["p"] = \
                    corr_coeff_rolling(temp1, temp2, 30, my_lag)
                df_st[exchg][sym]["corr_series_RV_30"]["l1"] = len(temp1)
                df_st[exchg][sym]["corr_series_RV_30"]["l2"] = len(temp2)
                # calculate correlation coefficient and p-value in rolling window - 180 days
                df_st[exchg][sym]["corr_series_RV_180"]["c"], df_st[exchg][sym]["corr_series_RV_180"]["p"] = \
                    corr_coeff_rolling(temp1, temp2, 180, my_lag)
                df_st[exchg][sym]["corr_series_RV_180"]["l1"] = len(temp1)
                df_st[exchg][sym]["corr_series_RV_180"]["l2"] = len(temp2)                
                                                   
            # if no data then add empty dataframe
            else:
                df_st[exchg][sym] = pd.DataFrame([])
    # return dict of dicts of dataframes with statistical data
    return dfo, df_st


# function to create scatterplots of Intraday volatility
def plot_leverage_effect(dta, plt_wdth, title_str, xlabel_str, ylabel_str, share_x, share_y):
    # dta : dataframes of currencies of actual exchange
    # other parameters:  to control plotting
    
    # define number of plots in one row  -  all currencies of an exchange side by side 
    plot_cols = len(dta.keys())
    # definition of plot layout
    fig, axes = plt.subplots(ncols = plot_cols, figsize = (plt_wdth, 6), 
                             sharex = share_x, sharey = share_y, tight_layout = True)
    # loop over currencies of actual exchange
    for j, sym in enumerate(list(dta.keys())):
        # select subplot
        ax = axes[j]
        # display scatterplot of percent change of Daily returns over Intraday volatility 
        # drop 1st row of dataframe as pct_change will be NA there
        ax.scatter(dta[sym]["data"].dropna(axis=0)["I_vol"], 
                   dta[sym]["data"].dropna(axis=0)["Ret_pct"], s=20, alpha=0.2)
        # add correlation coefficient and p-value to plot title 
        c = "%.3f" % dta[sym]["corr"]["coeff"]
        p = "%.8f" % dta[sym]["corr"]["p-value"]
        ax.set_title(title_str + sym + "\nCorr.-coeff.: " + c + "  p-value: " + p)
        ax.set_xlabel(xlabel_str)
        if j == 0: ax.set_ylabel(ylabel_str)
        # ax.set_yscale("log")
        
    
# function to create scatterplots of Intraday volatility for all exchanges (here: only gateio)                      
def scatterplot_exchanges(df_st, start_date, end_date, tf, share_x, share_y):
    # loop over all exchanges
    for i,exchg in enumerate(list(df_st.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(df_st[exchg][k]) > 0  for k in iter(df_st[exchg].keys())]):
            # call function to create scatterplots for the actual exchange
            plot_leverage_effect(df_st[exchg], 24, 
                                 "Leverage effect\n" + 
                                 start_date + " - " + end_date + "  /  " + tf + "\n" +
                                 "Exchange: " + exchg + " - Symbol: ", 
                                 "Intraday volatility", "Daily returns", share_x, share_y)



    
# call main read function for dict of dicts with all dataframes      
start_date = "2017-11-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "1h"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                              tf, start_date, end_date) 

# call main function to calculate Intraday volatility etc.
df_ohlcvs_stats_lev = {}
df_ohlcvs, df_ohlcvs_stats_lev = calculate_df_ohlcvs_stats_lev(df_ohlcvs)


# main routine to calculate and display scatterplots showing Leverage effect
# parameters: dict of dicts of dataframes, start and end date, timeframe 1h or 5min, same scale on x-axis resp. y-axis                            

# print("\nCalculating data and plotting may take a few seconds, please be patient !")
# scatterplot_exchanges(df_ohlcvs_stats_lev, start_date, end_date, tf, False, False)



# function to create Volatility plots over time
def plot_Intraday_Volatility_over_Time(curr_dta, plt_wdth, var_to_show,
                                       xlabel_str, ylabel_str, share_x, share_y):
    # dta : dataframes of currencies of actual exchange, 
    # plt_wdth: parameter for plot layout
    # title_str, xlabel_str, ylabel_str: parametrized plot labels
    # share_x, share_y: to control whether to use same scale on x-axis resp. y-axis
    
    # define number of plots to stack
    plot_rows = len(curr_dta.keys())
    # definition of plot layout
    fig, axes = plt.subplots(nrows = plot_rows, figsize = (plt_wdth, 16), 
                             sharex = share_x, sharey = share_y, tight_layout = True)
    # loop over currencies of actual exchange
    for j, sym in enumerate(list(curr_dta.keys())):
        print("Currency: ",sym)
        # select and setup subplot
        ax = axes[j]
        ax.set_title(sym, fontsize=24, loc="left")
        ax.set_xlabel(xlabel_str, fontsize=20)
        ax.set_ylabel(ylabel_str, fontsize=20)
        # convert data for x-axis
        p = "%Y-%m-%d %H:%M:%S%z"
        ts = pd.Series([dt.datetime.strptime(t,p)  for t in curr_dta[sym]["data"]["Timestamp"]])
        # set x-axis ticks
        ax.set_xticks([v  for k,v in enumerate(ts)  
                       if (ts.dt.month.iloc[k] % 6 == 1) & (ts.dt.day.iloc[k] == 1) ])
        ax.set_xticklabels([v.strftime("%Y-%m")  for k,v in enumerate(ts)  
                            if (ts.dt.month.iloc[k] % 6 == 1) & (ts.dt.day.iloc[k] == 1) ])
        ax.tick_params(axis='both', labelsize=20)

        # display Intraday volatility over time
        ax.plot(ts, curr_dta[sym]["data"][var_to_show])
        # ax.plot(ts, pd.Series(curr_dta[sym]["data"][var_to_show]).rolling(30).mean(), color = "red")
        ax.plot(ts, pd.Series(curr_dta[sym]["data"][var_to_show]).rolling(180).mean(), color = "red")
        ax.grid(visible=True, which='major', color='#303030',linewidth='0.5',  linestyle=':')
        
        

# function to create Intraday volatility and Realized volatility lots over time for all exchanges                
def I_vol_lineplots_exchanges(df, share_x, share_y):
    # loop over all exchanges
    for i,exchg in enumerate(list(df.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(df[exchg][k]) > 0  for k in iter(df[exchg].keys())]):
            # call function to create scatterplots for the actual exchange
            print("Exchange: ", exchg)
            plot_Intraday_Volatility_over_Time(df[exchg], 24, "I_vol",
                                 "Time", "Intraday\nvolatility Index", share_x, share_y)
            plot_Intraday_Volatility_over_Time(df[exchg], 24, "RV",
                                 "Time", "Realized\nvolatility", share_x, share_y)             


            
# main routine to calculate and display Volatilities over time showing Leverage effect
# parameters: dict of dicts of dataframes, same scale on x-axis resp. y-axis                            

print("\nProcessing and plotting a larger amount of data may take a few seconds, please be patient !")           
I_vol_lineplots_exchanges(df_ohlcvs_stats_lev, False, False)



# function to create Correlation plots over time
def plot_Correlation_over_Time(curr_dta, to_show, plt_wdth, xlabel_str, ylabel_str, share_x, share_y):
    # dta : dataframes of currencies of actual exchange, 
    # plt_wdth: parameter for plot layout
    # title_str, xlabel_str, ylabel_str: parametrized plot labels
    # share_x, share_y: to control whether to use same scale on x-axis resp. y-axis
    
    # define number of plots to stack
    plot_rows = len(curr_dta.keys())
    # definition of plot layout
    fig, axes = plt.subplots(nrows = plot_rows, figsize = (plt_wdth, 16), 
                             sharex = share_x, sharey = share_y, tight_layout = True)
    # loop over currencies of actual exchange
    for j, sym in enumerate(list(curr_dta.keys())):
        print("Currency: ",sym)
        # select and setup subplot
        ax = axes[j]
        ax.set_title(sym, fontsize=24, loc="left")
        ax.set_xlabel(xlabel_str, fontsize=20)
        ax.set_ylabel(ylabel_str, fontsize=20)
        # convert data for x-axis
        p = "%Y-%m-%d %H:%M:%S%z"
        ts = pd.Series([dt.datetime.strptime(t,p)  for t in curr_dta[sym]["data"]["Timestamp"]])
        st_idx = len(ts) - curr_dta[sym][to_show]["l1"]
        ts = ts[st_idx:]
        # set x-axis ticks
        ax.set_xticks([v  for k,v in enumerate(ts)  
                       if (ts.dt.month.iloc[k] % 6 == 1) & (ts.dt.day.iloc[k] == 1) ])
        ax.set_xticklabels([v.strftime("%Y-%m")  for k,v in enumerate(ts)  
                            if (ts.dt.month.iloc[k] % 6 == 1) & (ts.dt.day.iloc[k] == 1) ])
        ax.tick_params(axis='both', labelsize=20)

        # display Intraday volatility over time   (additional second axis to plot red shaded areas independently)
        ax2 = ax.twinx()
        ax.plot(ts, curr_dta[sym][to_show]["c"], color = "blue", linewidth = 1)
        ax.plot(ts, [0]*len(ts), color = "black", linewidth = 1.5)
        ax2.fill_between(ts, 0, 1, where=np.less_equal(curr_dta[sym][to_show]["p"], 0.05), color='red', alpha=.1)
        ax2.yaxis.set_major_locator(ticker.NullLocator())
        ax.grid(visible=True, which='major', color='#303030',linewidth='0.5',  linestyle=':')
        
        
                
# function to create Correlation plots over time for all exchanges 
# for a 30-day window (deactivated) and a 180-day window               
def correlation_lineplots_exchanges(df, share_x, share_y):
    # loop over all exchanges
    for i,exchg in enumerate(list(df.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(df[exchg][k]) > 0  for k in iter(df[exchg].keys())]):
            # call function to create scatterplots for the actual exchange
            print("Exchange: ", exchg)
            # plot_Correlation_over_Time(df[exchg], "corr_series_30", 24, 
            #                      "Time", "Correlation coefficient", share_x, share_y)
            plot_Correlation_over_Time(df[exchg], "corr_series_180", 24, 
                                 "Time", "Correlation\ncoefficient", share_x, share_y)
            # plot_Correlation_over_Time(df[exchg], "corr_series_RV_30", 24, 
            #                      "Time", "Correlation coefficient", share_x, share_y)            
            plot_Correlation_over_Time(df[exchg], "corr_series_RV_180", 24, 
                                 "Time", "Correlation\ncoefficient", share_x, share_y)      


             
# main routine to calculate and display Correlations over time showing Leverage effect
# parameters: dict of dicts of dataframes, same scale on x-axis resp. y-axis                            

print("\nProcessing and plotting a larger amount of data may take a few seconds, please be patient !")           
correlation_lineplots_exchanges(df_ohlcvs_stats_lev, False, False)


