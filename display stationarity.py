# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

# functions to calculate augmented Dickey-Fuller and KPSS test

# for table 2.2


import pandas as pd
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss
import math

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited to exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]



# Augmented Dickey-Fuller ("ADF") test
def adf_test(x):
    indices = ['Test Statistic', 'p-value', '# of Lags Used', '# of Observations Used']
    adf_test = adfuller(x, autolag='AIC')
    results = pd.Series(adf_test[0:4], index=indices, dtype = float)
    for key, value in adf_test[4].items():
        results[f'Critical Value ({key})'] = value
    return results


# Kwiatkowski-Phillips-Schmidt-Shin (“KPSS”) test
def kpss_test(timeseries):
    indices = ["Test Statistic", "p-value", "Lags Used"]
    kpsstest = kpss(timeseries, regression="c", nlags="auto")
    results = pd.Series(kpsstest[0:3], index=indices)
    for key, value in kpsstest[3].items():
        results[f'Critical Value ({key})'] = value
    return results



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
                
                # create new dataframe with statistical data for the actual exchange/symbo pair
                df_st[exchg][sym] = {"adf": pd.Series([], dtype = float),
                                     "adf_log": pd.Series([], dtype = float),
                                     "kpss": pd.Series([], dtype = float),
                                     "kpss_log": pd.Series([], dtype = float)
                                     }

                # run augmented Dickey-Fuller and KPSS test
                df_st[exchg][sym]["adf"]      = adf_test(dfo[exchg][sym]["Close"])
                df_st[exchg][sym]["adf_log"]  = adf_test(dfo[exchg][sym]["Ret_log"].dropna())
                df_st[exchg][sym]["kpss"]     = kpss_test(dfo[exchg][sym]["Close"])
                df_st[exchg][sym]["kpss_log"] = kpss_test(dfo[exchg][sym]["Ret_log"].dropna())
                                                   
            # if no data then add empty dataframe
            else:
                df_st[exchg][sym] = pd.DataFrame([])
    # return dict of dicts of dataframes with statistical data
    return dfo, df_st



    
# call main read function for dict of dicts with all dataframes      
start_date = "2021-03-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "1h"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                              tf, start_date, end_date) 

# call main function to calculate Intraday volatility etc.
df_ohlcvs_stats_lev = {}
df_ohlcvs, df_ohlcvs_stats_lev = calculate_df_ohlcvs_stats_lev(df_ohlcvs)

