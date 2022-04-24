# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

# Functions to create data for investigation of mean reversion

# for tables 4.1 - 4.6  


import numpy as np
import pandas as pd
from scipy import stats

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited ot exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]


# create new dataframe with detailed data for investigation of mean reversion for a given pair of timeframe/jump level
def df_for_mean_rev(df, exchg, sym, tfk, tfv, mink, minv):
    # df: data for one symbol of one exchange
    # exchg: name of exchange
    # sym: name of symbol
    # tfk: key of timeframe   tfv: value of timeframe
    # mink: key of jump threshold   minv: value of jump threshold
    print("  processing:  ", exchg, "  ", sym, "  ", tfk, "  ", mink)
    grp_col_header = "group_idx_" + str(tfk)
    base_epoch = df["EPOCH"][0]//1000
    # create temporary copy of original dataframe with exra columns for grouping 
    tmp = df.copy(deep=True)    
    tmp.insert(loc = tmp.shape[1], 
               column = grp_col_header,
               # create new column with index data to be used for grouping
               value = pd.Series([(df["EPOCH"][k]//1000-base_epoch) // (60*tfv) for k in range(0,tmp.shape[0])])
               )
    # create new dataframe with statistics
    df_st = {"data": pd.DataFrame({"Timestamp":[], "Ret":[], "Jumps":[], "Jumps_dt":[]}), 
             "move": {"jump_label": mink, "jump_thr": 0, "stddev": 0},
             "jumps": pd.DataFrame({"Ret":[], "Ret_dt":[]}),
             "stat": {"corr_coef": 0, "p_value": 0}
             }
    # calculate timestap of grouped data
    df_st["data"]["Timestamp"] = tmp.groupby([grp_col_header])["Timestamp"].nth(0)                
    # calculate returns of grouped data
    df_st["data"]["Ret"]       = tmp.groupby([grp_col_header])["Close"].nth(-1) / \
                                 tmp.groupby([grp_col_header])["Open"].nth(0) - 1                                 
    # STDDEV of returns
    df_st["move"]["stddev"]   = pd.Series(df_st["data"]["Ret"]).agg(np.std) 
    # jump threshold level
    df_st["move"]["jump_thr"] = df_st["move"]["stddev"] * minv
    # find all jumps that are above threshold
    df_st["data"]["Jumps"]    = pd.Series(np.greater_equal(np.array(np.abs(df_st["data"]["Ret"])),df_st["move"]["jump_thr"]))
    # shift by one to correlate
    df_st["data"]["Jumps_dt"] = df_st["data"]["Jumps"].shift(periods=1, fill_value=False)
    # find returns at jump points
    df_st["jumps"]["Ret"]    = pd.Series([df_st["data"]["Ret"][k] 
                                            for k in range(0,df_st["data"].shape[0])
                                            if df_st["data"]["Jumps"][k] == True],
                                         dtype = np.float64)
    # find returns at jump points shifted by 1
    df_st["jumps"]["Ret_dt"] = pd.Series([df_st["data"]["Ret"][k]
                                            for k in range(1,df_st["data"].shape[0])
                                            if df_st["data"]["Jumps_dt"][k] == True],
                                         dtype = np.float64)
    # calculate correlation if there are enough jumps resp. return data points
    if (len(df_st["jumps"].dropna(axis=0)["Ret_dt"]) >= 2 and 
        len(df_st["jumps"].dropna(axis=0)["Ret"]) >= 2):
        c,p = stats.pearsonr(df_st["jumps"].dropna(axis=0)["Ret_dt"], 
                             df_st["jumps"].dropna(axis=0)["Ret"])
    else:
        c,p = (0,0)
    # save correlation and related p-value 
    df_st["stat"]["corr_coef"] = c
    df_st["stat"]["p_value"] = p
    return df_st


# create mean reversion for all combinations of timeframes and signam levels resp. jump thresholds
def calculate_df_ohlcvs_stats_mean_rev(dfo, tf):
    timeframes = dict(zip(["5m", "15m", "30m", "1h", "2h", "4h"], 
                          [5, 15, 30, 60, 120, 240]))
    min_dict   = dict(zip(["0","1 sigma", "2 sigma", "3 sigma", "4 sigma", "5 sigma", "6 sigma"],
                          [0, 1, 2, 3, 4, 5, 6]))
    if tf == "1h":   # limit to values >= 1h
        timeframes = timeframes[3:]
        min_dict = min_dict[3:]
    dfmr = {}
    # for all exchanges
    for exchg in dfo:
        dfmr[exchg] = {}
        # for all symbols
        for sym in dfo[exchg]:
            dfmr[exchg][sym] = {}
            if len(dfo[exchg][sym]) > 0:
                # create empty summary tables
                dfmr[exchg][sym]["summary"] = {}
                dfmr[exchg][sym]["summary"]["corr_coef"] = pd.DataFrame(np.zeros((len(timeframes),len(min_dict)), dtype = float), 
                                                                        index = timeframes.keys(),
                                                                        columns = min_dict.keys()
                                                                        )
                dfmr[exchg][sym]["summary"]["p_value"] =   pd.DataFrame(np.zeros((len(timeframes),len(min_dict)), dtype = float), 
                                                                        index = timeframes.keys(),
                                                                        columns = min_dict.keys()
                                                                        )
                dfmr[exchg][sym]["summary"]["nr_jumps"] =  pd.DataFrame(np.zeros((len(timeframes),len(min_dict)), dtype = float), 
                                                                        index = timeframes.keys(),
                                                                        columns = min_dict.keys()
                                                                        )
                # ...and fill them !
                for tf in timeframes:
                    dfmr[exchg][sym][tf] = {}
                    for min in min_dict:
                        dfmr[exchg][sym][tf][min] = df_for_mean_rev(dfo[exchg][sym], exchg, sym, 
                                                                    tf, timeframes.get(tf),
                                                                    min, min_dict.get(min))
                        dfmr[exchg][sym]["summary"]["corr_coef"].loc[tf,min] = dfmr[exchg][sym][tf][min]["stat"]["corr_coef"]
                        dfmr[exchg][sym]["summary"]["p_value"].loc[tf,min] =   dfmr[exchg][sym][tf][min]["stat"]["p_value"]                   
                        dfmr[exchg][sym]["summary"]["nr_jumps"].loc[tf,min] =  dfmr[exchg][sym][tf][min]["jumps"].shape[0]                                           
    return dfmr



    
# call main read function for dict of dicts with all dataframes      
start_date = "2021-04-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "5m"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                            tf, start_date, end_date) 

# calculate mean reversion statistics for all exchanges and symbols in there
print("\nProcessing mean reversion calculation")
df_ohlcvs_stats_mean_rev = calculate_df_ohlcvs_stats_mean_rev(df_ohlcvs, tf)

