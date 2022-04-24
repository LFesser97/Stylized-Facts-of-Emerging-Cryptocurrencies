# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

# Function to create summary statistics of data downloaded from gateio

# for table 2.1

import numpy as np
import pandas as pd
from scipy import stats
import math

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited ot exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]



# calculate returns and Log returns for a symbol
def calculate_returns(dfo):
    # loop over all exchanges
    for i,exchg in enumerate(list(dfo.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(dfo[exchg][k]) > 0  for k in iter(dfo[exchg].keys())]):
            for j, sym in enumerate(list(dfo[exchg].keys())):
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
                for i,q in enumerate(dfo[exchg][sym]["Quot"]):
                    if math.isnan(q) or (q == 0): dfo[exchg][sym]["Ret_Log"][i] = float("nan") 
                    else: dfo[exchg][sym]["Ret_Log"][i] = math.log(q) 
    return dfo



# function to create summary statistics for all symbols of an exchange
def get_summary_statistics(exchg):
    sum_stat = pd.DataFrame(index = exchg.keys(), 
                            columns = ["Mean", "SD", "Min", "Max", "Skew", "Kurt", "Nobs"])
    for k in exchg.keys():
        my_data = pd.Series(exchg[k]["Ret_Log"])
        sum_stat.loc[k,"Mean"] = np.mean(my_data)
        sum_stat.loc[k,"SD"]   = np.std(my_data)
        sum_stat.loc[k,"Min"]  = np.min(my_data)
        sum_stat.loc[k,"Max"]  = np.max(my_data)
        sum_stat.loc[k,"Skew"] = stats.skew(my_data.dropna())
        sum_stat.loc[k,"Kurt"] = stats.kurtosis(my_data.dropna())
        sum_stat.loc[k,"Nobs"] = len(my_data)       
    return sum_stat

   
    
# call main read function for dict of dicts with all dataframes      
start_date = "2017-11-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "1h"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                            tf, start_date, end_date) 

# enhance dataframe with returns and Log returns 
df_ohlcvs = calculate_returns(df_ohlcvs)

# create summary statistics for all exchanges and print it
summary_statistics = {}
for exchg in df_ohlcvs.keys():
    summary_statistics[exchg] = get_summary_statistics(df_ohlcvs[exchg])
    print("Exchange: ", exchg, "\n", summary_statistics[exchg])
    

    

