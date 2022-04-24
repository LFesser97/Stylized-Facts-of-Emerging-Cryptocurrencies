# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

import pandas as pd
import os

# ----------------------------------------------------------------------------
# -----  functions to read csv data from files in defined sub-directory  -----
# ----------------------------------------------------------------------------


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