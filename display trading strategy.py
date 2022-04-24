# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 09:55:43 2022

@author: Lukas
"""

# Functions to simulate trading strategy, calculate and plot profits, margins etc and save to csv files

import numpy as np
import pandas as pd
import datetime as dt
import os
import matplotlib.pyplot as plt

from ohlcv_from_csv import read_df_ohlcv_data_from_csv_files


# list of exchanges - this is currently limited ot exchange gateio from the ccxt package
cert_exchg_list = ["gateio"]

# list of currencies which are currently in scope
curr_list = ["BCH", "BTC", "ETH", "LTC", "XRP"]


# function to calculate trading results for one exchange and one symbol in there
# similar to function in summary statistics, so reduced comments  -  pls see in file summary statistics for more details
def df_for_trades(df, exchg, sym, tfk, tfv, mink, minv):
    print("  processing:  ", exchg, "  ", sym, "  ", tfk, "  ", mink)
    grp_col_header = "group_idx_" + str(tfk)
    base_epoch = df["EPOCH"][0]//1000
    
    tmp = df.copy(deep=True)    
    tmp.insert(loc = tmp.shape[1], 
               column = grp_col_header,
               # create new column with index data to be used for grouping
               value = pd.Series([(df["EPOCH"][k]//1000-base_epoch) // (60*tfv) for k in range(0,tmp.shape[0])])
               )
    
    df_st = {"data": pd.DataFrame({"Timestamp":[], "Ret":[], "Jumps":[], "Jumps_dt":[]}), 
             "move": {"jump_label": mink, "jump_thr": 0, "stddev": 0},
             "trades": pd.DataFrame({"Timestamp":[], "Ret":[], "Ret_dt":[]}),
             "trade_summary": {"Total profit":0, "Average margin":0}
             }
    df_st["data"]["Timestamp"] = tmp.groupby([grp_col_header])["Timestamp"].nth(0)                
    df_st["data"]["Ret"]       = tmp.groupby([grp_col_header])["Close"].nth(-1) / \
                                 tmp.groupby([grp_col_header])["Open"].nth(0) - 1
    df_st["data"]["Close"]     = tmp.groupby([grp_col_header])["Close"].nth(-1)
                                 
                                 
    # df_st["move"]["stddev"] = pd.Series(df["Close"]).agg(np.std) 
    df_st["move"]["stddev"]   = pd.Series(df_st["data"]["Ret"]).agg(np.std) 
         
    df_st["move"]["jump_thr"] = df_st["move"]["stddev"] * minv
    
    df_st["data"]["Jumps"]    = pd.Series(np.greater_equal(np.array(np.abs(df_st["data"]["Ret"])), 
                                                            df_st["move"]["jump_thr"]))
    df_st["data"]["Jumps_dt"] = df_st["data"]["Jumps"].shift(periods=1, fill_value=False)
    
    df_st["trades"]["Timestamp"] = pd.Series([df_st["data"]["Timestamp"][k] 
                                             for k in range(0,df_st["data"].shape[0])
                                             if df_st["data"]["Jumps"][k] == True])
    
    df_st["trades"]["Ret"]    = pd.Series([df_st["data"]["Ret"][k] 
                                            for k in range(0,df_st["data"].shape[0])
                                            if df_st["data"]["Jumps"][k] == True],
                                         dtype = np.float64)
    df_st["trades"]["Ret_dt"] = pd.Series([df_st["data"]["Ret"][k]
                                            for k in range(1,df_st["data"].shape[0])
                                            if df_st["data"]["Jumps_dt"][k] == True],
                                         dtype = np.float64)
    
    # initialize empty lists for trading details
    buy = []
    sell = []
    profit = []
    margin = []
    last_idx = len(df_st["data"]["Jumps"]) - 1
    # for all jumps that trigger a trade activity
    for k in range(0,last_idx):
        if df_st["data"]["Jumps"][k] == True:
            # Go long
            if df_st["data"]["Ret"][k] < 0:     
                buy.append(df_st["data"]["Close"][k])
                if k < last_idx: 
                    sell.append(df_st["data"]["Close"][k+1])
                    profit.append(df_st["data"]["Close"][k+1] - df_st["data"]["Close"][k])
                    margin.append(df_st["data"]["Close"][k+1] / df_st["data"]["Close"][k] - 1)
                else: 
                    sell.append(np.nan)
                    profit.append(np.nan)
                    margin.append(np.nan)
            # Go short
            else:   
                buy.append(df_st["data"]["Close"][k+1])
                if k < last_idx: 
                    sell.append(df_st["data"]["Close"][k])
                    profit.append(df_st["data"]["Close"][k] - df_st["data"]["Close"][k+1])
                    margin.append(df_st["data"]["Close"][k] / df_st["data"]["Close"][k+1] - 1)
                else: 
                    sell.append(np.nan)
                    profit.append(np.nan)
                    margin.append(np.nan)
    # calculate cumulated profit and margin
    profit_cum = list(pd.Series(profit, dtype = np.float64).cumsum())
    margin_cum = list(pd.Series(margin, dtype = np.float64).cumsum())
    # save all data in new dataframe
    df_st["trades"] = pd.concat([df_st["trades"], 
                                 pd.DataFrame({"Buy":buy, "Sell":sell, 
                                               "Profit":profit, "Profit cum.": profit_cum, 
                                               "Margin":margin, "Margin cum.": margin_cum,
                                               },
                                              dtype = np.float64)
                                 ],
                                axis = 1)
    # ... include trade summary statistics
    df_st["trade_summary"]["total profit"] = df_st["trades"]["Profit"].sum()
    df_st["trade_summary"]["average margin"] = df_st["trades"]["Margin"].mean()
    # ... and return it
    return df_st



# function to calculate trading results for all exchanges and all symbols in there
def calculate_df_ohlcvs_stats_trading(dfo, tf):
    timeframes = dict(zip(["5m", "15m", "30m", "1h", "2h", "4h"], 
                          [5, 15, 30, 60, 120, 240]))
    min_dict   = dict(zip(["0","1 sigma", "2 sigma", "3 sigma", "4 sigma", "5 sigma", "6 sigma"],
                          [0, 1, 2, 3, 4, 5, 6]))
    if tf == "1h":   # limit to values >= 1h
        timeframes = timeframes[3:]
        min_dict = min_dict[3:]
    # create new empty dictionary
    dfmr = {}
    # for each exchange
    for exchg in dfo:
        dfmr[exchg] = {}
        # for each symbol
        for sym in dfo[exchg]:
            # create an empty dictionary
            dfmr[exchg][sym] = {"data":{}, 
                                "summary_df":{"Total profit": pd.DataFrame([], 
                                                                           columns = min_dict.keys(), 
                                                                           index = timeframes.keys()),
                                              "Average margin": pd.DataFrame([], 
                                                                             columns = min_dict.keys(), 
                                                                             index = timeframes.keys())}
                                }
            if len(dfo[exchg][sym]) > 0:
                # for all timeframes
                for tf in timeframes:
                    dfmr[exchg][sym]["data"][tf] = {}
                    # for all jump thresholds
                    for min in min_dict:
                        # calculate trading results ad save in dictionary
                        dfmr[exchg][sym]["data"][tf][min] = df_for_trades(dfo[exchg][sym], exchg, sym, 
                                                                    tf, timeframes.get(tf),
                                                                    min, min_dict.get(min))
                        dfmr[exchg][sym]["summary_df"]["Total profit"].loc[tf,min] = \
                            dfmr[exchg][sym]["data"][tf][min]["trade_summary"]["total profit"]
                        dfmr[exchg][sym]["summary_df"]["Average margin"].loc[tf,min] = \
                            dfmr[exchg][sym]["data"][tf][min]["trade_summary"]["average margin"]
                # save results in csv file            
                csv_dir = "df_ohlcvs trading margin - csv"
                # determine full path to csv file for margin
                fn = "df_ohlcv  " + exchg + "  " + sym + "  Average Margin.csv"
                full_fn = os.path.join(os.getcwd(), csv_dir, fn)
                full_csv_dir = os.path.join(os.getcwd(), csv_dir)                
                os.makedirs(full_csv_dir, exist_ok = True)
                dfmr[exchg][sym]["summary_df"]["Average margin"].to_csv(full_fn, sep = ";", decimal = ",")
                # determine full path to csv file for profit
                fn = "df_ohlcv  " + exchg + "  " + sym + "  Total profit.csv"
                full_fn = os.path.join(os.getcwd(), csv_dir, fn)
                full_csv_dir = os.path.join(os.getcwd(), csv_dir)                
                os.makedirs(full_csv_dir, exist_ok = True)
                dfmr[exchg][sym]["summary_df"]["Total profit"].to_csv(full_fn, sep = ";", decimal = ",")
                # confirm files were saved
                print(exchg, " ", sym, " - saved to csv")

    return dfmr


    
# call main read function for dict of dicts with all dataframes      
start_date = "2021-04-01"   # first date - included
end_date =   "2022-03-01"   # last date  - excluded  
tf = "5m"       
df_ohlcvs = read_df_ohlcv_data_from_csv_files(cert_exchg_list, curr_list, "df_ohlcvs - csv", 
                                            tf, start_date, end_date) 

# calculate trading results for all exchanges and all symbols in there
print("\nProcessing mean reversion calculation")
df_ohlcvs_stats_trading = calculate_df_ohlcvs_stats_trading(df_ohlcvs, tf)



# function to create lineplots of margin for all symbols in an exchanges
def plot_Trading_over_Time(curr_dta, plt_wdth,
                                       title_str, xlabel_str, ylabel_str, share_x, share_y):
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
        ax.set_title(title_str + sym)
        ax.set_xlabel(xlabel_str)
        ax.set_ylabel(ylabel_str)
        # convert data for x-axis
        p = "%Y-%m-%d %H:%M:%S%z"
        ts = pd.Series([dt.datetime.strptime(t,p)  
                        for t in curr_dta[sym]["data"]["2h"]["2 sigma"]["trades"]["Timestamp"]])
        # set x-axis ticks
        ax.set_xticks([v  for k,v in enumerate(ts)  
                       if (ts.dt.month.iloc[k] % 3 == 1) & (ts.dt.day.iloc[k] == 1) ])
        ax.set_xticklabels([v.strftime("%Y-%m-%d")  for k,v in enumerate(ts)  
                            if (ts.dt.month.iloc[k] % 3 == 1) & (ts.dt.day.iloc[k] == 1) ])
        tmp = curr_dta[sym]["data"]["2h"]["2 sigma"]["trades"]
        ax.plot(ts, tmp["Margin cum."])
        ax.grid(b=True, which='major', color='#303030',linewidth='0.5',  linestyle=':')
  
        
        
# function to create lineplots of margin for all exchanges 
def Trading_lineplots_exchanges(df, share_x, share_y):
    # loop over all exchanges
    for i,exchg in enumerate(list(df.keys())):
        # check there is data for all currencies in the actual exchange
        if all([len(df[exchg][k]) > 0  for k in iter(df[exchg].keys())]):
            # call function to create scatterplots for the actual exchange
            print("Exchange: ", exchg)
            plot_Trading_over_Time(df[exchg], 24,
                                 "Simple Trading Strategy - Average Margin\n" + 
                                 start_date + " - " + end_date + "\n" +
                                 "Exchange: " + exchg + " - Symbol: ", 
                                 "Time", "Average Margin", share_x, share_y)


# plot all trading results over time
print("\nProcessing and plotting a larger amount of data may take a few seconds, please be patient !")           
Trading_lineplots_exchanges(df_ohlcvs_stats_trading, False, False)


