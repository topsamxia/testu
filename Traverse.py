#coding=utf-8
import pandas as pd
import numpy as np
import tushare as ts
import os, time, datetime

# pd_hist = pd.read_csv('hist_sample/SZ300191.csv', names=['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'total'])
#
# hist_date_parse = '%Y/%m/%d'
# hist_time_parse = '%H:%M'
# date1 = datetime.datetime.strptime(pd_hist.iloc[1].date + pd_hist.iloc[1].time, hist_date_parse+hist_time_parse)
# print (date1)


# def get_delta_date_str (date_today_str, delta = -1, parse_str = "%Y/%m/%d"):
#     specific_date = datetime.datetime.strptime(date_today_str, parse_str)+datetime.timedelta(delta)
#     return specific_date.strftime('%Y/%m/%d')
#
# print (get_delta_date_str('2017/08/08'))

# load the csv file, and reindex with date and time
def traverse (file_name = 'hist_sample/SZ300191.csv'):

    hist_names = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'total']
    pd_hist = pd.read_csv(file_name, names=hist_names)
    pd_hist.set_index(["date", "time"], inplace=True)

    # print (pd_hist.index[0][0])   -> 2017/01/03
    # print (pd_hist.iloc[0].name)  -> ('2017/01/03', '09:35')

    # get all unique date indices and put them into a list
    date_str_list = []
    [date_str_list.append(date_str) for date_str in pd_hist.index.get_level_values('date').unique()]

    # print (date_str_list)

    date_index = pd_hist.index.get_level_values('date').unique()

    # print (np.where(date_index == '2017/01/05')[0][0])
    # print (date_index.size)

    # Zhang Ting
    def zhang_ting(close_old, close_new):
        return float(close_new)/float(close_old) >=1.098

    # Die Ting
    def die_ting(close_old, close_new):
        return float(close_new)/float(close_old) <=0.902


    # initialize the statistics, format is {date:[open, close, high, low, volume, total, zt, dt, occupied]}
    #                                               0   1       2       3   4       5     6   7    8
    statistics = {}
    transaction = {}
    date_since_buy = 0
    last_buy_price = 0
    # loop all days
    for date_current in date_str_list:

        date_current_index = date_str_list.index(date_current)

        pd_current = pd_hist.loc[date_current]

        open = pd_current.iloc[0].open
        close = pd_current.iloc[-1].close
        low = pd_current.iloc[0].low
        high = pd_current.iloc[0].high
        volume = 0
        total = 0

        zt = False
        dt = False

        occupied = False if date_current_index == 0 else statistics[date_str_list[date_current_index-1]][8]

        for time in pd_current.index:
            current_row = pd_current.loc[time]
            if current_row.high > high:
                high = current_row.high
            if current_row.low < low:
                low = current_row.low
            volume += current_row.volume
            total += current_row.total

            # TBD - to extract this logic to an abstract strategy class
            if date_current_index >= 2:
                yesterday = statistics[date_str_list[date_current_index-1]]
                day_before_yes = statistics[date_str_list[date_current_index-2]]
                if day_before_yes[6] and (yesterday[1]/day_before_yes[1] > 1.03) and ((close/yesterday[1] < 0.95) and (high < yesterday[1])):
                    if not occupied:
                        # buy = True
                        occupied = True
                        last_buy_price = current_row.close
                        transaction[date_current] = current_row.close

            if date_current_index >= 3:
                yesterday = statistics[date_str_list[date_current_index - 1]]
                day_before_yes = statistics[date_str_list[date_current_index - 2]]
                two_day_before_yes = statistics[date_str_list[date_current_index - 3]]
                if two_day_before_yes[6] and yesterday[1] / two_day_before_yes[1] < 0.92 and ((close/yesterday[1] < 0.97) and (high < 1.02 * yesterday[1])):
                    if not occupied:
                        # buy = True
                        occupied = True
                        last_buy_price = current_row.close
                        transaction[date_current] = -(current_row.close)

            if occupied:
                if current_row.open / last_buy_price > 1.08 or (date_since_buy >=5 and current_row.close > statistics[date_str_list[date_current_index - 1]][1]):
                    occupied = False
                    transaction[date_current] = current_row.close
                    date_since_buy = 0

            if occupied:
                date_since_buy += 1





        date_int_index = date_str_list.index(date_current)
        if date_int_index > 0:
            zt = zhang_ting(statistics[date_str_list[date_int_index - 1]][1], close)
            dt = die_ting(statistics[date_str_list[date_int_index - 1]][1], close)

        #                             0
        statistics[date_current] = [open, close, high, low, volume, total, zt, dt, occupied]

    print (statistics)
    print (transaction)

dir= os.path.abspath("D:\stock\Stk_5F_201708\Stk_5F_2017")
files = os.listdir(dir)

for file in files:
    traverse(os.path.join(dir, file))

