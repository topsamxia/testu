import pandas as pd
import tushare as ts
import os
import time

today_date = time.strftime("%Y%m%d")

# create today's data folder
cwd = os.getcwd()
target_dir = cwd+"\\"+today_date
# os.makedirs(name=target_dir)

# obtain stock list
stock_basics = ts.get_stock_basics()
code_series = stock_basics.index

# create folder
target_dir = cwd+"\\"+today_date
try:
    os.makedirs(name=target_dir)
except:
    print ('folder already existed')

#obtain the stock list, and save it to files
stock_day_df = pd.DataFrame
stock_5min_df = pd.DataFrame

index=0
size=code_series.size
for code in code_series:
    # export progress in console
    print ("%s out of %s\nObtaining code %s"%(index, size, code))
    # obtain daily history data
    stock_day_df = ts.get_k_data(code)
    csv_filename = target_dir+"\\"+"day_"+code+".csv"
    stock_day_df.to_csv(csv_filename)
    # obtain 5min history data
    stock_5min_df = ts.get_k_data(code, ktype='5')
    csv_filename = target_dir + "\\" + "5min_" + code + ".csv"
    stock_5min_df.to_csv(csv_filename)
    # export console
    print (stock_day_df.tail(3))
    print (stock_5min_df.tail(3))

    print ("\n")
    index=index+1
    time.sleep(0.1)

# target_dir = cwd+"\\"+today_date
# os.makedirs(name=target_dir)
#
# stock_df = pd.DataFrame()
# for stock_df in sk_5m_collection:
#     print (stock_df)
#
# csv_filename = target_dir+"\\"+"5min_"+stock_df.iloc[1]["code"]+".csv"
# stock_df.to_csv(csv_filename)
