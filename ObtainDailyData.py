import pandas as pd
import tushare as ts
import os
import time
import threadpool




# create folder
cwd = os.getcwd()
today_date = time.strftime("%Y%m%d")
target_dir = cwd+os.path.sep+today_date
try:
    os.makedirs(name=target_dir)
except:
    print ('folder already existed')

# clean up the index with numbers, and make date the default index
def pre_process(df):
    df.set_index('date', inplace=True)
    # df.drop(df.columns[0], axis=1, inplace=True)
    return df

def obtain_daily_data(sk_code, target_dir = ""):
    print ("Processing: "+sk_code)

    if target_dir!="":
        target_dir=target_dir+os.path.sep

    day_df = pre_process(ts.get_k_data(sk_code))
    csv_filename = target_dir + "day_" + sk_code + ".csv"
    day_df.to_csv(csv_filename)

    five_min_df = pre_process(ts.get_k_data(sk_code, ktype='5'))
    csv_filename = target_dir + "5min_" + sk_code + ".csv"
    five_min_df.to_csv(csv_filename)

    print (day_df.head(1))
    print (five_min_df.head(1))

# obtain stock list
stock_basics = ts.get_stock_basics()
code_series = stock_basics.index

args_list=[]
for code in code_series:
    args_list.append(([code, target_dir], None))

print (args_list)

pool = threadpool.ThreadPool(50)
requests = threadpool.makeRequests(obtain_daily_data, args_list)
[pool.putRequest(req) for req in requests]
pool.wait()

#
#
# index=0
# size=code_series.size
# for code in code_series:
#     # export progress in console
#     print ("%s out of %s\nObtaining code %s"%(index, size, code))
#     thread.start_new_thread(obtain_daily_data, (code, target_dir))
#     # time.sleep (0.1)
#
#     # # obtain daily history data
#     # stock_day_df = ts.get_k_data(code)
#     # csv_filename = target_dir+"\\"+"day_"+code+".csv"
#     # stock_day_df.to_csv(csv_filename)
#     # # obtain 5min history data
#     # stock_5min_df = ts.get_k_data(code, ktype='5')
#     # csv_filename = target_dir + "\\" + "5min_" + code + ".csv"
#     # stock_5min_df.to_csv(csv_filename)
#     # # export console
#     # print (stock_day_df.tail(3))
#     # print (stock_5min_df.tail(3))
#
#     print ("\n")
#     index=index+1
#     # time.sleep(0.1)
