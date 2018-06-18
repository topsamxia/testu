import pandas as pd
import tushare as ts
import os
import time, datetime
import threadpool
import StkDataKeeper as sk
import StkScheduler as ssch


def get_daily_data():
    # create folder
    cwd = os.getcwd()
    today_date = time.strftime("%Y%m%d")
    target_dir = today_date
    # target_dir = os.path.join("/home/samx/ramdisk/", today_date)
    try:
        os.makedirs(name=target_dir)
    except:
        print('folder already existed')

    # clean up the index with numbers, and make date the default index
    def pre_process(df):
        df.set_index('date', inplace=True)
        # df.drop(df.columns[0], axis=1, inplace=True)
        return df

    def obtain_daily_data(sk_code, target_dir=""):
        print("Processing: " + sk_code)

        if target_dir != "":
            target_dir = target_dir + os.path.sep

        day_df = pre_process(ts.get_k_data(sk_code))
        csv_filename = target_dir + "day_" + sk_code + ".csv"
        day_df.to_csv(csv_filename)

        print(day_df.head(1))

        five_min_df = pre_process(ts.get_k_data(sk_code, ktype='5'))
        csv_filename = target_dir + "5min_" + sk_code + ".csv"
        five_min_df.to_csv(csv_filename)

        print(five_min_df.head(1))

    # obtain stock list
    try:
        stock_basics = ts.get_stock_basics()
        code_series = stock_basics.index
    except:
        keeper = sk.StkDataKeeper()
        keeper.load_stock_list("stock_basics.csv")
        code_series = keeper.stock_list

    # prepare arguments
    args_list = []
    for code in code_series:
        args_list.append(([code, target_dir], None))

    # collect
    pool = threadpool.ThreadPool(30)
    requests = threadpool.makeRequests(obtain_daily_data, args_list)
    [pool.putRequest(req) for req in requests]
    pool.wait()

    print('done')

def get_daily_chart(analysis_type=-1):
    # determine 上涨
    def isShangzhang(s_df, index=-1, increase=0.02):
        if len(s_df.index) >= abs(index) + 1:
            return ((s_df.iloc[index].close / s_df.iloc[index - 1].close - 1) > increase)
        else:
            return False

    # determine 涨停
    def isZhangting(s_df, index=-1):
        return isShangzhang(s_df, index, 0.098)
        # # not enough data
        # if len(stock_df.index) < (abs(index) + 1):
        #     return False
        #
        # # return if shangzhang > 9.8%
        # if (s_df.iloc[index].close / s_df.iloc[index - 1].close) >= 1.098:
        #     return True
        # else:
        #     return False

    # determine 上涨
    def isXiadie(s_df, index=-1, increase=-0.02):
        if len(s_df.index) >= abs(index) + 1:
            return ((s_df.iloc[index].close / s_df.iloc[index - 1].close - 1) < increase)
        else:
            return False

    def isDieting(s_df, index=-1):
        return isXiadie(s_df, index, -0.098)


    # determine 条件四
    # 第一天涨停， 当中下跌2-3天，第四天涨
    def isQualifiedCondition4(s_df, index=-1):
        if len(stock_df.index) < (abs(index) + 3):
            return False
        if isZhangting(s_df, index-3) and isZhangting(s_df, index):
            # if isXiadie(s_df, index-2, -0.02) and isXiadie(s_df, index-1, -0.02):
            if (not isZhangting(s_df, index-2) and not isZhangting(s_df, index-1)):
                return True

        if len(stock_df.index) < (abs(index) + 4):
            return False
        if isZhangting(s_df, index-4) and isZhangting(s_df, index) and not isZhangting(s_df, index-3) and not isZhangting(s_df, index-2) and not isZhangting(s_df, index-1):
            # if isXiadie(s_df, index-3, -0.02) and isXiadie(s_df, index-2, -0.02) and isXiadie(s_df, index-1, -0.02) \
            #     and ((s_df.iloc[index-1].close / s_df.iloc[index-4].close - 1) < -0.09):
            if ((s_df.iloc[index-1].close / s_df.iloc[index-4].close - 1) < -0.05):
                return True

        return False

    # determine 条件一
    def isQualifiedCondition1(s_df, index=-1):
        if len(stock_df.index) >= (abs(index) + 2):
            if (s_df.iloc[index].close * s_df.iloc[index].volume > 200000):
                return (isZhangting(s_df, index - 1) and isShangzhang(s_df, index))
        return False

    # determine 条件二
    def isQualifiedCondition2(s_df, index=-1):
        if len(stock_df.index) >= (abs(index) + 3):
            if (s_df.iloc[index].close / s_df.iloc[index - 2].close < 0.92) \
                    and (s_df.iloc[index].close < s_df.iloc[index - 1].close) \
                    and (s_df.iloc[index - 1].close < s_df.iloc[index - 2].close) \
                    and isZhangting(s_df, index - 2):
                return True
        return False

    # determine 条件三
    def isQualifiedCondition3(s_df, index=-1):
        if len(stock_df.index) >= (abs(index) + 3):
            if isQualifiedCondition1(s_df, index - 1):
                if (s_df.iloc[index].close / s_df.iloc[index - 1].close < 0.95):
                    return True
            return False

    def completeCode(code_str):
        if len(code_str) < 6:
            return "0" * (6 - len(code_str)) + code_str
        else:
            return code_str

    print(time.strftime("%M:%S"))

    ########################################
    datafolder_dir = os.getcwd() + os.sep +"current"
    ########################################

    file_list = os.listdir(datafolder_dir)

    trading = -2
    post_analysis = -1

    analysis_type = post_analysis

    file_index = 0
    select_list = []
    file_dict = {}
    for file in file_list:
        # print ("Working on file %s: %s"%(file_index, file))
        file_index = file_index + 1
        if ("day" in file):
            stock_df = pd.read_csv(datafolder_dir + "/" + file)
            if isQualifiedCondition1(stock_df, analysis_type):
                select_list.append("condition 1:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["1:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition2(stock_df, analysis_type):
                select_list.append("condition 2:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["2:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition3(stock_df, analysis_type):
                select_list.append("condition 3:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["3:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition4(stock_df, analysis_type):
                select_list.append("condition 4:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["4:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file

    select_list.sort()

    # ------------------------------
    for file in select_list:
        print(file)

    for index in range(len(select_list)):
        select_list[index] = select_list[index][-8:]

    print(select_list)

    stock_df_list = []
    for item in select_list:
        stock_df_list.append(pd.read_csv(file_dict[item]))

    # ------------------------------

    painter = sk.StkDiagram()
    keeper = sk.StkDataKeeper()
    try:
        keeper.get_stock_basics_tu("stock_basics.csv")
    except:
        keeper.load_stock_list("stock_basics.csv")

    keeper.load_stock_list("stock_basics.csv")
    klines = painter.paint_klines(select_list, "stock_basics.csv")

    klines.width = 1600
    klines.render("lyd_result/render.html")
    klines.width = 990

    # get data from yesterday
    analysis_type = trading

    file_index = 0
    select_list = []
    file_dict = {}
    for file in file_list:
        # print ("Working on file %s: %s"%(file_index, file))
        file_index = file_index + 1
        if ("day" in file):
            stock_df = pd.read_csv(datafolder_dir + "/" + file)
            if isQualifiedCondition1(stock_df, analysis_type):
                select_list.append("condition 1:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["1:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition2(stock_df, analysis_type):
                select_list.append("condition 2:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["2:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition3(stock_df, analysis_type):
                select_list.append("condition 3:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["3:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition4(stock_df, analysis_type):
                select_list.append("condition 4:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["4:"+completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file

    select_list.sort()

    # ------------------------------
    for file in select_list:
        print(file)

    for index in range(len(select_list)):
        select_list[index] = select_list[index][-8:]

    print(select_list)

    stock_df_list = []
    for item in select_list:
        stock_df_list.append(pd.read_csv(file_dict[item]))

    # ------------------------------

    painter1 = sk.StkDiagram()
    keeper1 = sk.StkDataKeeper()
    keeper1.load_stock_list("stock_basics.csv")
    klines1 = painter1.paint_klines(select_list, "stock_basics.csv")

    klines1.width = 1600
    klines1.render("lyd_result/render1.html")
    klines1.width = 990

def get_current():
    # create folder
    cwd = os.getcwd()
    # today_date = time.strftime("%Y%m%d")
    target_dir = "current"  # today_date
    try:
        os.makedirs(name=target_dir)
    except:
        print('folder already existed')

    # clean up the index with numbers, and make date the default index
    def pre_process(df):
        df.set_index('date', inplace=True)
        return df

    def obtain_daily_data(sk_code, target_dir=""):
        try:
            # print ("Processing: "+sk_code)

            if target_dir != "":
                target_dir = target_dir + os.path.sep

            day_df = pre_process(ts.get_k_data(sk_code))
            csv_filename = target_dir + "day_" + sk_code + ".csv"
            day_df.to_csv(csv_filename)
        except:
            print ("error code is: "+sk_code)


    keeper = sk.StkDataKeeper()
    keeper.load_stock_list("stock_basics.csv")
    code_series = keeper.stock_list

    # prepare arguments
    args_list = []
    for code in code_series:
        args_list.append(([code, target_dir], None))

    # collect
    pool = threadpool.ThreadPool(20)
    requests = threadpool.makeRequests(obtain_daily_data, args_list)
    [pool.putRequest(req) for req in requests]
    pool.wait()

    print('Get data done')

def get_time_current_data(dir=""):
    if dir=="":
        cwd = os.getcwd()
        today_date = time.strftime("%Y%m%d")
        target_dir = cwd + os.path.sep + today_date + "_time"
        try:
            os.makedirs(name=target_dir)
        except:
            print('folder already existed')
        dir=target_dir

    time_mark = time.strftime("%Y%m%d %H%M%S")
    df = ts.get_today_all()
    df.to_csv(os.path.join(dir, time_mark)+".csv")

def update_stocklist():
    keeper = sk.StkDataKeeper()
    keeper.get_stock_basics_tu("stock_basics.csv")

if __name__ == "__main__":
    keeper = sk.StkDataKeeper()
    keeper.get_stock_basics_tu("stock_basics.csv")
    get_current()
    # ssch.scheduler_test_helper(get_time_current_data, second_interval="15")
    # ssch.sched.start()
    # while (1):
    #     time.sleep(1000000)
    # get_current()
    # print (os.sep)

    get_daily_chart()
