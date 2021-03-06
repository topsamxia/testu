
import pandas as pd
import tushare as ts
import os
import time, datetime
import threadpool
import StkDataKeeper as sk
import StkScheduler as ssch

exclude_stock_list = ["300362",]

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

        if sk_code in exclude_stock_list:
            return

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


def get_specific_day_chart(folder_name="", file_name_prefix="chart", split_unit=60, analysis_index=-1):
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

    # determine 条件五
    # 今天涨停，二三四天前也有涨停
    def isQualifiedCondition5(s_df, index=-1):
        if isZhangting(s_df, index):
            if (isZhangting(s_df, index - 2) or isZhangting(s_df, index - 3) or isZhangting(s_df, index - 4)
                    or isZhangting(s_df, index - 5)):
                return True
        return False

    # determine 条件四
    # 第一天涨停， 当中下跌2-3天，第四天涨
    def isQualifiedCondition4(s_df, index=-1):
        if len(s_df.index) < (abs(index) + 3):
            return False
        if isZhangting(s_df, index-3) and isZhangting(s_df, index):
            if isXiadie(s_df, index-2, -0.02) and isXiadie(s_df, index-1, -0.02):
                return True

        if len(s_df.index) < (abs(index) + 4):
            return False
        if isZhangting(s_df, index-4) and isZhangting(s_df, index):
            if isXiadie(s_df, index-3, -0.02) and isXiadie(s_df, index-2, -0.02) and isXiadie(s_df, index-1, -0.02) \
                and ((s_df.iloc[index-1].close / s_df.iloc[index-4].close - 1) < -0.09):
                return True

        return False

    # determine 条件一
    def isQualifiedCondition1(s_df, index=-1):
        if len(s_df.index) >= (abs(index) + 2):
            if (s_df.iloc[index].close * s_df.iloc[index].volume > 200000):
                return (isZhangting(s_df, index - 1) and isShangzhang(s_df, index))
        return False

    # determine 条件二
    def isQualifiedCondition2(s_df, index=-1):
        if len(s_df.index) >= (abs(index) + 3):
            if (s_df.iloc[index].close / s_df.iloc[index - 2].close < 0.92) \
                    and (s_df.iloc[index].close < s_df.iloc[index - 1].close) \
                    and (s_df.iloc[index - 1].close < s_df.iloc[index - 2].close) \
                    and isZhangting(s_df, index - 2):
                return True
        return False

    # determine 条件三
    def isQualifiedCondition3(s_df, index=-1):
        if len(s_df.index) >= (abs(index) + 3):
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
    try:
        file_list = os.listdir(folder_name)
    except:
        print(time.strftime("%M:%S"))
        return

    analysis_type = analysis_index

    file_index = 0
    select_list = []
    file_dict = {}
    for file in file_list:
        # print ("Working on file %s: %s"%(file_index, file))
        file_index = file_index + 1
        if ("day" in file):
            stock_df = pd.read_csv(folder_name + "/" + file)
            if isQualifiedCondition1(stock_df, analysis_type):
                select_list.append("condition 1:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["1:" + completeCode(str(stock_df.iloc[0].code))] = folder_name + "/" + file
            if isQualifiedCondition2(stock_df, analysis_type):
                select_list.append("condition 2:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["2:" + completeCode(str(stock_df.iloc[0].code))] = folder_name + "/" + file
            if isQualifiedCondition3(stock_df, analysis_type):
                select_list.append("condition 3:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["3:" + completeCode(str(stock_df.iloc[0].code))] = folder_name + "/" + file
            # if isQualifiedCondition4(stock_df, analysis_type):
            #     select_list.append("condition 4:" + completeCode(str(stock_df.iloc[0].code)))
            #     file_dict["4:" + completeCode(str(stock_df.iloc[0].code))] = folder_name + "/" + file
            if isQualifiedCondition5(stock_df, analysis_type):
                select_list.append("condition 5:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["5:" + completeCode(str(stock_df.iloc[0].code))] = folder_name + "/" + file

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

    # --------------------------------------
    splitUnit = 60
    numberOfCharts = len(select_list) // splitUnit
    if ((numberOfCharts * splitUnit) < len(select_list)):
        numberOfCharts += 1

    for i in range(numberOfCharts):
        index_low = i * splitUnit
        index_high = i * splitUnit + splitUnit
        if ((i + 1) == numberOfCharts):
            index_high = len(select_list)

        klines = painter.paint_klines(select_list[index_low:index_high], "stock_basics.csv",
                                      serial_from=(i * splitUnit))

        klines.width = 1600
        klines.render(file_name_prefix + str(i) + ".html")
        klines.width = 990


def get_daily_zhangting_list(folder_name="", index=-1, target_filename = "lyd_result/zhangting.txt"):

    if folder_name == "":
        today_date = time.strftime("%Y%m%d")
        target_dir = today_date
        folder_name = os.getcwd() + os.sep + target_dir

    # determine 上涨
    def isShangzhang(s_df, index=-1, increase=0.02):
        if len(s_df.index) >= abs(index) + 1:
            return ((s_df.iloc[index].close / s_df.iloc[index - 1].close - 1) > increase)
        else:
            return False

    # determine 涨停
    def isZhangting(s_df, index=-1):
        return isShangzhang(s_df, index, 0.098)

    def completeCode(code_str):
        if len(code_str) < 6:
            return "0" * (6 - len(code_str)) + code_str
        else:
            return code_str

    # get folder
    print(time.strftime("%M:%S"))
    try:
        file_list = os.listdir(folder_name)
    except:
        print(time.strftime("%M:%S"))
        print("failed to locate folder")
        return

    file_index = 0
    select_list = []

    keeper = sk.StkDataKeeper()
    try:
        keeper.get_stock_basics_tu("stock_basics.csv")
    except:
        keeper.load_stock_list("stock_basics.csv")

    keeper.load_stock_list("stock_basics.csv")

    for file in file_list:
        # print ("Working on file %s: %s"%(file_index, file))
        file_index = file_index + 1
        if ("day" in file):
            stock_df = pd.read_csv(folder_name + "/" + file)
            if isZhangting(stock_df, index):
                stk_id = completeCode(str(stock_df.iloc[0].code))
                stk_name = ""
                try:
                    stk_name = keeper.stock_basics.loc[stk_id][0]
                except:
                    stk_name = stk_id

                select_list.append(stk_name)

    try:
        print(target_filename)
        print(select_list)
        f = open(target_filename, "w", encoding='utf-8')
        print("file open successfully")
        for name in select_list:
            f.writelines(name + "\n")
            print(name)
        f.close()
    except:
        print("failed to output daily zhangting")

def get_specific_day_zhangting_chart(folder_name="", file_name_prefix="zhangting", split_unit=60, analysis_index=-1):
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


    def completeCode(code_str):
        if len(code_str) < 6:
            return "0" * (6 - len(code_str)) + code_str
        else:
            return code_str

    print(time.strftime("%M:%S"))
    try:
        file_list = os.listdir(folder_name)
    except:
        print(time.strftime("%M:%S"))
        return

    analysis_type = analysis_index

    file_index = 0
    select_list = []
    file_dict = {}
    for file in file_list:
        # print ("Working on file %s: %s"%(file_index, file))
        file_index = file_index + 1
        if ("day" in file):
            stock_df = pd.read_csv(folder_name + "/" + file)
            if isZhangting(stock_df, analysis_type):
                select_list.append("zhangting:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict["g:" + completeCode(str(stock_df.iloc[0].code))] = folder_name + "/" + file

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

    # --------------------------------------
    splitUnit = 60
    numberOfCharts = len(select_list) // splitUnit
    if ((numberOfCharts * splitUnit) < len(select_list)):
        numberOfCharts += 1

    for i in range(numberOfCharts):
        index_low = i * splitUnit
        index_high = i * splitUnit + splitUnit
        if ((i + 1) == numberOfCharts):
            index_high = len(select_list)

        klines = painter.paint_klines(select_list[index_low:index_high], "stock_basics.csv",
                                      serial_from=(i * splitUnit))

        klines.width = 1600
        klines.render(file_name_prefix + str(i) + ".html")
        klines.width = 990

def get_daily_chart(analysis_type=-1):
    ########################################
    today_date = time.strftime("%Y%m%d")
    target_dir = today_date
    datafolder_dir = os.getcwd() + os.sep + target_dir

    # datafolder_dir = os.getcwd() + os.sep +"current"
    ########################################
    get_specific_day_chart(datafolder_dir, "lyd_result/render_today", 60, -1)
    get_specific_day_chart(datafolder_dir, "lyd_result/render_yesterday", 60, -2)
    get_specific_day_zhangting_chart(datafolder_dir, "lyd_result/zhangting", 60, -1)


def get_daily_chart_current(analysis_type=-1):
    print(time.strftime("%M:%S"))

    ########################################
    datafolder_dir = os.getcwd() + os.sep + "current"
    ########################################

    get_specific_day_chart(datafolder_dir, "lyd_result/render_current", 60, -1)



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
    # keeper = sk.StkDataKeeper()
    # keeper.get_stock_basics_tu("stock_basics.csv")
    # get_current()
    # ssch.scheduler_test_helper(get_time_current_data, second_interval="15")
    # ssch.sched.start()
    # while (1):
    #     time.sleep(1000000)
    # get_current()
    # print (os.sep)
    # get_daily_data()
    # get_daily_chart()
    # get_current()
    # get_daily_chart_current()
    # get_daily_zhangting_list()
    today_date = time.strftime("%Y%m%d")
    target_dir = today_date
    datafolder_dir = os.getcwd() + os.sep + target_dir
    get_specific_day_zhangting_chart(datafolder_dir, "lyd_result/zhangting", 60, -1)


