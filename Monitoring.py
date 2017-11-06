#coding=utf-8

import pandas as pd
import tushare as ts
import os, time
import concurrent.futures


def isShangzhang(s_df, index=-1, diff=2.0):
    if len(s_df.index) < (abs(index) + 1):
        return False
    return (s_df.iloc[index].close/s_df.iloc[index-1].close) > (diff/100+1)


def isXiadie(s_df, index=-1, diff=-3.0):
    if len(s_df.index) < (abs(index) + 1):
        return False
    return (s_df.iloc[index].close / s_df.iloc[index - 1].close) < (diff / 100 + 1)

# determine 涨停
def isZhangting(s_df, index=-1):
    return isShangzhang(s_df, index, diff=9.8)


def isDieting(s_df, index=-1):
    return isXiadie(s_df, index, diff=-9.8)


def getVolumeRatio(s_df, index):
    if len(s_df.index) < (abs(index) + 1):
        return 9999
    volume_yesterday = s_df.iloc[index - 1].volume
    if volume_yesterday == 0:
        return 9999
    return s_df.iloc[index].volume/s_df.iloc[index-1].volume

# def isShangzhang(s_df, index=-1):
#     if len(s_df.index) >= (index) + 1:
#         return ((s_df.iloc[index].close / s_df.iloc[index - 1].close - 1) > 0.02)
#     else:
#         return False


# determine 条件一
def isQualifiedCondition1(s_df, index=-1):
    if len(s_df.index) >= (abs(index) + 2):
        if (s_df.iloc[index].close * s_df.iloc[index].volume > 200000):
            return (isZhangting(s_df, index - 1) and isShangzhang(s_df, index, 2.0))
    return False


# determine 条件二
def isQualifiedCondition2(s_df, index=-1):
    if len(s_df.index) >= (abs(index) + 4):

        # 相对涨停日下跌超过7%
        # 今日下跌
        # 昨日下跌
        # 昨日放量不超过前日的1.6倍
        # 今日放量不长过昨日的0.7倍
        # 涨停前一天上涨不超过3%
        # 前日涨停

        if (s_df.iloc[index].low / s_df.iloc[index - 2].close < 0.93) \
                and (s_df.iloc[index].close < s_df.iloc[index - 1].close) \
                and (s_df.iloc[index - 1].close < s_df.iloc[index - 2].close) \
                and getVolumeRatio(s_df, index - 1) < 1.6 \
                and getVolumeRatio(s_df, index) < 0.7 \
                and not isShangzhang(s_df, index - 3, 3.0) \
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


# determine 条件四, 接近跌停且量小
def isQualifiedCondition4(s_df, index=-1):
    if len(s_df.index) >= (abs(index) + 2):
        if isZhangting(s_df, index-1):
            if isXiadie(s_df, index, -9.2):
                if (s_df.iloc[index].volume / s_df.iloc[index-1].volume < 0.33):
                    return True
    return False

def isQualifiedCondition5(s_df, index=-1):
    if len(s_df.index) >= (abs(index) + 3):
        if isZhangting(s_df, index-2): # 前天涨停
            if isShangzhang(s_df, index-1): # 昨日上涨
                volume_ratio_yesterday = getVolumeRatio(s_df, index-1)
                if volume_ratio_yesterday > 1.4: # 量大至1.4倍
                    price_zhangting = s_df.iloc[index-2].close
                    price_today = s_df.iloc[index].close
                    if (volume_ratio_yesterday >=2.5 and price_today/price_zhangting<0.94) \
                        or (volume_ratio_yesterday <= 2.5 and price_today/price_zhangting<0.96):
                        if getVolumeRatio(s_df, index) < 0.65:
                            return True
    return False


def isQualifiedCondition6(s_df, index=-1):
    if len(s_df.index) >= (abs(index) + 4):
        if isZhangting(s_df, index - 3):  # 大前天涨停
            if isShangzhang(s_df, index - 2):  # 前日上涨
                volume_ratio_day_before_yesterday = getVolumeRatio(s_df, index - 2)
                if volume_ratio_day_before_yesterday > 1.4:  # 前日量大至1.4倍
                    # 昨日下跌不及预想，但量缩
                    if (volume_ratio_day_before_yesterday >= 2.5 and (not isXiadie(s_df, index-1, -6.0))) \
                        or (volume_ratio_day_before_yesterday <= 2.5 and (not isXiadie(s_df, index-1, -4.0))):
                            if getVolumeRatio(s_df, index-1) < 0.7 and getVolumeRatio(s_df, index) < 0.7:
                                if s_df.iloc[index].low/s_df.iloc[index-3].close < (1.0-7/100.0):
                                    return True
    return False


def isQualifiedCondition7(s_df, index=-1):
    if len(s_df.index) >= (abs(index) + 3):
        if isZhangting(s_df, index-1) and (not isShangzhang(s_df, index-2, 3.0)):
            if getVolumeRatio(s_df, index) < 1.1:
                if s_df.iloc[index].low/s_df.iloc[index].high < 0.94:
                    if isShangzhang(s_df, index, 0.0) and (not isShangzhang(s_df, index, 3.0)):
                        return True
    return False

def completeCode(code_str):
    if len(code_str) < 6:
        return "0" * (6 - len(code_str)) + code_str
    else:
        return code_str

def get_info(s_df, index=-1, prefix=""):
    try:
        date = s_df.iloc[index].date
        code = s_df.iloc[index].code
        ratio_tomorrow = ""
        ratio_tomorrow_plus = ""
        if index < -2:
            ratio_tomorrow = str(((s_df.iloc[index + 1].close / s_df.iloc[index].close) - 1) * 100)
            ratio_tomorrow_plus = str(((s_df.iloc[index + 2].close / s_df.iloc[index + 1].close) - 1) * 100)
        return (prefix+date+", "+code+", "+ratio_tomorrow+", "+ratio_tomorrow_plus)
    except:
        return ""


def traverse_today():
    print(time.strftime("%M:%S"))

    ########################################
    datafolder_dir = os.getcwd() + "/current"
    ########################################

    file_list = os.listdir(datafolder_dir)

    file_index = 0
    select_list = []
    file_dict = {}
    for file in file_list:
        # print ("Working on file %s: %s"%(file_index, file))
        file_index = file_index + 1
        if ("day" in file):
            stock_df = pd.read_csv(datafolder_dir + "/" + file)
            if isQualifiedCondition1(stock_df):
                select_list.append("condition 1:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict[completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition2(stock_df):
                select_list.append("condition 2:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict[completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file
            if isQualifiedCondition3(stock_df):
                select_list.append("condition 3:" + completeCode(str(stock_df.iloc[0].code)))
                file_dict[completeCode(str(stock_df.iloc[0].code))] = datafolder_dir + "/" + file

    select_list.sort()

    for file in select_list:
        print(file)

    print(file_dict)
    print(time.strftime("%M:%S"))

    for index in range(len(select_list)):
        select_list[index] = select_list[index][-6:]

    print(select_list)

    stock_df_list = []
    for item in select_list:
        print(item)
        stock_df_list.append(pd.read_csv(file_dict[item]))

    print(len(stock_df_list))
    print(stock_df_list[1].tail())

    for index in range(len(select_list)):
        df = ts.get_realtime_quotes(select_list[index])
        # print (float(stock_df_list[index].tail(1).close), float(df.iloc[0]["price"]))
        ratio = (float(df.iloc[0]["price"]) / float(stock_df_list[index].tail(1).close))
        if ratio < 0.97 and ratio > 0.85:
            print(select_list[index], "diff is %s" % ((1 - ratio) * 100))


def traverse_file(filename, index_range=-60):
    stock_df = pd.read_csv(filename, dtype={"code": str}, encoding="utf-8")
    # print(filename)

    while index_range < 0:
        # if isQualifiedCondition3(stock_df, index_range):
        #     print(get_info(stock_df, index_range, "Condition 3: "))

        if isQualifiedCondition4(stock_df, index_range):
            print(get_info(stock_df, index_range, "Condition 4: "))
        if isQualifiedCondition5(stock_df, index_range):
            print(get_info(stock_df, index_range, "Condition 5: "))
        if isQualifiedCondition6(stock_df, index_range):
            print(get_info(stock_df, index_range, "Condition 6: "))
        # if isQualifiedCondition7(stock_df, index_range):
        #     print(get_info(stock_df, index_range, "Condition 7: "))

        index_range += 1

if __name__ == "__main__":
    folder_dir = os.getcwd() + "/current"
    file_list = os.listdir(folder_dir)
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        for file in file_list:
            executor.submit(traverse_file,
                            os.path.join(folder_dir, file),
                            -300)
    # for file in file_list:
    #     stock_df = pd.read_csv(os.path.join(folder_dir, file), dtype={"code": str}, encoding="utf-8")
    #     print(os.path.join(folder_dir, file))
    #
    #     index_range = -60
    #     while index_range < 0:
    #         if isQualifiedCondition4(stock_df, index_range):
    #             print(get_info(stock_df, index_range, "Condition 4: "))
    #         if isQualifiedCondition5(stock_df, index_range):
    #             print(get_info(stock_df, index_range, "Condition 5: "))
    #         if isQualifiedCondition6(stock_df, index_range):
    #             print(get_info(stock_df, index_range, "Condition 6: "))
    #         if isQualifiedCondition7(stock_df, index_range):
    #             print(get_info(stock_df, index_range, "Condition 7: "))
    #         index_range += 1