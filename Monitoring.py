#coding=utf-8

import pandas as pd
import tushare as ts
import os, time



# determine 涨停
def isZhangting(s_df, index=-1):
    if (s_df.iloc[index].close / s_df.iloc[index - 1].close) >= 1.099:
        return True
    else:
        return False


# determine 上涨
def isShangzhang(s_df, index=-1):
    if len(s_df.index) >= (index) + 1:
        return ((s_df.iloc[index].close / s_df.iloc[index - 1].close - 1) > 0.02)
    else:
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


print (time.strftime("%M:%S"))

########################################
datafolder_dir = os.getcwd() + "/20170921"
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
    print (file)

print (file_dict)
print (time.strftime("%M:%S"))

for index in range(len(select_list)):
    select_list[index] = select_list[index][-6:]

print (select_list)

stock_df_list = []
for item in select_list:
    print (item)
    stock_df_list.append(pd.read_csv(file_dict[item]))

print (len(stock_df_list))
print (stock_df_list[1].tail())


for index in range(len(select_list)):
    df = ts.get_realtime_quotes(select_list[index])
    # print (float(stock_df_list[index].tail(1).close), float(df.iloc[0]["price"]))
    ratio = (float(df.iloc[0]["price"]) / float(stock_df_list[index].tail(1).close))
    if ratio < 0.97 and ratio > 0.85:
        print (select_list[index], "diff is %s"%((1-ratio)*100))

