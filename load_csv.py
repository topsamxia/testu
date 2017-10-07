#coding=utf-8
# import numpy as np
import pandas as pd
import os, time
import tushare as ts

# determine 涨停
def isZhangting(s_df, index=-1):
    if (s_df.iloc[index].close/s_df.iloc[index-1].close) >= 1.099:
        return True
    else:
        return False

#determine 上涨
def isShangzhang(s_df, index=-1):
    if len(s_df.index) >= (index)+1:
        return ((s_df.iloc[index].close/s_df.iloc[index-1].close-1) > 0.02)
    else:
        return False

#determine 条件一
def isQualifiedCondition1(s_df, index=-1):
    if len(stock_df.index)>=(abs(index)+2):
        if (s_df.iloc[index].close * s_df.iloc[index].volume > 200000):
            return  (isZhangting(s_df, index-1) and isShangzhang(s_df,index))
        else:
            return  False
    else:
        return False

def isQualifiedCondition2(s_df, index=-1):
    if len(stock_df.index)>=(abs(index)+3):
        if (s_df.iloc[index].close / s_df.iloc[index-2].close < 0.92) \
                and (s_df.iloc[index].close<s_df.iloc[index-1].close) \
                and (s_df.iloc[index-1].close<s_df.iloc[index-2].close)\
                and isZhangting(s_df, index-2):
            return True
        else:
            return False

def completeCode(code_str):
    if len(code_str)<6:
        return "0"*(6-len(code_str))+code_str
    else:
        return code_str

print (time.strftime("%M:%S"))
datafolder_dir = os.getcwd()+"\\20170913"
file_list = os.listdir(datafolder_dir)
# print (file_list[0:3])

# file_index = 4716
# stock_df = pd.read_csv(datafolder_dir+"\\"+file_list[file_index])
# print (stock_df.tail())
# print (len(stock_df.index))
# print (isQualfiedCondition1(stock_df))

file_index = 0
select_list = []

for file in file_list:
    # print ("Working on file %s: %s"%(file_index, file))
    file_index = file_index + 1
    if ("day" in file):
        stock_df = pd.read_csv(datafolder_dir+"\\"+file)
        if isQualifiedCondition1(stock_df):
            select_list.append("condition 1:" + completeCode(str(stock_df.iloc[0].code)))
        if isQualifiedCondition2(stock_df):
            select_list.append("condition 2:" + completeCode(str(stock_df.iloc[0].code)))

select_list.sort()

for file in select_list:
    print (file)

print (time.strftime("%M:%S"))
