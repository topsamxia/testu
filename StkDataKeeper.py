import pandas as pd
import numpy as np
import tushare as ts
import os, time, datetime
import concurrent.futures
import json, pickle

# pickle can be done by
# explicit pickle every property and make them a dictionary, then call pickle
# this could lead to a pre-processor and post-processor for property pickle
# reference page is
# https://docs.python.org/2.7/library/pickle.html#module-pickle
# https://www.ibm.com/developerworks/cn/linux/l-pypers/index.html
# http://www.cnblogs.com/itech/archive/2012/01/10/2318293.html
# https://python.freelycode.com/contribution/detail/460

class PickleEnabler(object):
    'define the interfaces to implement the pickle protocols'
    def __init__(self):
        self.pickle_storage={}

    # build pickle storage dictionary
    def pre_process_pickle(self):
        pass

    # restore pickle storage dictionary
    def post_process_pickle(self):
        pass

    def __getstate__(self):
        self.pre_process_pickle()
        return self.pickle_storage

    def __setstate__(self, state):
        self.pickle_storage = state
        self.post_process_pickle()
        del self.pickle_storage

class PickleDataTest(PickleEnabler):
    def __init__(self):
        self.df=None
        self.code=""
        super(PickleDataTest, self).__init__()

    def pre_process_pickle(self):
        self.pickle_storage["df"]=self.df.to_dict()
        self.pickle_storage["code"]=self.code

    def post_process_pickle(self):
        if "df" in self.pickle_storage.keys():
            self.df = pd.DataFrame.from_dict(self.pickle_storage["df"])
        if "code" in self.pickle_storage.keys():
            self.code = self.pickle_storage["code"]

class DailySequentialStatus(object):
    def __init__(self, data_tuple=()):
        self.open = 0.0
        self.close = 0.0
        self.high = 0.0
        self.low = 0.0
        self.volume = 0
        self.total = 0.0
        self.zdf = 0.0
        self.code = ""

        # sampling price across the day
        self.sampling = []

        # load data if available
        if len(data_tuple) == 9:
            self.load_data(data_tuple)

    def dump_data(self):
        return (self.open, self.close, self.high, self.low,
                self.volume, self.total, self.zdf, self.code,
                self.sampling)

    def load_data(self, data_tuple=()):
        if len(data_tuple) == 9:
            (self.open, self.close, self.high,
             self.low, self.volume, self.total,
             self.zdf, self.code, self.sampling) = data_tuple

        # super(StkHelperData, self).__init__()

class StkHelperData(PickleEnabler):
    'calculate and store the calculated statistics, for given stock'

    def __init__(self):
        # properties go here
        self.stk_code = ""

        # date->DailySequentialStatus
        self.seqential_status_map={}

        super(StkHelperData, self).__init__()

    # calculate the helper data by transaction data
    # input is the 5 minute series system
    def calculate_data(self, df=None):

        date_str_list = []
        [date_str_list.append(date_str) for date_str in df.index.get_level_values('date').unique()]

        for date_current in date_str_list:

            df_current = df.loc[date_current]

            sequential_status = DailySequentialStatus()
            sequential_status.open = df_current.iloc[0].open
            sequential_status.close = df_current.iloc[-1].close
            sequential_status.low = df_current['low'].min(axis=0)
            sequential_status.high = df_current['high'].max(axis=0)
            sequential_status.volume = df_current['volume'].sum(axis=0)
            sequential_status.total = df_current['total'].sum(axis=0)

            sequential_status.sampling = df_current['close'].tolist()

            date_current_index = date_str_list.index(date_current)
            if date_current_index >= 1:
                last_close = self.seqential_status_map[date_str_list[date_current_index-1]].close
                sequential_status.zdf = sequential_status.close/last_close - 1.0
            self.seqential_status_map[date_current] = sequential_status

    def pre_process_pickle(self):
        self.pickle_storage["stk_code"] = self.stk_code
        temp_map = {}
        for key in self.seqential_status_map.keys():
            temp_map[key] = self.seqential_status_map[key].dump_data()
        self.pickle_storage["sequential_status_map"] = temp_map

    def post_process_pickle(self):
        if "stk_code" in self.pickle_storage.keys():
            self.stk_code = self.pickle_storage["stk_code"]
        if "sequential_status_map" in self.pickle_storage.keys():
            temp_map = self.pickle_storage["sequential_status_map"]
            self.seqential_status_map={}
            for key in temp_map.keys():
                self.seqential_status_map[key] = DailySequentialStatus(temp_map[key])

    # save data
    def save_data(self, filename=""):
        try:
            with open(filename, 'w') as f:
                pickle.dump((self.stk_code, self.seqential_status_map), f)
                return True
        finally:
            return False

    # load data
    def load_data(self, filename=""):
        try:
            with open(filename, 'r') as f:
                self.stk_code, self.seqential_status_map = pickle.load(filename)
            return True
        finally:
            return False

    # update data with recent transactions
    def update_data_batch(self, df=None):
        pass

    # update data with most recent transaction,
    # one by one
    def update_data_single(self, single_transaction=None):
        pass

    # sample of future queries
    def is_zhangting(self, date=""):
        pass

    def is_dieting(self, date=""):
        pass

class StkDataKeeper(object):
    'read/write/process/hold data'
    def __init__(self, code_list_file="", helper_data_folder=""):

        # properties
        self.helper_data={}  # code -> StkHelperData
        pass


    def save_to_folder(self, data_folder=""):
        pass


    def splitDateTime(self, df=None):
        if len(df.index) == 0: pass

        def splitDate(x):
            return x.split(" ")[0]

        def splitTime(x):
            return x.split(" ")[1]

        series_date = df.date.apply(splitDate)
        series_time = df.date.apply(splitTime)

        df["date"] = series_date
        df["time"] = series_time

    # get data from given csv
    # return a formatted DataFrame object
    def read_csv(self, filename="", with_head=True, index=["date", "time"]):
        try:
            # if the original data has no index definition, then needs to set the index, else just honor the existing index
            if with_head:
                result_df = pd.read_csv(filename)
            else:
                hist_names = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'total']
                result_df = pd.read_csv(filename, names=hist_names)

            result_df.set_index(index, inplace=True)

        finally:
            return None

        return result_df

    # date             open   close  high   low    volume  code
    # 2014-11-17 14:55 21.946 22.305 22.465 21.047 33923.0 300191
    #
    # return formatted date & time indexed dataframe, with total estimation
    def read_csv_tu_5min(self, filename=""):
        df = self.read_csv(filename, True, index="date")
        self.splitDateTime(df)
        # simulate the total with calculation
        df["total"] = df["volume"]*100*(df["open"]+df["close"]+df["high"]+df["low"])/4
        return df

    # date       open   close  high   low    volume  code
    # 2014-11-17 21.946 22.305 22.465 21.047 33923.0 300191
    #
    # return: date indexed DataFrame
    def read_csv_tu_daily(self, filename=""):
        df = self.read_csv(filename, True, index="date")
        return df

    # date time open high low close volume total
    # 2015/1/5 9:35 16.47 16.47 16.18 16.2 2355 3838604
    def read_csv_hist_5min(self, filename=""):
        df = pd.read_csv(filename)
        # if len(df.index) == 0: pass
        #
        # def splitDate(x):
        #     return x.split(" ")[0]
        #
        # def splitTime(x):
        #     return x.split(" ")[1]
        #
        # series_date = df.date.apply(splitDate)
        # series_time = df.date.apply(splitTime)
        #
        # df["date"] = series_date
        # df["time"] = series_time
        # df["total"] = round((df["close"]+df["high"]+df["low"])*df["volume"]*100/3, 2)

        df.set_index(["date", "time"], inplace=True)
        return df

    # merge data for data maintenance
    # if daily data then index = ["date"]
    # if time transaction data then index = ["date", "time"]
    def merge_csv(self, base_file="", new_file="", target_file="", index=["date", "time"]):
        try:
            new_pd = self.read_csv(new_file, index=index)
            old_pd = self.read_csv(base_file, index=index)

            new_pd = old_pd.append(new_pd)
            new_pd.drop_duplicates(inplace=True)
            new_pd.to_csv(target_file)

            return True

        finally:
            print("error: " + new_file)
            return False

    # get realtime transaction
    # for specific code, or all when code = ""
    def get_realtime_transaction(self, code=""):
        if code == "":
            df = ts.get_today_all()
        else:
            df = ts.get_realtime_quotes(code)
        return df

    # update realtime transaction
    # append the recent transaction to the analysis
    def update_realtime_transaction(self, code=""):
        pass

    # Update the helper data with additional transaction calculation results
    # inout is either file of new daily transaction or realtime data
    def load_helper_data(self, folder_name="", df=None):
        pass


def testPickle():
    pickle_test = PickleDataTest()
    data_source = StkDataKeeper()
    pickle_test.df = data_source.get_realtime_transaction("000001")
    pickle_test.code = "300191"
    p1 = pickle.dumps(pickle_test)
    pickle_test_new = pickle.loads(p1)
    print(pickle_test.df)
    print("------------------------------")
    print(pickle_test_new.df)
    df = pickle_test.df.append(pickle_test_new.df).drop_duplicates()
    print(df)

if __name__ == "__main__":
    data_keeper = StkDataKeeper()
    # df = data_keeper.read_csv("stksample\\20170929_150000.csv", index="code")
    # print(df)
    df = data_keeper.read_csv_hist_5min("stksample\\SZ300191.csv")
    print(df)

    helper_data = StkHelperData()
    helper_data.calculate_data(df)

    p1 = pickle.dumps(helper_data)
    helper_data_new = pickle.loads(p1)
    print(helper_data_new.seqential_status_map.keys())
    # helper_data.save_data("stksample\\300191_helper.save")
    # helper_data_restore = StkHelperData()
    # helper_data_restore.load_data("data\\300191_helper.save")
    # print(helper_data_restore.seqential_status_map.keys())

