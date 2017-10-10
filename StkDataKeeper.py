import pandas as pd
import numpy as np
import tushare as ts
import os, time, datetime
import concurrent.futures
import json

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



class StkHelperData(object):
    'calculate and store the calculated statistics'

    def __init__(self):
        # properties go here
        stk_code = ""

        pass

    # calculate the helper data by transaction data
    def calculate_data(self, df=None):
        pass

    # save data
    def save_data(self, filename=""):
        pass

    # load data
    def load_data(self, filename=""):
        pass

    # update data with recent transactions
    def update_data_batch(self, df=None):
        pass

    # update data with most recent transaction,
    # one by one
    def update_data_single(self, single_transaction=None):
        pass

    # sample of future queries
    def is_zhangting(self, date):
        pass

    def is_dieting(self, date):
        pass


class StkDataKeeper(object):
    'read/write/process/hold data'
    def __init__(self, code_list_file="", helper_data_folder=""):

        # properties
        self.helper_data={}  # code -> StkHelperData
        pass


    def save_to_folder(self, data_folder=""):
        self.helper_data


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

        except:
            return None

        return result_df


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

        except:
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


if __name__ == "__main__":
    pickle_test = PickleDataTest()
    data_source = StkDataKeeper()
    pickle_test.df = data_source.get_realtime_transaction("000001")
    pickle_test.code = "300191"
    import pickle
    p1 = pickle.dumps(pickle_test)
    pickle_test_new = pickle.loads(p1)
    print(pickle_test.df)
    print("------------------------------")
    print(pickle_test_new.df)
    df = pickle_test.df.append(pickle_test_new.df).drop_duplicates()
    print(df)
