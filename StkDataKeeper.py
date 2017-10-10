import pandas as pd
import numpy as np
import tushare as ts
import os, time, datetime
import concurrent.futures
import json


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
    def __init__(self):

        # properties
        self.helper_data={}  # code -> StkHelperData
        pass

    # get data from given csv
    def read_csv(self, filename="", with_head=True, index=["date", "time"]):
        pass

    # merge data for data maintenance
    def merge_csv(self, base_file="", new_file="", target_file=""):
        pass

    # get realtime transaction
    def get_realtime_transaction(self):
        pass

    # update realtime transaction
    # append the recent transaction to the analysis
    def update_realtime_transaction(self):
        pass

    # Update the helper data with additional transaction profiles
    # inout is either file of new daily transaction or realtime data
    def load_helper_data(self, folder_name="", df=None):
        pass

    def deal_transaction_