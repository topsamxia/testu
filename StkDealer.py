#coding=utf-8

import pandas as pd
import tushare as ts
import StkFX as sfx
import StkDataKeeper as sdk
import json
import os, time, datetime


class DealContext(object):
    'store the data needed for the dealer decision making, initiated by external'
    def __init__(self):
        # a map of {code->data in dataframe}
        self.history_data = None
        # a map of {code->data in dataframe}
        self.today_data = None
        # a map of {code->data in dataframe}
        self.zhishu_data = None

    def get_history_data(self):
        return self.history_data

    def get_today_data(self):
        return self.today_data

    def get_zhishu_data(self):
        return self.zhishu_data


class DealStrategy(object):
    def __init__(self, deal_context=None):
        self.deal_context = deal_context
        pass

    # offset -1 stands for last trade day
    def shang_zhang(self, offset=-1, diff=3):
        pass

    def xia_die(self, offset=-1, diff=-3):
        pass

    def zhang_ting(self, offset=-1):
        pass

    def die_ting(self, offset=-1):
        pass

class StkDealer(object):
    '''simulate the dealer. Using DealContext as strategy; DealStrategy as decision maker;
    and SimulateDeal as balance record
    '''
    def __init__(self):
        self.context = None
        self.account = sfx.SimulateDeal()
        self.strategy = DealStrategy()

    def set_context(self, context):
        self.context = context

    def take_tick_data(self, tick_data):
        pass


class RegressionTester(object):
    'take one data source and perform regression testing for certain strategy'
    def __init__(self):
        self.dealer = StkDealer()
        self.trade_dates = []
        pass

    # get the list of all dates that is trade-able
    def get_trade_dates(self):
        df = ts.get_hist_data('sh')
        self.trade_dates = df.index.tolist()

    # filename uses processed 5 min history data
    def load_data(self, dir_path=""):
        data_files = os.listdir(dir_path)
        data_keeper = sdk.StkDataKeeper()

        history_data = {}
        for data_file in data_files:
            data_keeper.load_stock_list()
            df = data_keeper.read_csv_hist_5min(data_file)
            try:
                stk_code = df.iloc[0].code
                history_data[stk_code] = df
            except:
                pass

        deal_context = DealContext()
        deal_context.history_data = history_data
        self.dealer.set_context(deal_context)

    def loop_daily(self):
        pass



