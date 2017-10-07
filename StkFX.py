# Last Buy time check is wrong in row 246
#
# to add cost calculation in dealer.


#coding=utf-8

import pandas as pd
import numpy as np
import tushare as ts
import os, time, datetime

# Zhang Ting
def zhangTing(close_old, close_new):
    return float(close_new) / float(close_old) >= 1.098

# Die Ting
def dieTing(close_old, close_new):
    return float(close_new) / float(close_old) <= 0.902

# Shang Zhang
def shangZhang(close_old, close_new, factor = 3):
    return float(close_new) / float(close_old) >= factor/100 + 1

# Xia Die
def xiaDie(close_old, close_new, factor = -3):
    return float(close_new) / float(close_old) <= factor/100 + 1

class SingleDeal(object):
    'single deal data struct'
    def __init__(self, date="", time="", open = 0.0, close = 0.0, high = 0.0, low = 0.0, volume = 0.0, total = 0.0, zt = False, dt = False):
        self.date = date
        self.time = time
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.volume = volume
        self.total = total
        self.zt = zt
        self.dt = dt

    def getDeal(self):
        return [self.date, self.time, self.open, self.close, self.high, self.low, self.volume, self.total]

    def setDeal(self, dealInfo):
        if isinstance(dealInfo, pd.Series) :
            # self.date = dealInfo.name
            self.time = dealInfo.name
            self.open = dealInfo.open
            self.close = dealInfo.close
            self.high = dealInfo.high
            self.low = dealInfo.low
            self.volume = dealInfo.volume
            self.total = dealInfo.total
        else:
            if isinstance(dealInfo, SingleDeal):
                self.date = dealInfo.name
                self.time = dealInfo.time
                self.open = dealInfo.open
                self.close = dealInfo.close
                self.high = dealInfo.high
                self.low = dealInfo.low
                self.volume = dealInfo.volume
                self.total = dealInfo.total

class SimulateDeal(object):
    'transaction simulator'
    def __init__(self, balance = 0.0):
        self.record = []
        self.last_buy_time = ("1980/1/1", "00:00")
        self.last_sell_time = ("1980/1/1", "00:00")
        self.balance = balance
        self.hold = {}
        self.occupied = False
        self.daily_lock = {}

    # for string representative
    def __str__(self):
        return ("Balance =" + str(self.balance) + "\n" \
                + "Hold = " + str(self.hold) + "\n" \
                + "Record =" + str(self.record))

    # reset the daily lock
    def resetDaily(self):
        self.daily_lock = {}

    # evaluate daily worth
    def evalWorth(self, priceDict = {}):
        balance = self.balance
        for code in self.hold.keys():
            if code in priceDict.keys():
                balance += priceDict[code] * self.hold[code] * 100
        return balance

    #percentage various between 0~1, means the percentage of the balance for this buy
    def buy(self, date="", time="", code = "", price=0.1, percentage=1):

        # calculate affordable volume
        amount = int((self.balance*percentage/price)//100)

        # not enough money, return
        if amount <= 0:
            return False
        else:
            # update the record amounts
            # reduce balance
            self.balance -= price*amount*100
            # set occupied = True if short of money for a share
            if self.balance/price//100 <= 0: self.occupied = True

            self.record.append(("buy", date, time, code, price, amount))

            if code in self.hold.keys():
                self.hold[code] += amount
            else:
                self.hold[code] = amount
            self.last_buy_time = (date, time)

            if code in self.daily_lock.keys():
                self.daily_lock[code] += amount
            else:
                self.daily_lock[code] = amount

            return True

    def getSellableAmount(self, date="", time="", code=""):
        if not (code in self.hold.keys()): return 0

        if date == self.last_buy_time[0]:
            if not (code in self.daily_lock.keys()):
                return self.hold[code]
            else:
                return self.hold[code]-self.daily_lock[code]
        else: # assume the date difference means the new transaction happens later, so all holds are sellable
            return self.hold[code]


    def sell(self, date="", time="", code = "", price=0.1, amount=-1):
        # return False if no such stk in hold, or zero number of hold, or not enough amount
        if not (code in self.hold.keys()): return False

        # obtain amount that is sellable today
        sellable_amount = self.getSellableAmount(date, time, code)
        if sellable_amount == 0: return False

        # default to sell all amount
        if amount == -1: amount = sellable_amount
        if self.hold[code] <= 0 \
                or sellable_amount < amount: return False

        # sell action
        self.hold[code] -= amount
        self.balance += amount*price*100
        self.occupied = False
        self.record.append(("sell", date, time, code, price, amount))
        self.last_sell_time = (date, time)

        return True

class BaseSerialDeal(object):
    'base class to hold stock data and common utilities'

    def __init__(self):
        self.type = 'base'
        self.initialized = False

        self.data = None # store whole trade data
        self.date_index = None # store the date index as series
        self.date_list = [] # store date list as unique dates
        self.time_index = None # store time index as series
        self.time_list = [] # store time list as unique time

        self.start_date = None
        self.end_date = None
        self.last_update_time = datetime.datetime.now()

        self.daily_summary = {}
        self.dealer = SimulateDeal()

    def setBalance(self, balance):
        self.dealer.balance = balance


    def setDailySummary(self, single_deal):
        if not (single_deal is None):
            self.daily_summary[single_deal.date] = single_deal

    def loadCsv(self, path='default.csv', index=["date", "time"]):
        pass

    def saveCsv(self, path='default.csv'):
        pass

class TimeSerialDeal(BaseSerialDeal):
    'base class to hold stock data and common utilities'

    def __init__(self):
        super(TimeSerialDeal, self).__init__()


    def __str__(self):
        if self.data is None:
            print  ("Empty serial data...")
        else:
            return self.data.__str__()

    # load csv utility for historical data
    def loadCsv(self, file_path='default.csv', with_index = False, index=["date", "time"]):
        # if the original data has no index definition, then needs to set the index, else just honor the existing index
        if not with_index:
            hist_names = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'total']
            self.data = pd.read_csv(file_path, names=hist_names)
        else:
            self.data = pd.read_csv(file_path)

        self.data.set_index(["date", "time"], inplace=True)
        [self.date_list.append(date_str) for date_str in self.data.index.get_level_values('date').unique()]

    def loadCsvNoHead(self, file_path='default.csv', index=["date", "time"]):
        return self.loadCsv(file_path, False, index)

    def loadCsvHasHead(self, file_path='default.csv', index=["date", "time"]):
        return self.loadCsv(file_path, True, index)

    def saveCsv(self, file_path='default.csv'):
        if isinstance(self.data, pd.DataFrame):
            self.data.to_csv(file_path)

    def isQualifiedCondition1(self, date, time, code, price):
        abs_date_index = self.date_list.index(date)
        if abs_date_index>=3: # has at least 3 days of history data to trace
            date_1 = self.date_list[abs_date_index - 1] # yesterday
            date_2 = self.date_list[abs_date_index - 2] # day before yesterday
            date_3 = self.date_list[abs_date_index - 3] # today -3
            if (self.daily_summary[date_2].close * self.daily_summary[date_2].volume > 200000): # if volume is right
                # if day before yesterday is Zhang Ting,  and yesterday Shang Zhang, and today Xia Die
                return (zhangTing(self.daily_summary[date_3].close, self.daily_summary[date_2].close) \
                        and shangZhang(self.daily_summary[date_2].close, self.daily_summary[date_1].close, 3.0) \
                        and xiaDie(self.daily_summary[date_1], price, -3.0)) # if today xia Die -3% or more

        return False

    def isQualifiedCondition2(self, date, time, code, price):
        abs_date_index = self.date_list.index(date)
        if abs_date_index >= 4:
            date_1 = self.date_list[abs_date_index - 1] # today - 1
            date_2 = self.date_list[abs_date_index - 2] # today - 2
            date_3 = self.date_list[abs_date_index - 3] # today - 3
            date_4 = self.date_list[abs_date_index - 3] # today - 4

            if (self.daily_summary[date_3].close * self.daily_summary[date_3].volume > 200000):
                # if day -3 is Zhangting, and Xia Die for 2 days, and now Xia Die again
                return (zhangTing(self.daily_summary[date_4].close, self.daily_summary[date_3].close) \
                    and xiaDie(self.daily_summary[date_3].close, self.daily_summary[date_2].close) \
                    and xiaDie(self.daily_summary[date_2].close, self.daily_summary[date_1].close) \
                    and xiaDie(self.daily_summary[date_1].close, price, -1))
        return False

    def isQualifiedSellCondition1(self, date, time, code, price):
        abs_date_index = self.date_list.index(date)
        if abs_date_index - self.date_list.index(self.dealer.last_buy_time[0]) >= 5: return True
        # to add more sell conditions, and add one record in dealer for current cost to calculate the gain
        # if self.dealer.evalWorth({code, price}) / self.dealer.getCost(code) >= 1.08: return True

    def dealActions(self, date, time, code, price):
        if self.isQualifiedCondition1(date, time, code, price) \
            or self.isQualifiedCondition2(date, time, code, price):
            self.dealer.buy(date, time, code, price)
        if self.isQualifiedSellCondition1(date, time, code, price):
            self.dealer.sell(date, time, code, price)

    def traverseDaily(self, date = "", determine_func = None, determine_param = ()):

        if self.data is None:
            pass
        # initialize daily data and index
        daily_data = self.data.loc[date]
        daily_index = self.date_list.index(date)

        # initialize daily deal status for loop
        daily_deal_status = SingleDeal()
        deal_open = daily_data.iloc[0]
        daily_deal_status.setDeal(deal_open) # set to first transaction numbers

        for time in daily_data.index:
            current_deal = daily_data.loc[time]

            if current_deal.high > daily_deal_status.high:
                daily_deal_status.high = current_deal.high
            if current_deal.low < daily_deal_status.low:
                daily_deal_status.low = current_deal.low

            daily_deal_status.volume += current_deal.volume
            daily_deal_status.total += current_deal.total

            daily_deal_status.close = current_deal.close

            # trigger deal actions -------------------------------------------
            self.dealActions(date, time, "300191", current_deal.close)
            # ----------------------------------------------------------------

        # store the daily deal status to the record
        self.daily_summary[date] = daily_deal_status


        # determine_func(*determine_param)
        #
        # for index, item in enumerate(daily_data.index):
        #     print (index, "--> ", daily_data.loc[item])

    # dummy strategy
    def determineStrategy(self, determine_param = []):
        print (determine_param.index)


    def traverseAll(self, determine_func = None, determine_param = ()):
        for index, this_date in enumerate(self.date_list):
            self.traverseDaily(this_date, determine_func, determine_param)



if __name__ == '__main__':
    serial_data = TimeSerialDeal()
    serial_data.loadCsv('hist_sample/SZ300191.csv')

    for date in serial_data.date_list:
        serial_data.traverseDaily(date)

    # #print (serial_data)
    # serial_data.traverseDaily("2017/01/03", serial_data.determineStrategy, ("sdfe",))
    #
    # stk_deal = SimulateDeal(100000)
    # stk_deal.buy(date="2017/9/23", time="10:00", code = "300191", price=18.5, percentage=0.5)
    # stk_deal.sell(date="2017/9/23", time="", code="300191", price=20.2)
    # stk_deal.buy(date="2017/9/23", time="10:00", code = "300191", price=20, percentage=0.5)
    #
    # value = stk_deal.evalWorth({"300191": 19})
    #
    # stk_deal.resetDaily()
    # stk_deal.sell(date="2017/9/24", time="09:30", code= "300191", price=21)
    # print (stk_deal.evalWorth())
    #
    # print (stk_deal)





