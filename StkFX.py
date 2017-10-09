# Last Buy time check is wrong in row 246
#
# to add cost calculation in dealer.


#coding=utf-8

import pandas as pd
import numpy as np
import tushare as ts
import os, time, datetime
import concurrent.futures
import json

# Shang Zhang
def shangZhang(close_old, close_new, factor = 3):
    return float(close_new) / float(close_old) >= factor/100 + 1

# Xia Die
def xiaDie(close_old, close_new, factor = -3):
    return float(close_new) / float(close_old) <= factor/100 + 1

# Zhang Ting
def zhangTing(close_old, close_new):
    return shangZhang(close_old, close_new, 9.8)

# Die Ting
def dieTing(close_old, close_new):
    return xiaDie(close_old, close_new, -9.8)

# structure that to hold single transaction or daily summary
class SingleDeal(object):
    'single deal data struct'
    def __init__(self, code="", is_buy = True, date="", time="", open = 0.0, close = 0.0, high = 0.0, low = 0.0, volume = 0, total = 0.0, zt = False, dt = False):
        self.code = code
        self.is_buy = is_buy
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

    def __str__(self):
        return str([self.code, \
                    self.is_buy, \
                    self.date, \
                    self.time, \
                    self.open, \
                    self.close, \
                    self.high, \
                    self.low, \
                    self.volume, \
                    self.total, \
                    self.zt, \
                    self.dt])

    def __repr__(self):
        return self.__str__()

    def getDeal(self):
        return [self.code, self.is_buy, self.date, self.time, self.open, self.close, self.high, self.low, self.volume, self.total, self.zt, self.dt]

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
                self.code = dealInfo.code
                self.is_buy = dealInfo.is_buy
                self.date = dealInfo.date
                self.time = dealInfo.time
                self.open = dealInfo.open
                self.close = dealInfo.close
                self.high = dealInfo.high
                self.low = dealInfo.low
                self.volume = dealInfo.volume
                self.total = dealInfo.total
                self.zt = dealInfo.zt
                self.dt = dealInfo.dt


class SimulateDeal(object):
    'transaction simulator, obtain data, process data, store result'
    def __init__(self, balance = 0.0):

        # basic properties
        self.last_buy_time = ("1980/1/1", "00:00") # date, time
        self.last_sell_time = ("1980/1/1", "00:00")
        self.last_transaction_time = ("1980/1/1", "00:00")
        self.balance = balance

        # transaction records and holdings

        self.records = []    # series of SingleDeals
        self.holding = {}      # map of code -> [quantity, average price]
        self.occupied = False   # total balance is occupied
        self.sell_lock = {}    # map of code->volume

    # to update the hold value for all holdings
    def updateValue(self):
        pass

    # for string representative
    def __str__(self):
        return ("Balance =" + str(self.balance) + "\n" \
                + "Holding = " + str(self.holding) + "\n" \
                + "Records =" + str(self.records))

    # reset the daily lock
    def resetDaily(self):
        self.sell_lock = {}

    # evaluate daily worth
    def evalWorth(self, priceDict = {}):
        balance = self.balance
        for code in self.holding.keys():
            if code in priceDict.keys():
                balance += priceDict[code] * self.holding[code] * 100
        return balance

    # update the buy action after the
    def updateRestrictions(self, is_buy = True, date="", time="", code="", volume=0):
        if is_buy:
            if self.last_buy_time[0] != date:
                self.sell_lock = {}
            else:
                if code in self.sell_lock.keys():
                    self.sell_lock[code] += volume
                else:
                    self.sell_lock[code] = volume
            self.last_buy_time = (date, time)
            self.last_transaction_time = (date, time)
        else: # is sell
            self.last_sell_time = (date, time)
            self.last_transaction_time = (date, time)

    #percentage various between 0~1, means the percentage of the balance for this buy
    def buy(self, date="", time="", code="", price=0.1, percentage=1.0):
        # calculate affordable volume
        shou_amount = int((self.balance*percentage/price)//100)
        # not enough money, return
        if shou_amount <= 0:
            return False
        else:
            # update the record amounts
            # reduce balance
            expenditure = price*shou_amount*100
            self.balance -= expenditure
            # set occupied = True if short of money for a share
            if self.balance/price//100 <= 0: self.occupied = True

            dealInfo = SingleDeal(code = code, \
                                  is_buy = True, \
                                  date = date, \
                                  time = time, \
                                  open = price, close = price, high = price, low = price, \
                                  volume = shou_amount, \
                                  total = expenditure, \
                                  zt = False, dt = False)
            self.records.append(dealInfo)

            if code in self.holding.keys():
                # original value = volume * price
                original_value = self.holding[code][0]*100*self.holding[code][1]
                # add the new buy expenditure
                new_value = expenditure + original_value
                # add the new volume
                new_volume = self.holding[code][0]+shou_amount
                # find the new average price of the holdings
                new_price = new_value/new_volume/100.0
                self.holding[code] = [new_volume, new_price]
            else:
                self.holding[code] = [shou_amount, price]

            # note down the sellable restrictions
            self.updateRestrictions(True, date, time, code, shou_amount)

        return True

    def getSellableVolume(self, date="", code=""):
        # no stock to sell
        if code not in self.holding.keys(): return 0
        
        if date == self.last_buy_time[0]:
            if not (code in self.sell_lock.keys()):
                return self.holding[code][0]
            else:
                return self.holding[code][0]-self.sell_lock[code]
        else: # assume the date difference means the new transaction happens later, so all holdings are sellable
            return self.holding[code][0]


    def sell(self, date="", time="", code = "", price=0.1, volume=-1):
        # return False if no such stk in holding, or zero number of holding, or not enough amount
        if not (code in self.holding.keys()): return False

        # obtain sellable volume today
        sellable_volume = self.getSellableVolume(date, code)
        if sellable_volume == 0: return False
        # default to sell all amount
        if volume == -1: volume = sellable_volume

        # Fail if not enough volume to sell
        if sellable_volume < volume: return False

        # sell action
        net_value_origin = self.holding[code][0]*self.holding[code][1]*100
        net_sold = volume * 100 * price

        self.balance += volume * 100 * price
        self.occupied = False

        self.holding[code][0] -= volume
        if self.holding[code][0] == 0:
            del self.holding[code]
        else:
            self.holding[code][1] = (net_value_origin - net_sold)/self.holding[code][0]/100

        dealInfo = SingleDeal(code=code, \
                              # not buy
                              is_buy=False, \
                              date=date, \
                              time=time, \
                              open=price, close=price, high=price, low=price, \
                              volume=volume, \
                              total=volume*100*price, \
                              zt=False, dt=False)
        self.records.append(dealInfo)

        self.updateRestrictions(False, date, time, code, volume)

        return True

class BaseSerialDeal(object):
    'base class to holding stock data and common utilities'

    def __init__(self):
        self.type = 'base'
        self.initialized = False

        self.code = ""
        self.data = None # store whole trade data
        self.date_index = None # store the date index as series
        self.date_list = [] # store date list as unique dates
        self.time_index = None # store time index as series
        self.time_list = [] # store time list as unique time

        self.start_date = None
        self.end_date = None
        self.last_update_time = datetime.datetime.now()

        self.daily_summary = {} # map of: date -> single_deal that to summary every date under current code
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
    'base class to holding stock data and common utilities'

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

        self.data.set_index(index, inplace=True)
        [self.date_list.append(date_str) for date_str in self.data.index.get_level_values('date').unique()]

    def loadCsvNoHead(self, file_path='default.csv', index=["date", "time"]):
        return self.loadCsv(file_path, False, index)

    def loadCsvHasHead(self, file_path='default.csv', index=["date", "time"]):
        return self.loadCsv(file_path, True, index)

    def saveCsvWithHead(self, file_path='default.csv'):
        if isinstance(self.data, pd.DataFrame):
            self.data.to_csv(file_path)

    def isQualifiedCondition1(self, date, time, code, price):
        abs_date_index = self.date_list.index(date)
        if abs_date_index>=3: # has at least 3 days of history data to trace
            date_1 = self.date_list[abs_date_index - 1] # yesterday
            date_2 = self.date_list[abs_date_index - 2] # day before yesterday
            date_3 = self.date_list[abs_date_index - 3] # today -3
            if (self.daily_summary[date_2].close * self.daily_summary[date_2].volume * 100
                    > 10000000): # if volume is right
                # if day before yesterday is Zhang Ting,  and yesterday Shang Zhang, and today Xia Die
                return (zhangTing(self.daily_summary[date_3].close, self.daily_summary[date_2].close) \
                        and shangZhang(self.daily_summary[date_2].close, self.daily_summary[date_1].close, 3.0) \
                        and xiaDie(self.daily_summary[date_1].close, price, -3.0)) # if today xia Die -3% or more

        return False

    def isQualifiedCondition2(self, date, time, code, price):
        abs_date_index = self.date_list.index(date)
        if abs_date_index >= 4:
            date_1 = self.date_list[abs_date_index - 1] # today - 1
            date_2 = self.date_list[abs_date_index - 2] # today - 2
            date_3 = self.date_list[abs_date_index - 3] # today - 3
            date_4 = self.date_list[abs_date_index - 3] # today - 4

            if (self.daily_summary[date_3].close * self.daily_summary[date_3].volume * 100 > 10000000):
                # if day -3 is Zhangting, and Xia Die for 2 days, and now Xia Die again
                return (zhangTing(self.daily_summary[date_4].close, self.daily_summary[date_3].close) \
                    and xiaDie(self.daily_summary[date_3].close, self.daily_summary[date_2].close) \
                    and xiaDie(self.daily_summary[date_2].close, self.daily_summary[date_1].close) \
                    and xiaDie(self.daily_summary[date_1].close, price, -1))
        return False

    def isQualifiedSellCondition1(self, date, time, code, price):
        # over 5 days holding
        abs_date_index = self.date_list.index(date)
        if (self.dealer.last_buy_time[0] in self.date_list) \
            and (abs_date_index - self.date_list.index(self.dealer.last_buy_time[0]) >= 7):
            return True

        # over 8% profit
        if code in self.dealer.holding.keys():
            if price/self.dealer.holding[code][1] > 1.08: # more than 10% profit
                return True

        # to add more sell conditions, and add one records in dealer for current cost to calculate the gain

        return False

    def dealActions(self, date, time, code, price):
        if self.isQualifiedCondition1(date, time, code, price) \
            or self.isQualifiedCondition2(date, time, code, price):
            self.dealer.buy(date, time, code, price)
        if self.isQualifiedSellCondition1(date, time, code, price):
            self.dealer.sell(date, time, code, price)

    def traverseDaily(self, date = "", code = "", determine_func = None, determine_param = ()):

        if self.data is None:
            pass
        # initialize daily data and index
        daily_data = self.data.loc[date]
        daily_index = self.date_list.index(date)

        # initialize daily deal status for loop
        daily_deal_status = SingleDeal()
        deal_open = daily_data.iloc[0]
        daily_deal_status.setDeal(deal_open) # set to first transaction numbers
        daily_deal_status.code = code
        daily_deal_status.volume = 0
        daily_deal_status.total = 0

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
            self.dealActions(date, time, code, current_deal.close)
            # ----------------------------------------------------------------

        # store the daily deal status to the records
        self.daily_summary[date] = daily_deal_status


        # determine_func(*determine_param)
        #
        # for index, item in enumerate(daily_data.index):
        #     print (index, "--> ", daily_data.loc[item])

    # dummy strategy
    def determineStrategy(self, determine_param = []):
        print (determine_param.index)

    def traverseAll(self, code="", determine_func=None, determine_param=()):
        for index, this_date in enumerate(self.date_list):
            self.traverseDaily(this_date, code, determine_func, determine_param)

def dailyTest(filename=""):
    serial_data = TimeSerialDeal()
    serial_data.loadCsvHasHead(filename)
    code = "000000" if len(filename) < 10 else filename[-10:-4]  # remove .csv

    serial_data.dealer.balance = 100000

    for date in serial_data.date_list:
        serial_data.traverseDaily(date, code)

    final_close = serial_data.daily_summary[serial_data.date_list[-1]].close
    net_value = serial_data.dealer.evalWorth({code: final_close})
    print(code + "----------------------------")
    print(net_value)
    print(serial_data.dealer)


# convert the tushare data in 5 minutes to the storage format
# that can be merged later
# input: tushare 5 minute result
# output: result with head in given format
def processDailyData(tu_inputfile="", defined_exportfile=""):
    df = pd.read_csv(tu_inputfile)
    if len(df.index) == 0: pass

    def splitDate(x):
        return x.split(" ")[0]

    def splitTime(x):
        return x.split(" ")[1]

    series_date = df.date.apply(splitDate)
    series_time = df.date.apply(splitTime)

    df["date"] = series_date
    df["time"] = series_time

    df.set_index(["date", "time"], inplace=True)
    print (df)
    df.to_csv(defined_exportfile)

def updateStockBasics(filename="stock_basics.json", withDate=False):
    if withDate:
        date_postfix = "_"+time.strftime("%Y%m%d")
        filename = ".".join([".".join(filename.split(".")[:-1])+date_postfix, filename.split(".")[-1]])

    stock_basics = ts.get_stock_basics()
    stock_basics.to_json(filename)
    # code_series = list(stock_basics.index)
    # with open(filename, 'w', encoding='utf-8') as f:
    #     json.dump(code_series, f)

def getStockBasics(filename="stock_basics.json"):
    return pd.read_json(filename)


if __name__ == '__main__':

    file_dir = os.path.abspath("D:\stock\YiQi_new")
    files = os.listdir(file_dir)

    print(ts.get_stock_basics())

    updateStockBasics()
    basics = getStockBasics()
    print(basics)
    print(basics.index)

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        for file in files:
            file_name = os.path.join(file_dir, file)
            executor.submit(dailyTest, file_name)


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





