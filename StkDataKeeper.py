import pandas as pd
import tushare as ts
import pyecharts as pec
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
        except:
            return False

    # load data
    def load_data(self, filename=""):
        try:
            with open(filename, 'r') as f:
                self.stk_code, self.seqential_status_map = pickle.load(filename)
            return True
        except:
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
        self.stock_basics=None
        self.stock_list=[]
        if code_list_file != "":
            self.load_stock_list(code_list_file)

        # to implement - load data from folder

    def load_stock_list(self, code_list_file=""):
        if code_list_file != "":
            self.stock_basics = self.get_stock_basics_file(code_list_file)
        else:
            self.stock_basics = self.get_stock_basics_tu()
        self.stock_list = self.stock_basics.index.tolist()



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
    def read_csv(self, filename="", with_head=True, head_names=[], index="", ):
        try:
            # if the original data has no index definition, then needs to set the index, else just honor the existing index
            if with_head:
                result_df = pd.read_csv(filename, dtype={"code":str}, encoding="utf-8")

            else:
                if head_names == []:
                    head_names = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'total']
                result_df = pd.read_csv(filename, names=head_names, dtype={"code":str}, encoding="utf-8")
            if index != "":
                result_df.set_index(index, inplace=True)

        except:
            return None

        return result_df


    # date             open   close  high   low    volume  code
    # 2014-11-17 14:55 21.946 22.305 22.465 21.047 33923.0 300191
    #
    # return formatted date & time indexed dataframe, with total estimation
    def read_csv_tu_5min(self, filename=""):
        try:
            df = self.read_csv(filename, True)
            self.splitDateTime(df)
            # simulate the total with calculation
            df["total"] = df["volume"] * 100 * (df["open"] + df["close"] + df["high"] + df["low"]) / 4
            df["date"] = [c.replace('-', '/') for c in df["date"]]  # this doesnt' work - df["date"].replace('-', '/')
            df["code"] = [("0"*(6-len(c))+c) for c in df["code"]] # add missing "0" if any

            df.set_index(["date", "time"], inplace=True)

            # df.index = pd.MultiIndex.from_tuples([(x[0].replace('/', '-'), x[1]) for x in df.index])
        except:
            return None

        return df


    # date       open   close  high   low    volume  code
    # 2014-11-17 21.946 22.305 22.465 21.047 33923.0 300191
    #
    # return: date indexed DataFrame
    def read_csv_tu_daily(self, filename=""):
        try:
            df = self.read_csv(filename, True, index="date")
            df["total"] = df["volume"] * 100 * (df["open"] + df["close"] + df["high"] + df["low"]) / 4
        except:
            return None

        return df


    # date time open high low close volume total
    # 2015/1/5 9:35 16.47 16.47 16.18 16.2 2355 3838604
    def read_csv_hist_5min(self, filename=""):
        df = self.read_csv(filename, with_head=True, index=["date", "time"])
        return df


    # get the stock list from online interface, and store it to given file
    # return DataFrame
    def get_stock_basics_tu(self, filename=""):
        if not (self.stock_basics is None):
            return self.stock_basics
        else:
            self.stock_basics = ts.get_stock_basics()
            if filename != "":
                self.stock_basics.to_csv(filename, encoding="utf-8")
            return self.stock_basics


    def get_stock_basics_file(self, filename=""):
        try:
            self.stock_basics = self.read_csv(filename, True)
            self.stock_basics.set_index("code", inplace=True)
            return self.stock_basics
        except:
            return None

    def get_stock_outstanding(self, stk_code):
        try:
            if self.stock_basics is None:
                self.load_stock_list()
            return self.stock_basics.loc[stk_code].outstanding * 100000000 / 100
        except:
            return 0.0

    # merge data for data maintenance
    # if daily data then index = ["date"]
    # if time transaction data then index = ["date", "time"]
    def merge_csv(self, base_file="", new_file="", target_file="", index=["date", "time"]):
        try:
            new_pd = self.read_csv(new_file, index=index)
            old_pd = self.read_csv(base_file, index=index)

            new_pd = old_pd.append(new_pd)
            new_pd = self.drop_duplicate(new_pd)
            new_pd.to_csv(target_file)

            return True

        except:
            print("error: " + new_file)
            return False


    def merge_df(self, df_base, df_new, filename=""):
        try:
            merged_df = df_base.append(df_new)
            merged_df = self.drop_duplicate(merged_df)
            merged_df.to_csv(filename)
        except:
            return False

        return True


    # add the code column according to the filename. Maintenance for the purchased data
    def add_code_single(self, input_filename="", target_filename="", stock_only=False, stock_record_file="stock_basics.csv"):
        try:
            stk_keeper = sd.StkDataKeeper()
            code = input_filename.split(".")[0][-6:]
            if stock_only:
                stock_df = stk_keeper.load_stock_list(stock_record_file)
                stock_list = stock_df.index.tolist()
                if code not in stock_list:
                    return ("cannot find this stock: " + input_filename)

            df = stk_keeper.read_csv_hist_5min(input_filename)
            df["code"] = code
            df.to_csv(target_filename, encoding="utf-8")
            return "done"

        except:
            return ("fail: " + input_filename)


    # add code column for all files under given folder. Maintenance for the purchased data
    def add_code_folder(self, base_dir="", target_dir=""):

        file_list = os.listdir(base_dir)

        result = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=15) as executor:
            for file_name in file_list:
                base_file = os.path.join(base_dir, file_name)
                target_file = os.path.join(target_dir, file_name)

                obj = executor.submit(self.add_code_single, base_file, target_file)
                result.append(obj)

        for obj in result:
            if obj.result() != "done":
                print(obj.result())


    def drop_duplicate(self, df):
        return df[~df.index.duplicated(keep='last')]


    def merge_hist_with_daily_single(self, hist_file="", daily_file="", target_file="", isIncrementalTu=True):
        inline_debug = False
        if target_file=="": return [hist_file, daily_file, target_file]

        try:
            # if only one source is provided, then save it directly
            if hist_file=="" or daily_file=="":
                if daily_file!="":
                    if isIncrementalTu:
                        df = self.read_csv_tu_5min(daily_file)
                    else:
                        df = self.read_csv_hist_5min(daily_file)
                else:
                    df = self.read_csv_hist_5min(hist_file)
                df.to_csv(target_file)

            # else all three inputs are ready
            else:
                hist_df = self.read_csv_hist_5min(hist_file)
                if isIncrementalTu:
                    daily_df = self.read_csv_tu_5min(daily_file)
                else:
                    daily_df = self.read_csv_hist_5min(daily_file)

                if inline_debug:# debug code goes here
                    hist_df.to_csv("hist_df.csv")
                    daily_df.to_csv("daily_df.csv")

                new_df = hist_df.append(daily_df)

                if inline_debug:
                    print (len(hist_df.index), len(daily_df.index), len(new_df.index))
                    new_df.to_csv(target_file+".csv")

                new_df = self.drop_duplicate(new_df)
                # new_df.drop_duplicates(inplace=True)

                if inline_debug:
                    print(len(new_df.index))

                new_df.to_csv(target_file)
                return []
        except:
            return [hist_file, daily_file, target_file]

    # isIncrementalSource = True means the source is from TuShare so the format is converted
    # isIncrementalSource = False means the source is also in format of history, which is data merge operation
    def merge_hist_with_daily_folder(self, hist_dir, daily_dir, target_dir, isIncrementalSource=True):

        if self.stock_list == []:
            self.load_stock_list()

        hist_file_list = os.listdir(hist_dir)
        daily_files_list = os.listdir(daily_dir)

        hist_map = {}
        for hist_file in hist_file_list:
            code = hist_file[-10:-4]
            if code in self.stock_list:
                if ((is_SH(code) and hist_file[:2]=="SH")
                    or (is_SZ(code) and hist_file[:2]=="SZ")):
                    hist_map[code] = hist_file
                # else is something else

        daily_map = {}
        for daily_file in daily_files_list:
            code = daily_file[-10:-4]
            daily_map[code] = daily_file

        results = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=15) as executor:
            for code in daily_map.keys():
                daily_file = os.path.join(daily_dir, daily_map[code])
                prefix = "SH" if is_SH(code) else "SZ"
                target_file = os.path.join(target_dir, prefix+code+'.csv')

                if code in hist_map.keys():
                    hist_file = os.path.join(hist_dir, hist_map[code])
                else:
                    hist_file = ""

                # self.merge_hist_with_daily_single(hist_file, daily_file, target_file)
                obj = executor.submit(self.merge_hist_with_daily_single, hist_file, daily_file, target_file, isIncrementalSource)
                results.append(obj)


        for obj in results:
            if obj.result() != []:
                print(obj.result())

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

class StkDiagram(object):
    'Utility class that accepts the dataframe and paint set of klines'

    def __init__(self):
        pass

    # Known limitation - cannot paint double axis diagrams
    def paint_grid(self, diagram_coll=[], column=3, margin = 3.0, unit_width=600, unit_height=400):

        if len(diagram_coll) == 0:
            return None
        row = len(diagram_coll) // column + (1 if len(diagram_coll) % column != 0 else 0)

        # every cell is grid_top, grid_bottom, grid_left, grid_right
        positioning = []
        pos_unit = 100 / column
        height_unit = 100 / row
        for i_row in range(row):
            for j_col in range(column):
                positioning.append(["{0:.0f}%".format(height_unit * i_row + margin),
                                    "{0:.0f}%".format(100 - height_unit * (i_row + 1) + margin),
                                    "{0:.0f}%".format(pos_unit * j_col + margin),
                                    "{0:.0f}%".format(100 - pos_unit * (j_col + 1) + margin)])

        grid = pec.Grid(width=unit_width*column, height=unit_height*row)
        for i, diagram in enumerate(diagram_coll):
            grid.add(diagram,
                     grid_top=positioning[i][0],
                     grid_bottom=positioning[i][1],
                     grid_left=positioning[i][2],
                     grid_right=positioning[i][3])
        grid.render("C:\\Users\\samx\\PycharmProjects\\index.html")
        return grid

    def paint_page(self, diagram_coll=[]):
        page = pec.Page()
        [page.add(diagram) for diagram in diagram_coll]
        return page


    def paint_kline(self, sk_code="300191", target_file="", tail=24, no_volume=False):
        df = None
        try:
            df = ts.get_k_data(sk_code)
            df.set_index("date", inplace=True)
            if tail != 0:
                df = df[-(tail):]
        except:
            if df == None: return None

        v_label = []
        v_kline = []
        v_line = []
        v_volume = []
        for index in df.index:
            v_kline.append([df.loc[index].open,
                            df.loc[index].close,
                            df.loc[index].low,
                            df.loc[index].high])
            v_label.append(index)
            v_volume.append(df.loc[index].volume)
            v_line.append(df.loc[index].close)

        kline = pec.Kline(sk_code)
        kline.add("", v_label, v_kline)
        # kline.render("C:\\Users\\samx\\PycharmProjects\\kline.html")
        line = pec.Line(sk_code)
        line.add("", v_label, v_line, yaxis_min=min(v_line))
        #line.render("C:\\Users\\samx\\PycharmProjects\\line.html")
        bar = pec.Bar(sk_code)
        bar.add("", v_label, v_volume, yaxis_max=max(v_volume)*5)
        #bar.render("C:\\Users\\samx\\PycharmProjects\\bar.html")

        overlap = pec.Overlap()
        overlap.add(line)
        overlap.add(kline)
        if not no_volume:
            overlap.add(bar, is_add_yaxis=True, yaxis_index=1)
        if target_file != "":
            overlap.render(target_file)

        return overlap

def is_SH(code=""):
    if len(code) != 6: return False
    return True if code[:2]=="60" else False

def is_SZ(code=""):
    if len(code) != 6: return False
    return True if code[:2]=="00" or code[:3]=="300" else False

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

def test_pickle_help_data():
    data_keeper = StkDataKeeper()
    # df = data_keeper.read_csv("stksample\\20170929_150000.csv", index="code")
    # print(df)
    df = data_keeper.read_csv_hist_5min("stksample//SZ300191.csv")
    print(df)

    helper_data = StkHelperData()
    helper_data.calculate_data(df)

    p1 = pickle.dumps(helper_data)
    helper_data_new = pickle.loads(p1)
    print(helper_data_new.seqential_status_map.keys())

def test_csv_readers():

    sample_dir = os.path.abspath("stksample")

    data_keeper = StkDataKeeper()
    test_file = os.path.join(sample_dir, "SZ300191.csv")
    df = data_keeper.read_csv(test_file, index="date")
    df = df.set_index("time", append=True)
    print(df.head())

    test_file = os.path.join(sample_dir, "5min_300191.csv")
    df = data_keeper.read_csv_tu_5min(test_file)
    print(df.head())

    test_file = os.path.join(sample_dir, "day_300191.csv")
    df = data_keeper.read_csv_tu_daily(test_file)
    print(df.head())

    test_file = os.path.join(sample_dir, "SZ300191.csv")
    df = data_keeper.read_csv_hist_5min(test_file)
    print(df.head())

def test_merge_df():

    sample_dir = os.path.abspath("stksample")

    data_keeper = StkDataKeeper()
    test_file_base = os.path.join(sample_dir, "SZ300191.csv")
    test_file_new = os.path.join(sample_dir, "5min_300191.csv")
    df_base = data_keeper.read_csv_hist_5min(test_file_base)
    df_new = data_keeper.read_csv_tu_5min(test_file_new)
    data_keeper.merge_df(df_base, df_new, "result.csv")

    df = data_keeper.read_csv_hist_5min("result.csv")
    print(df.head(), df.tail())

    data_keeper.merge_df(df_new, df_base, "result.csv")
    df = data_keeper.read_csv_hist_5min("result.csv")
    print(df.head(), df.tail())

def test_stk_stock_basics():
    stk_keeper = StkDataKeeper()
    df = stk_keeper.get_stock_basics_tu("stock_basics.csv")
    print(df.head(), df.tail())
    df1 = stk_keeper.get_stock_basics_file("stock_basics.csv")
    print(df1.head(), df1.tail())

def test_stk_daily_batch_merge():
    stk_keeper = StkDataKeeper(code_list_file="stock_basics.csv")
    hist_dir = "D://stock//stk_new"
    daily_dir = "D://stock//accumulated_data//20170930//fivemin"
    target_dir = "D://stock//stk_new"
    now = datetime.datetime.now()

    stk_keeper.merge_hist_with_daily_folder(hist_dir, daily_dir, target_dir)
    time.sleep(3)
    print((datetime.datetime.now() - now))

def test_stk_diagram():
    stk_diagram = StkDiagram()
    stk_list = ['000418', '002194', '002846', '300176', '300425', '300707']
    diagram_list = []
    for stk in stk_list:
        kline = stk_diagram.paint_kline(stk, no_volume=True)
        # kline.render(stk+".html")
        diagram_list.append(kline)
    grid = stk_diagram.paint_grid(diagram_list)
    grid.render()

def set_pandas_format():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

if __name__ == "__main__":

    set_pandas_format()

    # mark_done_get = datetime.datetime.now()
    #
    # hist_dir = os.path.abspath("D:\stock\stk_new_update")
    # daily_dir = os.path.abspath("D:\stock\stk_update_incremental")
    # target_dir = os.path.abspath("D:\stock\stk_new_merge")
    #
    # stk_keeper = StkDataKeeper()
    # stk_keeper.load_stock_list()
    # stk_keeper.merge_hist_with_daily_folder(hist_dir, daily_dir, target_dir, False)
    # print("Refresh data in: " + str(datetime.datetime.now() - mark_done_get))

    stk_keeper = StkDataKeeper()
    print(stk_keeper.get_stock_basics_tu().head())
    print(stk_keeper.stock_basics.tail())