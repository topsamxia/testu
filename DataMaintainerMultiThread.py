import StkFX
import os
import threadpool
import datetime
import time, random
import pandas as pd

def mergeData(file_new = "", file_base = "", file_target = ""):
    try:
        if file_new == "" and file_target == "": pass
        if file_new == "" and file_base == "": pass

        # if both files are valid
        if file_new != "" and file_base != "":
            time_deal_base = StkFX.TimeSerialDeal()
            time_deal_new = StkFX.TimeSerialDeal()
            time_deal_base.loadCsvHasHead(file_base)
            time_deal_new.loadCsvHasHead(file_new)
            time_deal_new.data = time_deal_base.data.append(time_deal_new.data).drop_duplicates()
            time_deal_new.data.to_csv(file_target)

        else: # one of the file names is empty, then pure copy
            file_name = file_new if file_new != "" else file_base
            time_deal_new = StkFX.TimeSerialDeal()
            time_deal_new.loadCsvHasHead(file_name)
            time_deal_new.data.to_csv(file_target)
    except:
        print ("something is wrong here: "+file_new)

def mergeData1(file_new = "", file_base = "", file_target = ""):
    try:
        if file_new == "" and file_target == "": pass
        if file_new == "" and file_base == "": pass

        # if both files are valid
        if file_new != "" and file_base != "":

            time_deal_base = pd.read_csv(file_base)
            time_deal_base.set_index(["date", "time"], inplace=True)
            time_deal_new = pd.read_csv(file_new)
            time_deal_new.set_index(["date", "time"], inplace=True)

            time_deal_new = time_deal_base.append(time_deal_new).drop_duplicates()
            time_deal_new.to_csv(file_target)

        else: # one of the file names is empty, then pure copy
            file_name = file_new if file_new != "" else file_base
            time_deal_new = pd.read_csv(file_new)
            time_deal_new.set_index(["date", "time"], inplace=True)
            time_deal_new.to_csv(file_target)
    except:
        print ("something is wrong here: "+file_new)

def testFunction(file_new = "", file_base = "", file_target = ""):
    timeduration = int(random.random() * 10.0)
    time.sleep(timeduration)
    print (file_new + file_base + file_target)

if __name__ == '__main__':
    base_dir = os.path.abspath("D:\stock\stk_5F_2015_new")
    base_files = os.listdir(base_dir)

    new_dir = os.path.abspath('D:\stock\stk_5F_2016_new')
    new_files = os.listdir(new_dir)

    target_dir = os.path.abspath('C:\~data\stock')

    files_not_found = []

    # prepare argument list
    args_list = []
    for new_file in new_files:
        new_file_full = os.path.join(new_dir, new_file)
        base_file_full = ""
        if new_file in base_files:
            base_file_full = os.path.join(base_dir, new_file)
        target_file_full = os.path.join(target_dir, new_file)

        args_list.append(([new_file_full, base_file_full, target_file_full], None))

    pool = threadpool.ThreadPool(2)

    print (datetime.datetime.now())
    requests = threadpool.makeRequests(mergeData1, args_list)
    [pool.putRequest(req) for req in requests]
    pool.wait()
    print (datetime.datetime.now())





