#coding=utf-8
# import numpy as np
import pandas as pd
import os, time
import concurrent.futures
import multiprocessing



old_dir= os.path.abspath("D:\stock\merge_data\data_base")
new_dir = os.path.abspath("D:\stock\merge_data\data_new")
target_dir = os.path.abspath("D:\stock\merge_data\data_result")

new_files = os.listdir(new_dir)
old_files = os.listdir(old_dir)


def merge_data(new_file, old_file, target_file):
    try:
        new_pd = pd.read_csv(new_file, index_col='date')
        old_pd = pd.read_csv(old_file, index_col='date')
        new_pd = old_pd.append(new_pd)
        new_pd.drop_duplicates(inplace=True)
        new_pd.to_csv(target_file)
        return True
    except:
        print("error: "+new_file)
        return False


def printMsg(x, y):
    print ("Execute: "+str(x) + ", " + str(y))


if __name__ == '__main__':
    # multiprocessing.freeze_support()

    result = []

    time_str_begin = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:

        for new_file in new_files:
            if new_file in old_files:
                # obj = executor.submit(printMsg, new_files.index(new_file), new_files.index(new_file)+1)
                # result.append(obj)
                obj = executor.submit(merge_data,
                                      os.path.join(new_dir, new_file),
                                      os.path.join(old_dir, new_file),
                                      os.path.join(target_dir, new_file))
                result.append (obj)
                print(new_file)

    for ojb in result:
        print(obj.result())

    # print ([obj.result() for obj in result])
    print(time.time()-time_str_begin)
