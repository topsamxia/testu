#coding=utf-8
# import numpy as np
import pandas as pd
import os, time

time_str_begin = time.strftime("%H%M%S")

old_dir= os.path.abspath("20170910")
new_dir = os.path.abspath("aa20170913")

new_files = os.listdir(new_dir)
old_files = os.listdir(old_dir)

for new_file in new_files:
    old_size, new_size = 0, 0
    if new_file in old_files:
        try:
            new_pd = pd.read_csv(os.path.join(new_dir, new_file), index_col='date')
            old_pd = pd.read_csv(os.path.join(old_dir, new_file), index_col='date')
            # print (new_pd.head(2), old_pd.head(2))
            # print (new_pd.tail(2), old_pd.tail(2))

            new_pd = old_pd.append(new_pd)
            new_pd.drop_duplicates(inplace=True)
            new_pd.to_csv(os.path.join(new_dir, new_file))
            #print ('merge file', new_file, 'size:', len(old_pd.index), '->', len(new_pd.index))
        except:
            print ("newfile error")
    #print ('new file', new_file)

time_str_end = time.strftime("%H%M%S")

print (str(int(time_str_begin) - int(time_str_end)))