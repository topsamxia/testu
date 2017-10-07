import pandas as pd
import os

def pre_process(df):
    df.set_index('date', inplace=True)
    df.drop(df.columns[0], axis=1, inplace=True)
    return df

dir = os.getcwd()+os.path.sep+"20170912"
list = os.listdir(dir)
index = 0
for file in list:
    print (index)
    index = index + 1
    try:
        pre_process(pd.read_csv(dir+os.path.sep+file)).to_csv(dir+os.path.sep+file)
    except:
        print (file)
