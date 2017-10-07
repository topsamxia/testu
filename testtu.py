# coding=utf-8
import tushare as ts
import numpy as np
import pandas as pd

import time

print (time.strftime("%Y%m%d-%H:%M:%S"))

import datetime

i = datetime.datetime.now()
print ("当前的日期和时间是 %s" % i)
# print (u"ISO格式的日期和时间是 %s" % i.isoformat() )
# print (u"当前的年份是 %s" %i.year)
# print (u"当前的月份是 %s" %i.month)
# print (u"当前的日期是  %s" %i.day)
# print (u"dd/mm/yyyy 格式是  %s/%s/%s" % (i.day, i.month, i.year) )
# print (u"当前小时是 %s" %i.hour)
# print (u"当前分钟是 %s" %i.minute)
# print (u"当前秒是  %s" %i.second)


starting_date = '20160701'
sample_numpy_data = np.array(np.arange(24)).reshape((6, 4))
dates_index = pd.date_range(starting_date, periods=6)
sample_df = pd.DataFrame(sample_numpy_data, index=dates_index, columns=list('ABCD'))

sample_df_2 = sample_df.copy()
sample_df_2['Fruits'] = ['apple', 'orange', 'banana', 'strawberry', 'blueberry', 'pineapple']

sample_series = pd.Series([1, 2, 3, 4, 5, 6], index=pd.date_range(starting_date, periods=6))
sample_df_2['Extra Data'] = sample_series * 3 + 1

second_numpy_array = np.array(np.arange(len(sample_df_2))) * 100 + 7
sample_df_2['G'] = second_numpy_array

browser_index = ['Firefox', 'Chrome', 'Safari', 'IE10', 'Konqueror']

browser_df = pd.DataFrame({
    'http_status': [200, 200, 404, 404, 301],
    'response_time': [0.04, 0.02, 0.07, 0.08, 1.0]},
    index=browser_index)

new_index = ['Safari', 'Iceweasel', 'Comodo Dragon', 'IE10', 'Chrome']
browser_df_2 = browser_df.reindex(new_index)

browser_df_3 = browser_df_2.dropna(how='any')
browser_df_2.fillna(value=-0.05555)

pd.set_option('display.precision', 2)

sample_df_2.apply(np.cumsum, axis=0)

# concat
pieces = [sample_df_2[:2], sample_df_2[2:4], sample_df_2[4:]]
new_list = pieces[0], pieces[2]
pd.concat(new_list)

# append returns a new object
new_last_row = sample_df_2.iloc[2]
sample_df_2.append(new_last_row)

# merge returns the database style join
left = pd.DataFrame({'my_key': ['K0', 'K1', 'K2', 'K3'],
                     'A': ['A0', 'A1', 'A2', 'A3'],
                     'B': ['B0', 'B1', 'B2', 'B3']})
right = pd.DataFrame({'my_key': ['K0', 'K1', 'K2', 'K3'],
                      'C': ['C0', 'C1', 'C2', 'C3'],
                      'D': ['D0', 'D1', 'D2', 'D3']})

result = pd.merge(left, right, on='my_key')

file_name_string_csv = 'C:/~data/pyproj/Ex_Files_Pandas_Data/Exercise Files/02_07/Final/EmployeesWithGrades.csv'
employees_df = pd.read_csv(file_name_string_csv)

print (ts.__version__)

print (sample_df['C'])
print (sample_df[1:4])
print (sample_df_2)

# resample
my_index = pd.date_range('9/1/2016', periods=9, freq='min')
my_series = pd.Series(np.arange(9), index=my_index)
my_series.resample('3min').sum()
my_series.resample('3min', label='right').sum()
my_series.resample('3min', label='right', closed='right').sum()
my_series.resample('30S').asfreq()[0:5]


def custom_arithmetic(array_like):
    temp = 3 * np.sum(array_like) + 5
    return temp


my_series.resample('3min').apply(custom_arithmetic)

# series
my_simple_series = pd.Series(np.random.randn(5), index=['a', 'b', 'c', 'd', 'e'])
print(my_simple_series.index)

my_dictionary = {'a': 45., 'b': -19.5, 'c': 4444}
my_second_series = pd.Series(my_dictionary)

my_simple_series = pd.Series(np.random.randn(5))
print (my_second_series['b'])
print (my_second_series.get('a'))
print (my_second_series.get('f'))

# scalar must be provided with an index
my_scalar_series = pd.Series(5., index=['a', 'b', 'c', 'd', 'e'])
print (my_scalar_series)


def multiply_by_ten(input_element):
    return input_element * 10.0


my_series.map(multiply_by_ten)

series_of_strings = pd.Series(['A', 'B', 'C', 'Aaba', 'Baca', np.nan, 'CABA', 'dog', 'cat'])
print (series_of_strings.str.lower())

# date arithmetic
from datetime import datetime

now = datetime.now()

print (now.year, now.month, now.day)

delta = now - datetime(2001, 1, 1)

print (delta.days, delta.seconds, delta.microseconds)
print (pd.Timedelta('4 days 7 hours'))
print (pd.Timedelta(days=1, seconds=1))

us_memorial_day = datetime(2016, 5, 30)
print(us_memorial_day)
us_labor_day = datetime(2016, 9, 5)
print(us_labor_day)
us_summer_time = us_labor_day - us_memorial_day
print(us_summer_time)
type(us_summer_time)

us_summer_time_range = pd.date_range(us_memorial_day, periods=us_summer_time.days, freq='D')
us_summer_time_time_series = pd.Series(np.random.randn(us_summer_time.days), index=us_summer_time_range)
print (us_summer_time_time_series.tail())

# create various data type
my_dictionary = {'a': 45., 'b': -19.5, 'c': 4444}
print(my_dictionary.keys())
print(my_dictionary.values())

my_dictionary_df = pd.DataFrame(my_dictionary, index=['first', 'again'])
print (my_dictionary_df)

cookbook_df = pd.DataFrame({'AAA': [4, 5, 6, 7], 'BBB': [10, 20, 30, 40], 'CCC': [100, 50, -30, -50]})

# dictionary with series as value
series_dict = {'one': pd.Series([1., 2., 3.], index=['a', 'b', 'c']),
               'two': pd.Series([1., 2., 3., 4.], index=['a', 'b', 'c', 'd'])}
series_df = pd.DataFrame(series_dict)
print (series_df)

produce_dict = {'veggies': ['potatoes', 'onions', 'peppers', 'carrots'],
                'fruits': ['apples', 'bananas', 'pineapple', 'berries']}
print (pd.DataFrame(produce_dict))

data2 = [{'a': 1, 'b': 2}, {'a': 5, 'b': 10, 'c': 20}]
print (pd.DataFrame(data2))

sample_multi_index_df = pd.DataFrame({('a', 'b'): {('A', 'B'): 1, ('A', 'C'): 2},
                                      ('a', 'a'): {('A', 'C'): 3, ('A', 'B'): 4},
                                      ('a', 'c'): {('A', 'B'): 5, ('A', 'C'): 6},
                                      ('b', 'a'): {('A', 'C'): 7, ('A', 'B'): 8},
                                      ('b', 'b'): {('A', 'D'): 9, ('A', 'B'): 10}})
print (sample_multi_index_df)

cookbook_df = pd.DataFrame({'AAA': [4, 5, 6, 7], 'BBB': [10, 20, 30, 40], 'CCC': [100, 50, -30, -50]})
print (cookbook_df['BBB'] * cookbook_df['CCC'], cookbook_df['AAA'])

del cookbook_df['BBB']
print (cookbook_df)

last_column = cookbook_df.pop('CCC')

# add a column
cookbook_df['DDD'] = [32, 21, 43, 'hike']

# insert a column
cookbook_df.insert(1, "new column", [3, 4, 5, 6])
print (cookbook_df)

# indexing and selection
df = pd.DataFrame(np.random.randn(10, 4), columns=['A', 'B', 'C', 'D'])
df2 = pd.DataFrame(np.random.randn(7, 3), columns=['A', 'B', 'C'])
sum_df = df + df2
print (sum_df)

np.exp(sum_df)

# index
produce_dict = {'veggies': ['potatoes', 'onions', 'peppers', 'carrots'],'fruits': ['apples', 'bananas', 'pineapple', 'berries']}
produce_df = pd.DataFrame(produce_dict)
print (produce_df)
print (produce_df[['fruits', 'veggies']])
print (produce_df.iloc[2])
print (produce_df.iloc[0:2])
print (produce_df.iloc[:-2])

df = pd.DataFrame(np.random.randn(10, 4), columns=['A', 'B', 'C', 'D'])
df2 = pd.DataFrame(np.random.randn(7, 3), columns=['A', 'B', 'C'])
sum_df = df + df2
print (sum_df>0)
print (sum_df[sum_df>0])
mask = sum_df['B'] < 0
print (mask)

i=0
for i in range(mask.size):
    mask[i] = not(mask[i])
print (mask)
print (sum_df[mask])


print (produce_df.isin(['apples', 'onions']))
print (produce_df.where(produce_df > 'k'))

# universal
print (sum_df.T)
print (sum_df.values)
print (np.transpose(sum_df.values))

A_df = pd.DataFrame(np.arange(15).reshape((3,5)))
B_df = pd.DataFrame(np.arange(10).reshape((5,2)))
print (A_df, B_df, A_df.dot(B_df))

C_Series = pd.Series(np.arange(5, 10))
print (C_Series, C_Series.dot(C_Series))

# panel
from pandas_datareader import data, wb
import datetime
pd.set_eng_float_format(accuracy=2, use_eng_prefix=True)
my_first_panel = pd.Panel(np.random.randn(2, 5, 4),
                          items=['Item01', 'Item02'],
                          major_axis=pd.date_range('9/6/2016', periods=5),
                          minor_axis=['A', 'B', 'C', 'D'])

dictionary_of_data_frames = {'Item1' : pd.DataFrame(np.random.randn(4, 3)),
                             'Item2' : pd.DataFrame(np.random.randn(4, 2))}
my_dictionary_panel = pd.Panel(dictionary_of_data_frames)

oriented_panel = pd.Panel.from_dict(dictionary_of_data_frames, orient='minor')

start = datetime.datetime(2017, 9, 4)
end = datetime.datetime(2017, 9, 5)
pdata = pd.Panel(dict((stk, data.DataReader("F", 'yahoo', start, end)) for stk in ['AAPL', 'GOOG', 'MSFT', 'ADSK']))

pdata = pdata.swapaxes('items', 'minor')
print (pdata)

pdata.ix[:, '7/12/2016', :]

# convert to multi-index frame
stacked = pdata.ix[:, '6/30/2016':, :].to_frame()
print(type(stacked))
print (stacked)

# convert back to a panel
print (stacked.to_panel())

import matplotlib.pyplot as plt
plt.style.use('ggplot')

mu, sigma = 100, 15
data_set = mu + sigma * np.random.randn(10000)

# the histogram of the data
n, bins, patches = plt.hist(data_set, 50, normed=1, facecolor='g', alpha=0.75)

plt.xlabel('Smarts')
plt.ylabel('Probability')
plt.title('Histogram of IQ')
plt.text(60, .025, r'$\mu=100,\ \sigma=15$')
plt.axis([40, 160, 0, 0.03])
plt.grid(True)
plt.show()

