import pandas as pd
import multiprocessing, datetime, concurrent.futures
import os
import tushare
import StkDataKeeper as sd

# add the code column according to the filename
def add_code_single(input_filename="", target_filename="", stock_only=False, stock_record_file="stock_basics.csv"):
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


if __name__ == "__main__":
	base_dir = os.path.abspath("D://stock//stk_5F_2015_new")
	target_dir = os.path.abspath("D://stock//stk_5F_2015_new_code")
	file_list = os.listdir(base_dir)

	result = []
	with concurrent.futures.ProcessPoolExecutor(max_workers=15) as executor:
		for file_name in file_list:

			base_file = os.path.join(base_dir, file_name)
			target_file = os.path.join(target_dir, file_name)

			obj = executor.submit(add_code_single, base_file, target_file)
			result.append(obj)

	for obj in result:
		if obj.result() != "done":
			print (obj.result())




