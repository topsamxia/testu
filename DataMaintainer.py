import StkFX
import os

if __name__ == '__main__':
    base_dir = os.path.abspath("D:\stock\stk_5f_1516_new")
    base_files = os.listdir(base_dir)

    new_dir = os.path.abspath('D:\stock\stk_5F_2017_new')
    new_files = os.listdir(new_dir)

    target_dir = os.path.abspath('D:\stock\stk_new')

    time_deal_base = StkFX.TimeSerialDeal()
    time_deal_new = StkFX.TimeSerialDeal()

    files_not_found = []

    for each_file in new_files:

        time_deal_new.loadCsvHasHead(os.path.join(new_dir, each_file))

        if each_file in base_files:
            time_deal_base.loadCsvHasHead(os.path.join(base_dir, each_file))
            print ("--------------------------------")
            print (each_file + " old length = ", len(time_deal_base.data.index), "new length = ", len(time_deal_new.data.index))

            time_deal_new.data = time_deal_base.data.append(time_deal_new.data).drop_duplicates()
            print ("merged length = ", len(time_deal_new.data.index))

            time_deal_new.saveCsv(os.path.join(target_dir, each_file))
            print (time_deal_new.data.head(2), time_deal_new.data.tail(2))

        else:
            print ("--------------------------------")
            print ("no base, merged length = ", len(time_deal_new.data.index))
            time_deal_new.saveCsv(os.path.join(target_dir, each_file))
            print (time_deal_new.data.head(2), time_deal_new.data.tail(2))
            files_not_found.append(each_file)

    print (("not found files are: "), files_not_found)



