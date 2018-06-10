from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time
import os

def job():
    time_mark_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(time_mark_str)
    with open('log.txt', 'a') as f:
        f.write(time_mark_str+os.linesep)

    # 定义BlockingScheduler
sched = BackgroundScheduler()

# Task every minute_interval during trade time
def schedule_minute_helper(job_func, kwargs=None, minute_interval='5'):
    # 9:30-9:59
    minute_30 = '30-59/' + minute_interval
    sched.add_job(job_func, 'cron', kwargs=kwargs, day_of_week='mon-fri', hour='9', minute=minute_30)
    # 11:00-11:30
    minute_30 = '0-30/' + minute_interval
    sched.add_job(job_func, 'cron', kwargs=kwargs, day_of_week='mon-fri', hour='11', minute=minute_30)
    # 10:00-10:59, 13:00-14:59
    minute_60 = '0-59/' + minute_interval
    sched.add_job(job, 'cron', kwargs=kwargs, day_of_week='mon-fri', hour='10,13,14', minute=minute_60)
    # 15:00
    sched.add_job(job, 'cron', kwargs=kwargs, day_of_week='mon-fri', hour='15', minute='0')

# Daily task
def schedule_daily_helper(job_func, kwargs=None, jitter_min='10'):
    # 15:00 + jitter_min
    sched.add_job(job_func, 'cron', kwargs=kwargs, day_of_week='mon-fri', hour='15', minute=jitter_min)

def scheduler_test_helper(job_func, kwargs=None, second_interval="5"):
    sched.add_job(job_func, 'cron', kwargs=kwargs, second="0-59/"+second_interval)

if __name__ == "__main__":
    def testfun(arg1=1, arg2=2):
        print(arg1)
        print(arg2)
        print(datetime.now())
        time.sleep(1.5)


    # sched = BlockingScheduler()
    # schedule_daily_helper(sched, testfun, {"arg1":3, "arg2":4}, '1')
    scheduler_test_helper(testfun, kwargs={"arg1":3, "arg2":4}, second_interval='2')
    # sched.add_job(testfun, 'cron', kwargs={"arg1":3, "arg2":4}, second="1-59/2")
    # sched.add_job(testfun, 'cron', kwargs={"arg1":3, "arg2":4}, second="1-59/3")
    sched.start()
    while(1):
        time.sleep(100)
