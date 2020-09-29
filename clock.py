from time import sleep
from apscheduler.schedulers.blocking import BlockingScheduler
from app import AdmobApi
import datetime

already_done = []


def collect_data(start_date, end_date):
    bot = AdmobApi()
    report_json = bot.get_report(start_date, end_date)
    bot.parse_data(report_json)
    bot.insert_db()


def make_call():
    year = "2020"
    month = "09"
    for day in range(1, 4):
        time_now = datetime.datetime.utcnow()
        st = '-'.join([year, month, str(day)])
        print(time_now, st)
        if st not in already_done:
            print("Collecting data for:", st)
            already_done.append(st)
            collect_data(st, st)


if __name__ == '__main__':
    sched = BlockingScheduler()
    sched.add_job(make_call, 'interval', minutes=3)
    sched.start()
