import datetime


def get_week_day(date):
    week_day_dict = {
        0: '星期一',
        1: '星期二',
        2: '星期三',
        3: '星期四',
        4: '星期五',
        5: '星期六',
        6: '星期天',
    }
    day = date.weekday()
    return week_day_dict[day]


def string_toDatetime(st):
    print("2.把字符串转成datetime: ", datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S"))


print(get_week_day(datetime.datetime.now()))
print(datetime.datetime.now())
print(get_week_day(datetime.datetime.strptime('2021-11-26', "%Y-%m-%d")))
