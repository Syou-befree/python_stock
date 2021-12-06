# 导入及处理数据
import pandas as pd
import numpy as np
import pymysql
# 绘图
import matplotlib.pyplot as plt
import matplotlib as mpl
import os
# 设置图像标签显示中文
# plt.rcParams['font.sans-serif'] = ['SimHei']
# plt.rcParams['axes.unicode_minus'] = False


# 解决一些编辑器(VSCode)或IDE(PyCharm)等存在的图片显示问题，
# 应用Tkinter绘图，以便对图形进行放缩操作
mpl.use('TkAgg')
# 连接数据库
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")


def filter_bolling():
    stock_list = get_db_data("SELECT * FROM stock_list ORDER BY list_id ASC")
    for stock in stock_list:
        sql = "select * from history_data where code = %s order by date desc" % (repr(stock[3]))
        df = pd.read_sql(sql, connect)
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df.set_index(['date'], inplace=True)
        if len(df) > 250:
            df = df[0:250]
        bolling(df, stock[3])
        exit()


def bolling(df, code):
    # SMA:简单移动平均(Simple Moving Average)
    time_period = 26  # SMA的计算周期，默认为20
    stdev_factor = 2  # 上下频带的标准偏差比例因子
    history = []  # 每个计算周期所需的价格数据
    sma_values = []  # 初始化SMA值
    upper_band = []  # 初始化阻力线价格
    lower_band = []  # 初始化支撑线价格

    # 构造列表形式的绘图数据
    for close_price in df['close']:
        #
        history.append(close_price)

        # 计算移动平均时先确保时间周期不大于20
        if len(history) > time_period:
            del (history[0])

        # 将计算的SMA值存入列表
        sma = np.mean(history)
        sma_values.append(sma)
        # 计算标准差
        stdev = np.sqrt(np.sum((history - sma) ** 2) / len(history))
        upper_band.append(sma + stdev_factor * stdev)
        lower_band.append(sma - stdev_factor * stdev)

    # 将计算的数据合并到DataFrame
    df = df.assign(close=pd.Series(df['close'], index=df.index))
    df = df.assign(middle=pd.Series(sma_values, index=df.index))
    df = df.assign(zuli=pd.Series(upper_band, index=df.index))
    df = df.assign(zhicheng=pd.Series(lower_band, index=df.index))

    # 绘图
    ax = plt.figure()
    # 设定y轴标签
    ax.ylabel = '%s price in ￥' % code

    df['close'].plot(color='k', lw=1., legend=True)
    df['middle'].plot(color='b', lw=1., legend=True)
    df['zuli'].plot(color='r', lw=1., legend=True)
    df['zhicheng'].plot(color='g', lw=1., legend=True)
    # plt.show()
    # ax.tight_layout()

    if not os.path.exists('./figure'):
        os.mkdir('./figure')

    ax.savefig('./figure/' + code + '.png'.format(code))
    plt.close()


def get_db_data(sql):
    # 创建一个游标对象:有两种创建方法
    cursor = connect.cursor()
    # 或：cursor=pymysql.cursors.Cursor(connect)
    # 使用游标的execute()方法执行sql语句
    cursor.execute(sql)
    # 使用fetchall()获取全部数据
    data = cursor.fetchall()
    # 关闭游标连接
    cursor.close()
    return data


if __name__ == '__main__':
    filter_bolling()
    connect.close()

