import pymysql
import pandas as pd
from sqlalchemy import create_engine
from xml.dom import minidom
import time
import os


today_date = time.strftime("%Y-%m-%d", time.localtime())

engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')

# 连接数据库
connect = pymysql.connect(host="localhost", user="root", password="root", port=3306, db="stock")
# 创建一个游标对象:有两种创建方法
cursor = connect.cursor()
# 使用游标的execute()方法执行sql语句
cursor.execute("SELECT * FROM stock_list where status = '0'")
# 使用fetchall()获取全部数据
stock_list = cursor.fetchall()
# 关闭游标连接
cursor.close()
# 关闭数据库连接
connect.close()


def do_filter():
    stock_xml = ''
    for stock in stock_list:
        # sql_kdj = 'select * from history_data where code = %s order by date asc' % (repr(stock[3]))
        sql_vegas = 'select * from history_data where code = %s order by date desc' % (repr(stock[3]))
        # sql_bolling = 'select * from history_data where code = %s order by date asc' % (repr(stock[3]))
        # data_kdj = pd.read_sql(sql_kdj, engine)
        data_vegas = pd.read_sql(sql_vegas, engine)
        # data_bolling = pd.read_sql(sql_bolling, engine)

        # if filter_by_kdj(data_kdj) and filter_by_vegas(data_vegas):
        if filter_by_vegas(data_vegas):
            print(data_vegas.loc[0]['code'] + '---' + data_vegas.loc[0]['name'])
            if data_vegas.loc[0]['code'][0] == '6':
                stock_xml = stock_xml + '0,'
            else:
                stock_xml = stock_xml + '1,'

            stock_xml = stock_xml + data_vegas.loc[0]['code'] + ';'

    make_xml(stock_xml, 'vegas')


def filter_by_bolling(data):
    data['mid'] = data['close'].rolling(window=20).mean()

    data['std'] = data['close'].rolling(window=20).std()

    data['up'] = data['mid'] + 2 * data['std']

    data['down'] = data['mid'] - 2 * data['std']

    if data.loc[data.index[-1]]['low'] < data.loc[data.index[-1]]['down'] < data.loc[data.index[-1]]['close']:
        return True
    else:
        return False


def filter_by_kdj(data):
    low_list = data['low'].rolling(9, min_periods=9).min()
    low_list.fillna(value=data['low'].expanding().min(), inplace=True)
    high_list = data['high'].rolling(9, min_periods=9).max()
    high_list.fillna(value=data['high'].expanding().max(), inplace=True)
    rsv = (data['close'] - low_list) / (high_list - low_list) * 100

    data['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
    data['D'] = data['K'].ewm(com=2).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    if data.loc[data.index[-1]]['J'] <= 0:
        return True
    else:
        return False


def filter_by_vegas(data):
    if len(data) > 288:
        data12 = data[0: 12]
        data144 = data[0: 144]
        data144_2 = data[2: 146]
        data169 = data[0: 169]
        data169_2 = data[2: 171]
        data288 = data[0: 288]
        data288_2 = data[2: 290]
        # data338 = data[0: 338]

        ma12 = data12['close'].sum() / 12
        ma144 = data144['close'].sum() / 144
        ma144_2 = data144_2['close'].sum() / 144
        ma169 = data169['close'].sum() / 169
        ma169_2 = data169_2['close'].sum() / 169
        ma288 = data288['close'].sum() / 288
        ma288_2 = data288_2['close'].sum() / 288
        # ma338 = data338['close'].sum() / 338

        if ma288 > ma144 > ma169 > data.loc[0]['close']:
            return True
        else:
            return False
    else:
        return False


def filter_by_vegas2(data):
    if len(data) > 338:
        data12 = data[0: 12]
        data144 = data[0: 144]
        # data144_2 = data[2: 146]
        data169 = data[0: 169]
        # data169_2 = data[2: 171]
        data288 = data[0: 288]
        # data288_2 = data[2: 290]
        data338 = data[0: 338]

        ma12 = data12['close'].sum() / 12
        ma144 = data144['close'].sum() / 144
        # ma144_2 = data144_2['close'].sum() / 144
        ma169 = data169['close'].sum() / 169
        # ma169_2 = data169_2['close'].sum() / 169
        ma288 = data288['close'].sum() / 288
        # ma288_2 = data288_2['close'].sum() / 288
        ma338 = data338['close'].sum() / 338

        if ma338 > ma288 > data.loc[0]['close'] > ma169 > ma144:
            return True
        else:
            return False
    else:
        return False


def make_xml(stock_xml, type):
    # 创建Document
    xml = minidom.Document()

    # 创建root节点
    root = xml.createElement('自选股')
    xml.appendChild(root)

    child = xml.createElement('List')
    child.setAttribute('Name', today_date + '_' + type)
    child.setAttribute('Key', today_date + '_' + type)
    child.setAttribute('Stock', stock_xml)
    root.appendChild(child)

    if not os.path.exists('/Users/zhangzoushin/Documents/xml/' + type):
        os.mkdir('/Users/zhangzoushin/Documents/xml/' + type)

    fp = open('/Users/zhangzoushin/Documents/xml/' + type + '/' + today_date + '_' + type + '.xml', 'w', encoding='gbk')
    xml.writexml(fp, indent='	', addindent='	', newl='\n')


if __name__ == '__main__':
    do_filter()
