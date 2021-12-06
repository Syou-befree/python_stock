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
cursor.execute("SELECT * FROM stock_list")
# 使用fetchall()获取全部数据
stock_list = cursor.fetchall()
# 关闭游标连接
cursor.close()
# 关闭数据库连接
connect.close()


def do_filter():
    stock_xml = ''
    for stock in stock_list:
        sql_vegas = 'select * from weekly_data where code = %s order by end_date desc' % (repr(stock[3]))
        sql_bolling = 'select * from weekly_data where code = %s order by end_date asc' % (repr(stock[3]))

        data_vegas = pd.read_sql(sql_vegas, engine)
        data_bolling = pd.read_sql(sql_bolling, engine)

        if filter_by_bolling(data_bolling):
            print(data_bolling.loc[0]['code'] + '---' + data_bolling.loc[0]['name'])
            if data_bolling.loc[0]['code'][0] == '6':
                stock_xml = stock_xml + '0,'
            else:
                stock_xml = stock_xml + '1,'

            stock_xml = stock_xml + data_bolling.loc[0]['code'] + ';'

    make_xml(stock_xml, 'weekly')


def filter_by_bolling(data):
    if len(data) > 20:
        data['mid'] = data['close'].rolling(window=20).mean()

        data['std'] = data['close'].rolling(window=20).std()

        data['up'] = data['mid'] + 2 * data['std']

        data['down'] = data['mid'] - 2 * data['std']

        if data.loc[data.index[-1] - 1]['close'] < data.loc[data.index[-1] - 1]['mid'] \
                and data.loc[data.index[-1]]['close'] > data.loc[data.index[-1]]['mid']:
            return True
        else:
            return False
    else:
        return False


def filter_by_vegas(data):
    if len(data) > 174:
        ma12 = data[0: 12]['close'].sum() / 12
        ma144 = data[0: 144]['close'].sum() / 144
        ma169 = data[0: 169]['close'].sum() / 169

        if data[0: 169]['close'].sum() / 169 > data[0: 144]['close'].sum() / 144 \
                and data[1: 170]['close'].sum() / 169 > data[1: 145]['close'].sum() / 144 \
                and data[2: 171]['close'].sum() / 169 > data[2: 146]['close'].sum() / 144 \
                and data[3: 172]['close'].sum() / 169 > data[3: 147]['close'].sum() / 144 \
                and data[4: 173]['close'].sum() / 169 > data[4: 148]['close'].sum() / 144 \
                and data[5: 174]['close'].sum() / 169 > data[5: 149]['close'].sum() / 144 \
                and data[5: 174]['close'].sum() / 169 - data[5: 149]['close'].sum() / 144 \
                > data[4: 173]['close'].sum() / 169 - data[4: 148]['close'].sum() / 144 \
                > data[3: 172]['close'].sum() / 169 - data[3: 147]['close'].sum() / 144 \
                > data[2: 171]['close'].sum() / 169 - data[2: 146]['close'].sum() / 144 \
                > data[1: 170]['close'].sum() / 169 - data[1: 145]['close'].sum() / 144 \
                > data[0: 169]['close'].sum() / 169 - data[0: 144]['close'].sum() / 144 \
                and data.loc[0]['close'] > ma144:
            return True
        else:
            return False

        # if data[0: 169]['close'].sum() / 169 > data[0: 144]['close'].sum() / 144 \
        #         and data[1: 170]['close'].sum() / 169 > data[1: 145]['close'].sum() / 144 \
        #         and data[2: 171]['close'].sum() / 169 > data[2: 146]['close'].sum() / 144 \
        #         and data[3: 172]['close'].sum() / 169 > data[3: 147]['close'].sum() / 144 \
        #         and data[4: 173]['close'].sum() / 169 > data[4: 148]['close'].sum() / 144 \
        #         and data[5: 174]['close'].sum() / 169 > data[5: 149]['close'].sum() / 144 \
        #         and ma12 > ma144 > ma169 \
        #         and data[5: 174]['close'].sum() / 169 - data[5: 149]['close'].sum() / 144 \
        #         > data[4: 173]['close'].sum() / 169 - data[4: 148]['close'].sum() / 144 \
        #         > data[3: 172]['close'].sum() / 169 - data[3: 147]['close'].sum() / 144 \
        #         > data[2: 171]['close'].sum() / 169 - data[2: 146]['close'].sum() / 144 \
        #         > data[1: 170]['close'].sum() / 169 - data[1: 145]['close'].sum() / 144 \
        #         > data[0: 169]['close'].sum() / 169 - data[0: 144]['close'].sum() / 144 \
        #         and data.loc[0]['close'] > ma169:
        #     return True
        # else:
        #     return False
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
