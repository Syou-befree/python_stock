# 按照成交量突然放大筛选股票
import pandas as pd
from sqlalchemy import create_engine
from xml.dom import minidom
import time


today_date = time.strftime("%Y-%m-%d", time.localtime())
engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')


def get_by_vol():
    stock_list = pd.read_sql("SELECT code, name FROM stock_list ORDER BY list_id ASC", engine)
    stock_xml = ''

    for row in stock_list.itertuples():
        sql = 'select * from history_data where code = %s order by date DESC' % (repr(getattr(row, 'code')))
        data = pd.read_sql(sql, engine)

        if len(data) > 14:
            if data.loc[0]['volume'] > data.loc[1]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[2]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[3]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[4]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[5]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[6]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[7]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[8]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[9]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[10]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[11]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[12]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[13]['volume'] * 2 and \
                    data.loc[0]['volume'] > data.loc[14]['volume'] * 2:
                print(data.loc[0]['code'] + '---' + data.loc[0]['name'])

                if data.loc[0]['code'][0] == '6':
                    stock_xml = stock_xml + '0,'
                else:
                    stock_xml = stock_xml + '1,'

                stock_xml = stock_xml + data.loc[0]['code'] + ';'
    make_xml(stock_xml)


def make_xml(stock_xml):
    # 创建Document
    xml = minidom.Document()

    # 创建root节点
    root = xml.createElement('自选股')
    xml.appendChild(root)

    child = xml.createElement('List')
    child.setAttribute('Name', today_date + '_vol')
    child.setAttribute('Key', today_date + '_vol')
    child.setAttribute('Stock', stock_xml)
    root.appendChild(child)

    fp = open('/Users/zhangzoushin/Documents/xml/' + today_date + '.xml', 'w', encoding='gbk')
    xml.writexml(fp, indent='	', addindent='	', newl='\n')


if __name__ == '__main__':
    get_by_vol()
