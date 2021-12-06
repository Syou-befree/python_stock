import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:root@localhost:3306/stock')

code = '603809'

sql = "select * from history_data where code = %s order by date DESC" % (repr(code))
data = pd.read_sql(sql, engine)

if len(data) > 338:
    data144 = data[0: 144]
    data169 = data[0: 169]
    data288 = data[0: 288]
    data338 = data[0: 338]

    ma144 = data144['close'].sum() / 144
    ma169 = data169['close'].sum() / 169
    ma288 = data288['close'].sum() / 288
    ma338 = data338['close'].sum() / 338

    print('ma144-----' + str(ma144))
    print('ma169-----' + str(ma169))
    print('ma288-----' + str(ma288))
    print('ma338-----' + str(ma338))

