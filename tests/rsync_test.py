# -- coding: utf-8 --
# @Time : 2023/1/16 2:20 下午
# @Author : zhuo.wang
# @File : rsync_test.py
import warnings

import pandas as pd

warnings.filterwarnings('ignore')
import fast_sql
import time

if __name__ == '__main__':
    to_db = 'mysql+pymysql://root:123456@127.0.0.1:3306/haier_api'
    from_db = 'mysql+pymysql://root:123456@127.0.0.1:3306/electric_api'
    st = time.time()
    # fast_sql.to_sql("select * from WX_SPOOR",from_db=from_db,to_db=to_db,if_exists='delete',write_desc='user write',
    #                 delete_sql='truncate table WX_SPOOR',chunksize=20000,thread_num=20,thread_w=8)
    fast_sql.to_sql("select * from WX_SPOOR",from_db=from_db,to_db=to_db,if_exists='append',
                    df=pd.read_pickle('/Users/wangzhuo/workdir/fastsql/952f6282-957c-11ed-b97d-784f4371cabd)WX_SPOOR/a99e1f38-957c-11ed-b97d-784f4371cabd.pkl')
                    )
    print(time.time() - st)

