#### 带有进度条显示的 快读快写sql,快速迁移表的python包(多线程版)

##### 一、安装

- pip install fast_sql

##### 二、示例

- 快读读取sql生成DataFrame

```python
import fast_sql
from sqlalchemy import create_engine
sql = "select * from test where id <1000000"
con = create_engine("oracle+cx_oracle://wuwukai:test@localhost:1521/helowin")
res = fast_sql.read_sql(sql,con,show_progress=True,thread_num=8)
res.head()
```

image:

![快读示例](http://www.chaoyue.red/static/media/example_1.png)



- 快速读取sql生成csv

  ```python
  sql = "select * from test where id <1000000"
  path = '/home/test.csv'
  con = create_engine("oracle+cx_oracle://wuwukai:wuwukai@localhost:1521/helowin")
  res = fast_sql.to_csv(sql,con,path_or_buf=path,
                       show_progress=True,thread_num=8,index=None)
  
  ```

  

- 快速表迁移

  ```python
  #1.在线迁移
  sql = "select * from student where id <1000000"
  con = create_engine("oracle+cx_oracle://wuwukai:test@localhost:1521/helowin")
  to_db = create_engine("mysql+pymysql://root:123456@localhost:3306/aps_2")
  res = fast_sql.to_sql(sql,from_db=con,to_db=to_db,if_exists='delete',
         			    mode='rw',to_table='stu',delete_cache=True,show_progress=True,)
  ```

  ![](http://www.chaoyue.red/static/media/example_2.png)

  ```python
  # 2.离线迁移，先序列化到本地，在迁移
  sql = "select * from student where id <1000000"
  con = create_engine("oracle+cx_oracle://wuwukai:test@localhost:1521/helowin")
  to_db = create_engine("mysql+pymysql://root:123456@localhost:3306/aps_2")
  # 写入本地，保存在当前工作目录
  res = fast_sql.to_sql(sql,from_db=con,
         				  mode='r',to_table='stu',show_progress=True,)
  # 入库，file_path 为保存的目录
  res = fast_sql.to_sql(sql,to_db=con,file_path='/home/test'
        				  mode='w',to_table='stu',show_progress=True,)
  
  ```

  

