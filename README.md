#### 带有进度条显示的 多线程快读快写sql,快速迁移表的python包

##### 一、安装

- pip install fast_sql

##### 二、依赖环境

- python3.6+ 
- mysql | oracle

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



- 快速读sql生成csv

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

  

##### 三、参数说明

```python
其他参数兼容pandas read_sql,to_csv
sql: sql语句
con: 数据库连接 sqlalchemy连接对象 | sqlalchemy连接字符串 | pymysql连接对象
thread_num: 开启的线程数量
encoding: 编码
show_progress: 是否显示进度条
from_db: 数据源库
to_db: 目标库
if_exists: 目标库相同数据是否删除 delete append orthe(不做处理)
to_table: 目标库表名，默认与原始sql表名相同
mode: r > 读取sql序列化到本地
      w > 将序列化的文件入库
      rw > 从源库读取写入到目标库
file_path: 读取数据序列化路径
delete_cache: 是否删除迁移过程中缓存的序列化文件
to_columns: 指定目标库的列名，默认与原列名相同
    
```



