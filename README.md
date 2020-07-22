#### 带有进度条显示的 快速迁移表的python包,快读sql生成DataFrame,生成CSV.

- 1.3.0 更新说明,添加 cx_Oracle.connect 直接做数据库连接支持
- **支持千万级数据迁移,支持自定义迁移列,自定义迁移数据结构**
- **在您 无法使用load 或dump命令，或者所迁移表结构不同,需要对数据做处理时使用。**
- **标准化 多线程读写,无需关注读写过程,只需关注入库前数据的处理**
- 采用本地缓存，不占用内存，

##### 一、安装

- pip install fast_sql

##### 二、依赖环境

- Python3.6+ 
- Mysql , Oracle

##### 二、示例


```python
import fast_sql
```

##### 多线程 读表生成DataFrame

```python
# con 数据库连接字符串，或者 sqlalchemy 对象
# chunksize 单个线程读取数量 默认20000
# show_progress 是否显示进度条
# thread_num 线程数量
# return Dataframe
# 其他参数兼容 pandas read_sql
con = "oracle+cx_oracle://wuwukai:wuwukai@localhost:1521/helowin"
df = fast_sql.read_sql('select * from student where SNO<2000000',con,show_progress=True,
                       chunksize = 40000,
                       thread_num = 15,)
```

```
Read the scheduler: 100%|█████████████████| 500001/500001 [01:20<00:00, 28192.45it/s]
```

##### 表迁移

```python
def astype_df(df):
    # 目标库 ctime列为str类型，这里做 转换返回
    df.CTIME = df.CTIME.astype('str')
    return df
```

```python
# from_db 数据源
# to_db 目标库
# to_table 目标表名
# if_exists 是否删除目标库数据 （append,delete,other）
# 如果 if_exists='delete' 可以指定删除语句 delete_sql = 'delete from xxx' ,默认使用源sql delete
# mode 迁移方式，rw 在线迁移，r 序列化到本地（需指定 save_path）， w 本地文件到数据库 (需指定file_path)
# delete_cache 是否删除迁移过程中 缓存文件，默认删除
# data_processing 入库前数据是否做处理，如目标库 列 类型 不一致，列名不同等，接受一个函数，参数为入库前
# DataFrame，需返回处理后的DataFrame
# chunksize 每个线程迁移数量
# thread_num 每个线程读取数量
# thread_w 写入线程数量
con = create_engine("oracle+cx_oracle://wuwukai:wuwukai@localhost:1521/helowin")
to_db = create_engine("mysql+pymysql://root:123456@localhost:3306/aps_2")
sql = '''select * from student where SNO<2000000'''
fast_sql.to_sql(sql,
                from_db = con,
                to_db = to_db,
                to_table = 'stu',
                if_exists='append',
                mode='rw',show_progress=True,
                delete_cache=True,
                data_processing=astype_df)
    
```

```
Read the scheduler:   0%|                                 | 0/500001 [00:00<?, ?it/s]
```

```
Read the scheduler:  76%|█████████████▋    | 380000/500001 [01:02<00:13, 8921.37it/s]
```

```
Write db Scheduler:  96%|██████████████████████████▉ | 26/27 [01:13<00:01,  1.10s/it]    
```

```
Write db Scheduler: 100%|████████████████████████████| 27/27 [01:13<00:00,  1.16it/s]
```

```
'finish'
```



##### 读表生成csv

```python
sql = '''select * from student where SNO<2000000'''
path = '/home/test.csv'
to_db = create_engine("mysql+pymysql://root:123456@localhost:3306/aps_2")
fast_sql.to_csv(sql,con,path_or_buf=path,show_progress=True,index=None)
```


