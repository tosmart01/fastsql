import os
import uuid
import re
import sys
import threading
import shutil
import pandas as pd
from tqdm import tqdm
from queue import Queue
from fast_sql.utils.exception import DB_Exceptions
from concurrent.futures import ThreadPoolExecutor
from fast_sql.utils.common import DB_Pool, collection_error,Sqlalchemy_Pool


class Read_sql:

    def __init__(
            self,
            sql,
            con=None,
            thread_num=None,
            encoding='utf8',
            show_progress=False,
            chunksize=15000,
            desc=None
            ):
        if con is not None:
            self.db_pool = Sqlalchemy_Pool(con, num=thread_num + 6, encoding=encoding)
            self.driver = self.db_pool.driver
        self.sql = sql.strip().replace('\n', ' ')
        self.thread_num = thread_num
        self.avg_list = None
        self.result = []
        self.pd_params = None
        self.lock = threading.Lock()
        self.ordering = None
        self.show_progress = show_progress
        self.progress = None
        self.queue = Queue()
        self.count = None
        self.tqdm = None
        self.chunksize = chunksize
        self.tqdm_w = None
        self.desc = desc

    def read_sql(self, **kwargs):
        con = self.db_pool.get_db()
        self.pd_params = kwargs
        self.avg_list = self.verify_sql()
        self.tqdm_init(self.count, weight=85)
        if self.avg_list is None:
            result = pd.read_sql(self.sql, con, **self.pd_params)
            result.columns = [i.upper() for i in result.columns.tolist()]
            self.tqdm_update(self.count)
        else:
            task_list,pool = self.start_thread_read()
            self.get_thread_result(task_list)
            result = self.result.pop(0)
            for i in range(len(self.result)):
                result = result.append(self.result.pop(0), ignore_index=True)

            result.columns = [i.upper() for i in result.columns.tolist()]

            if self.driver == 'oracle':
                result.drop('NO', axis=1, inplace=True)

        if self.ordering:
            result.sort_values([i.strip().upper()
                                for i in self.ordering], inplace=True)
        self.db_pool.close_db(con)
        self.result.clear()
        return result

    def tqdm_update(self, count=None,mode=None):
        tqdm = self.tqdm if mode is None  else self.tqdm_w
        if self.show_progress:
            if count:
                tqdm.update(count)

            elif tqdm.n + self.chunksize < self.count:
                tqdm.update(self.chunksize)

            else:
                tqdm.update(self.count - tqdm.n)

    def tqdm_init(self, count,weight,mode=None):
        if self.show_progress:
            if mode is None:
                self.tqdm = tqdm(total=count, desc=self.desc, ncols=weight)
            else:
                self.tqdm_w = tqdm(total=count, desc=self.desc, ncols=weight)

    @collection_error
    def get_sql_query(self, st, en, *args, **kwargs):
        if self.driver == 'oracle':
            sql_1 = f'''
            select * from (select rownum no,d.* from (
            {self.sql}
            ) d where rownum<{en}) where no>={st}
            '''
        else:
            sql_1 = f'''
                 select * from ({self.sql}) as t limit {st}{','+str(en)}
            '''
        con = self.db_pool.get_db()
        query = pd.read_sql(sql_1, con, **self.pd_params)
        self.save_query(query)
        self.db_pool.close_db(con)
        del query

    def save_query(self, df_value):
        self.lock.acquire()
        self.result.append(df_value)
        self.tqdm_update()
        self.lock.release()

    def start_thread_read(self,):
        pool = ThreadPoolExecutor(max_workers=self.thread_num)
        task_list = [pool.submit(self.get_sql_query, st, en)
                     for st, en in self.avg_list]
        return task_list,pool

    def verify_sql(self):
        con = self.db_pool.get_db()
        self.count = self.get_query_count(con, self.sql)
        # self.chunksize = self.count // self.thread_num // 2
        # self.tqdm_init(self.count, desc='Read the scheduler', weight=85)

        if 'order' in self.sql.lower():
            buf = self.sql.split('order')
            self.ordering = buf[1].split('by')[1].strip().split(',')
            self.sql = buf[0]

        if self.count < 2000:
            return None

        if self.driver == 'oracle':
            avg_list = [
                (i,
                 i +
                 self.chunksize) for i in range(
                    1,
                    self.count +
                    self.chunksize +
                    1,
                    self.chunksize)]

        else:
            avg_list = [(self.chunksize * i, self.chunksize)
                        for i in range(0, self.count // self.chunksize + 1)]

        self.db_pool.close_db(con)
        return avg_list

    def get_query_count(self, con, sql):
        # _sql = "select count(*) " + re.search(r"from\s+.*", sql, re.I).group()
        _sql = f"select count(*) from ({sql}) t"
        count = pd.read_sql(_sql, con, **self.pd_params).iloc[0, 0]
        # assert count > 0, DB_Exceptions('DB_EMPTY:')
        return count

    def get_thread_result(self,task_list):
        for job in task_list:
            job.result()

    def __del__(self):
        if self.show_progress:
            if self.tqdm is not None:
                self.tqdm.update(self.count - self.tqdm.n)
        sys.stdout.flush()


class to_csv(Read_sql):

    def __init__(self, *args, path_or_buf=None, **kwargs):
        super(to_csv, self).__init__(*args, **kwargs)
        self.path = path_or_buf
        self.args = None
        self.kwargs = None

    def write_csv_header(self):
        if self.driver == 'oracle':
            sql_1 = f'''
            select * from (select rownum no,d.* from (
            {self.sql}
            ) d where rownum<{2}) where no>={1}
            '''
        else:
            sql_1 = f'''
                 select * from ({self.sql}) as t limit 1
            '''
        return sql_1

    def build_csv(self, *args, **kwargs):
        con = self.db_pool.get_db()
        self.args = args
        self.kwargs = kwargs
        self.kwargs.pop('mode')
        self.kwargs.pop('chunksize')
        self.pd_params = {'columns': None}
        self.avg_list = self.verify_sql()
        self.tqdm_init(self.count, weight=85)
        if self.avg_list is None:
            pd.read_sql(self.sql, con).to_csv(*args, **kwargs)
            self.tqdm_update(self.count)
        else:
            df = pd.read_sql(self.write_csv_header(),con)
            # df = next(pd.read_sql(self.sql, con, chunksize=1)).iloc[1:, ]
            df.to_csv(*args, **kwargs)
            task_list,pool = self.start_thread_read()
            self.db_pool.close_db(con)
            self.get_thread_result(task_list)

        self.db_pool.close_db(con)
        return 'finish'

    def save_query(self, df_value):
        self.lock.acquire()
        if self.driver == 'oracle':
            df_value.drop(df_value.columns[0], axis=1, inplace=True)

        # if self.ordering:
        #     df_value.columns = [i.lower() for i in df_value.columns.tolist()]
        #     df_value.sort_values([i.strip() for i in self.ordering], inplace=True)
        self.kwargs.update(header=None)
        df_value.to_csv(*self.args, mode='a', **self.kwargs,)
        self.tqdm_update()
        self.lock.release()


class to_sql(Read_sql):

    def __init__(self, *args, **kwargs):
        to_db = kwargs.pop('to_db')
        kwargs.update(con=kwargs.pop('from_db'))
        self.to_table = kwargs.pop('to_table')
        self.to_columns = kwargs.pop('to_columns')
        self.if_exists = kwargs.pop('if_exists')
        self.mode = kwargs.pop('mode').lower()
        self.file_path = kwargs.pop('file_path')
        self.delete_cache = kwargs.pop('delete_cache')
        self.save_path = kwargs.pop('save_path',None)
        self.thread_w = kwargs.pop('thread_w',3)
        self.delete_sql= kwargs.pop('delete_sql',None)
        self.dir_path = None
        self.task_count = None
        self.execute_count = 0
        self.data_processing = kwargs.pop('data_processing',None)
        super().__init__(*args, **kwargs)
        if to_db is not None:
            self.to_db = DB_Pool(to_db, num=8, encoding=kwargs.get('encoding'))
            self.write_driver = self.to_db.driver
        self.lock_b = threading.Lock()
        self.execute_pool = None

    def delete_table(self):
        sql = self.sql.replace(
            re.search(
                r'from\s+(\S+)',
                self.sql).group(1),
            self.to_table)
        sql = re.sub('(order|ORDER)\s+(by|BY).*','',sql)
        sql = 'delete ' + re.search('from.*', sql, re.I).group()
        con = self.to_db.get_db()
        db = con.cursor()
        if self.delete_sql is not None:
            sql = self.delete_sql
        db.execute(sql)
        con.commit()
        self.to_db.close_db(con)


    def decision(self):
        if self.save_path is None:
            self.dir_path = os.path.join(os.getcwd(),f'{uuid.uuid1()}){self.to_table}')
            os.mkdir(self.dir_path)
        else:
            self.dir_path = self.save_path
        self.file_path = self.dir_path

        if self.avg_list is None:
            con = self.db_pool.get_db()
            df = pd.read_sql(self.sql, con, **self.pd_params)
            file_path = os.path.join(self.dir_path, f'{uuid.uuid1()}.pkl')
            df.to_pickle(file_path)
            if self.mode in ('wr', 'rw'):
                self.task_count = 1
                self.tqdm_init(self.task_count, weight=85, mode=True)
                self.queue.put(file_path)
                self.write_db()
            else:
                self.tqdm_update(count=self.task_count)

            self.db_pool.close_db(con)

        else:
            task_list,pool = self.start_thread_read()
            self.task_count = len(self.avg_list)
            if self.mode in ('wr', 'rw'):
                self.tqdm_init(self.task_count, weight=85, mode=True)
                _pool = ThreadPoolExecutor(max_workers=self.thread_w)
                p = [pool.submit(self.write_db) for i in range(5)]
                self.get_thread_result(p)
            self.get_thread_result(task_list)

    def rsync_db(self, *args, **kwargs):

        self.pd_params = kwargs

        self.sql = self.sql.strip().replace('\n', ' ')

        if self.to_table is None:
            self.to_table = re.search(r'from\s+(\S+)', self.sql, re.I).group(1)

        if self.if_exists == 'delete' and self.mode !='r':
            self.delete_table()

        if self.mode == 'w':
            return self.write()

        self.avg_list = self.verify_sql()
        self.tqdm_init(self.count, weight=85)

        self.decision()

        return 'finish'

    def write(self):
        file_list = [
            os.path.join(
                self.file_path,
                i) for i in os.listdir(
                self.file_path)]
        self.task_count = len(file_list)
        self.tqdm_w = tqdm(
            total=self.task_count,
            desc=self.desc,
            ncols=80)
        T = ThreadPoolExecutor(max_workers=self.thread_w)
        put_list = [self.queue.put(path) for path in file_list]
        task_list = [T.submit(self.write_db) for i in put_list]
        T.shutdown(wait=True)
        return 'finish'

    def save_query(self, df_value):
        file_path = os.path.join(self.dir_path, f'{uuid.uuid1()}.pkl')
        if self.driver == 'oracle':
            df_value.drop(df_value.columns[0], axis=1, inplace=True)
        df_value.to_pickle(file_path)
        self.queue.put(file_path)
        self.tqdm_update()

    def get_sys_guid_col(self, columns, con):
        col_type = pd.read_sql(
            f"select a.COLUMN_NAME,a.DATA_TYPE,a.DATA_SCALE,a.DATA_DEFAULT from all_tab_cols a where TABLE_NAME='{self.to_table.upper()}'",
            con)
        sys_guid = col_type.loc[col_type['DATA_DEFAULT']
                                == ' SYS_GUID() ', 'COLUMN_NAME']
        if not sys_guid.empty:
            columns.remove(sys_guid.values[0])
            return columns
        else:
            return columns

    def get_sql(self, columns,):
        insert_col = ','.join(columns)
        # con = self.to_db.get_db()
        if self.write_driver == 'oracle':
            # columns = self.get_sys_guid_col(columns, con)
            sql_col = ':' + ',:'.join([str(i)
                                       for i in range(1, len(columns) + 1)])
            sql = f'insert into {self.to_table}({insert_col}) values({sql_col})'
            return sql

        elif self.write_driver == 'mysql':
            sql_col = '%' + ',%'.join('s' * len(columns))
            sql = f"insert into {self.to_table}({insert_col}) values({sql_col})"
            return sql

    @collection_error
    def write_db(self):
        while True:
            self.lock_b.acquire()
            if self.execute_count < self.task_count:
                # df = pd.read_pickle(file_path)
                self.execute_count += 1
                self.lock_b.release()
                path = self.queue.get()
                self.insert_db(path)
            else:
                # self.tqdm_w.update(self.task_count - self.tqdm_w.n)
                self.lock_b.release()
                break


    def insert_db(self,path):
        df = pd.read_pickle(path)
        df.columns = [i.upper() for i in df.columns]
        if self.data_processing is not None:
            df = self.data_processing(df)

        c = [(column, str(date)) for column, date in zip(df.columns.tolist(), df.dtypes) if 'date' in str(date)]
        if self.write_driver == 'mysql':
            for column, date in c:
                df[column] = df[column].astype('str')
                df.replace('NaT', None, inplace=True)
        else:
            for column, date in c:
                df[column] = df[column].astype('object')

        df = df.mask(df.isna(), None)

        con = self.to_db.get_db()
        db = con.cursor()

        if self.to_columns is not None:
            df = df[self.to_columns]

        columns = df.columns.tolist()
        sql = self.get_sql(columns)

        try:
            db.executemany(sql, df.values.tolist())
        except Exception as e:
            con.rollback()
            raise e
        else:
            con.commit()
            self.tqdm_update(count=1, mode=True)
        finally:
            del df
            db.close()
            con.close()

    def __del__(self):

        if self.delete_cache:
            if self.mode in ('wr', 'rw') and self.save_path is None:
                shutil.rmtree(self.file_path)
        if self.show_progress:
            if self.tqdm is not None:
                self.tqdm.update(self.count - self.tqdm.n)
            if self.tqdm_w is not None:
                self.tqdm_w.update(self.task_count - self.tqdm_w.n)
        sys.stdout.flush()
