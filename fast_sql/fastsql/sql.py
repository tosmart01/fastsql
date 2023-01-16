import copy
import os
import inspect
import uuid
import re
import sys
import threading
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from fast_sql.utils.common import DB_Pool, collection_error, Sqlalchemy_Pool


class ReadSql:
    def __init__(
        self,
        sql,
        con=None,
        thread_num=None,
        encoding="utf8",
        show_progress=False,
        chunksize=15000,
        desc=None,
        columns_upper=True,
        auto_order=False,
    ):
        if con is not None:
            self.db_pool = Sqlalchemy_Pool(con, num=thread_num + 1, encoding=encoding)
            self.driver = self.db_pool.driver
        self.sql = sql.strip().replace("\n", " ")
        self.thread_num = thread_num
        self.avg_list = None
        self.result = []
        self.pd_params = None
        self.lock = threading.Lock()
        self.ordering = None
        self.show_progress = show_progress
        self.progress = None
        self.count = None
        self.tqdm = None
        self.chunksize = chunksize
        self.tqdm_w = None
        self.desc = desc
        self.columns_upper = columns_upper
        self.auto_order = auto_order
        self.consumer_count = 0
        self.pool = ThreadPoolExecutor(max_workers=self.thread_num)
        self.task_list = []

    def read_sql(self, **kwargs):
        con = self.db_pool.get_db()
        self.pd_params = kwargs
        self.avg_list = self.verify_sql()
        self.tqdm_init(self.count, weight=85)
        if self.avg_list is None:
            result = pd.read_sql(self.sql, con, **self.pd_params)
            if self.columns_upper:
                result.columns = [i.upper() for i in result.columns.tolist()]
            self.tqdm_update(self.count)
        else:
            self.start_thread_read()
            self.get_thread_result()
            result = self.result.pop(0)
            for i in range(len(self.result)):
                result = result.append(self.result.pop(0), ignore_index=True)
            if self.columns_upper:
                result.columns = [i.upper() for i in result.columns.tolist()]

            if self.driver == "oracle":
                result.drop("NO", axis=1, inplace=True)

        if self.auto_order:
            result.sort_values(
                [i.strip() for i in self.ordering],
                inplace=True,
                ascending=self.sort_type,
            )
        self.db_pool.close_db(con)
        self.result.clear()
        return result

    def tqdm_update(self, count=None, mode=None):
        tqdm = self.tqdm if mode is None else self.tqdm_w
        if self.show_progress:
            if count:
                tqdm.update(count)
            elif tqdm.n + self.chunksize < self.count:
                tqdm.update(self.chunksize)

            else:
                tqdm.update(self.count - tqdm.n)

    def tqdm_init(self, count, weight, mode=None, desc=None):
        if self.show_progress:
            if mode is None:
                self.tqdm = tqdm(total=count, desc=desc or self.desc, ncols=weight)
            else:
                self.tqdm_w = tqdm(total=count, desc=desc or self.desc, ncols=weight)

    @collection_error
    def get_sql_query(self, st, en, *args, **kwargs):
        if self.driver == "oracle":
            sql_1 = f"""
            select * from (select rownum no,d.* from (
            {self.sql}
            ) d) where no<{en} and no>={st}
            """
        else:
            sql_1 = f"""
                 select * from ({self.sql}) as t limit {st}{',' + str(en)}
            """
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

    def start_thread_read(
        self,
    ):
        for st, en in self.avg_list:
            self.task_list.append(self.pool.submit(self.get_sql_query, st, en))

    def verify_sql(self):
        con = self.db_pool.get_db()
        self.count = self.get_query_count(con, self.sql)
        # self.chunksize = self.count // self.thread_num // 2
        # self.tqdm_init(self.count, desc='Read the scheduler', weight=85)
        if self.auto_order:
            buf = re.search("(?<=order[\s+]by)(.*)(?=limit)", self.sql, re.I)
            if buf:
                buf = buf.group()
                self.sort_type = False if "desc" in buf.lower() else True
                buf = (
                    buf.replace("desc", "")
                    .replace("asc", "")
                    .replace("DESC", "")
                    .replace("ASC", "")
                )
                self.sql = (
                    "select "
                    + re.search("(?<=select)(.*)(?=limit)", self.sql, re.I).group()
                )
                self.ordering = buf.split(",")

        if self.count < 2000:
            return None

        if self.driver == "oracle":
            avg_list = [
                (i, i + self.chunksize)
                for i in range(1, self.count + self.chunksize + 1, self.chunksize)
            ]

        else:
            avg_list = [
                (self.chunksize * i, self.chunksize)
                for i in range(0, self.count // self.chunksize + 1)
            ]

        self.db_pool.close_db(con)
        return avg_list

    def get_query_count(self, con, sql):
        _sql = f"select count(*) from ({sql}) t"
        count = pd.read_sql(_sql, con, **self.pd_params).iloc[0, 0]
        return count

    def get_thread_result(self):
        for task in as_completed(self.task_list):
            task.result()

    def __del__(self):
        if self.show_progress:
            if self.tqdm is not None:
                self.tqdm.update(self.count - self.tqdm.n)
        sys.stdout.flush()


class ToCsv(ReadSql):
    def __init__(self, *args, path_or_buf=None, **kwargs):
        super(ToCsv, self).__init__(*args, **kwargs)
        self.path = path_or_buf
        self.args = None
        self.kwargs = None

    def write_csv_header(self):
        if self.driver == "oracle":
            sql_1 = f"""
            select * from (select rownum no,d.* from (
            {self.sql}
            ) d where no<{2} and no>={1}
            """
        else:
            sql_1 = f"""
                 select * from ({self.sql}) as t limit 1
            """
        return sql_1

    def build_csv(self, *args, **kwargs):
        con = self.db_pool.get_db()
        self.args = args
        self.kwargs = kwargs
        self.kwargs.pop("mode")
        self.kwargs.pop("chunksize")
        self.pd_params = {"columns": None}
        self.avg_list = self.verify_sql()
        self.tqdm_init(self.count, weight=85)
        if self.avg_list is None:
            pd.read_sql(self.sql, con).to_csv(*args, **kwargs)
            self.tqdm_update(self.count)
        else:
            df = pd.read_sql(self.write_csv_header(), con)
            # df = next(pd.read_sql(self.sql, con, chunksize=1)).iloc[1:, ]
            df.to_csv(*args, **kwargs)
            self.start_thread_read()
            self.db_pool.close_db(con)
            self.get_thread_result()

        self.db_pool.close_db(con)
        return "finish"

    def save_query(self, df_value):
        self.lock.acquire()
        if self.driver == "oracle":
            df_value.drop(df_value.columns[0], axis=1, inplace=True)

        # if self.ordering:
        #     df_value.columns = [i.lower() for i in df_value.columns.tolist()]
        #     df_value.sort_values([i.strip() for i in self.ordering], inplace=True)
        self.kwargs.update(header=None)
        df_value.to_csv(
            *self.args,
            mode="a",
            **self.kwargs,
        )
        self.tqdm_update()
        self.lock.release()


class ToSql(ReadSql):
    def __init__(self, *args, **kwargs):
        to_db = kwargs.pop("to_db")
        kwargs.update(con=kwargs.pop("from_db"))
        self.to_table = kwargs.pop("to_table")
        self.to_columns = kwargs.pop("to_columns")
        self.if_exists = kwargs.pop("if_exists")
        self.mode = kwargs.pop("mode").lower()
        self.file_path = kwargs.pop("file_path")
        self.delete_cache = kwargs.pop("delete_cache")
        self.save_path = kwargs.pop("save_path", None)
        self.thread_w = kwargs.pop("thread_w", 5)
        self.delete_sql = kwargs.pop("delete_sql", None)
        self.dir_path = None
        self.task_count = None
        self.execute_count = 0
        self.data_processing = kwargs.pop("data_processing", None)
        self.extra_param = kwargs.pop("extra_param", None)
        self.write_desc = kwargs.pop("write_desc", "write the scheduler")
        self.df = kwargs.pop("df", None)
        super().__init__(*args, **kwargs)
        if to_db is not None:
            self.to_db = DB_Pool(to_db, num=8, encoding=kwargs.get("encoding"))
            self.write_driver = self.to_db.driver
        self.lock_b = threading.Lock()
        self.execute_pool = None
        self.w_pool = ThreadPoolExecutor(max_workers=self.thread_w)
        self.task_list = []

    def delete_table(self):
        sql = self.sql.replace(
            re.search(r"from\s+(\S+)", self.sql).group(1), self.to_table
        )
        sql = re.sub("(order|ORDER)\s+(by|BY).*", "", sql)
        sql = "delete " + re.search("from.*", sql, re.I).group()
        con = self.to_db.get_db()
        db = con.cursor()
        if self.delete_sql is not None:
            sql = self.delete_sql
        db.execute(sql)
        con.commit()
        self.to_db.close_db(con)

    def decision(self):
        if self.save_path is None:
            self.dir_path = os.path.join(os.getcwd(), f"{uuid.uuid1()}){self.to_table}")
            os.mkdir(self.dir_path)
        else:
            self.dir_path = self.save_path
        self.file_path = self.dir_path

        if self.avg_list is None:
            con = self.db_pool.get_db()
            df = pd.read_sql(self.sql, con, **self.pd_params)
            file_path = os.path.join(self.dir_path, f"{uuid.uuid1()}.pkl")
            df.to_pickle(file_path)
            if self.mode in ("wr", "rw"):
                self.task_count = 1
                self.tqdm_init(
                    self.task_count, weight=85, mode=True, desc=self.write_desc
                )
                self.insert_db(file_path)
            else:
                self.tqdm_update(count=self.task_count)
            self.db_pool.close_db(con)

        else:
            self.task_count = len(self.avg_list)
            self.tqdm_init(self.task_count, weight=85, mode=True, desc=self.write_desc)
            self.start_thread_read()
            self.get_thread_result()

    def rsync_db(self, *args, **kwargs):
        self.pd_params = kwargs
        self.sql = self.sql.strip().replace("\n", " ")
        if self.to_table is None:
            self.to_table = re.search(r"from\s+(\S+)", self.sql, re.I).group(1)
        if self.if_exists == "delete" and self.mode != "r":
            self.delete_table()
        if self.mode == "w":
            return self.write()
        if self.df is not None:
            self.count = 1
            self.task_count = 1
            self.tqdm_init(1, weight=85, mode="w")
            return self.insert_db(df=self.df)
        self.avg_list = self.verify_sql()
        self.tqdm_init(self.count, weight=85)
        self.decision()
        return "finish"

    def write(self):
        file_list = [
            os.path.join(self.file_path, i) for i in os.listdir(self.file_path)
        ]
        self.task_count = len(file_list)
        self.tqdm_w = tqdm(total=self.task_count, desc=self.write_desc, ncols=80)
        self.task_list.extend(
            [self.w_pool.submit(self.insert_db, i) for i in file_list]
        )
        self.get_thread_result()

    def save_query(self, df_value):
        file_path = os.path.join(self.dir_path, f"{uuid.uuid1()}.pkl")
        if self.driver == "oracle":
            df_value.drop(df_value.columns[0], axis=1, inplace=True)
        df_value.to_pickle(file_path)
        self.tqdm_update()
        self.task_list.append(self.w_pool.submit(self.insert_db, file_path))

    def get_sys_guid_col(self, columns, con):
        col_type = pd.read_sql(
            f"select a.COLUMN_NAME,a.DATA_TYPE,a.DATA_SCALE,a.DATA_DEFAULT from all_tab_cols a where TABLE_NAME='{self.to_table.upper()}'",
            con,
        )
        sys_guid = col_type.loc[
            col_type["DATA_DEFAULT"] == " SYS_GUID() ", "COLUMN_NAME"
        ]
        if not sys_guid.empty:
            columns.remove(sys_guid.values[0])
            return columns
        else:
            return columns

    def get_sql(
        self,
        columns,
    ):
        insert_col = ",".join(columns)
        if self.write_driver == "oracle":
            sql_col = ":" + ",:".join([str(i) for i in range(1, len(columns) + 1)])
            sql = f"insert into {self.to_table}({insert_col}) values({sql_col})"
            return sql

        elif self.write_driver == "mysql":
            sql_col = "%" + ",%".join("s" * len(columns))
            insert_col = ",".join([f"`{i}`" for i in columns])
            sql = f"insert into {self.to_table}({insert_col}) values({sql_col})"
            return sql

    @collection_error
    def insert_db(self, path=None, df=None):
        df = pd.read_pickle(path) if path else df
        if self.columns_upper:
            df.columns = [i.upper() for i in df.columns]
        if self.data_processing is not None:
            if "extra_param" in inspect.signature(self.data_processing).parameters:
                df = self.data_processing(
                    df, extra_param=copy.deepcopy(self.extra_param)
                )
            else:
                df = self.data_processing(df)

        c = [
            (column, str(date))
            for column, date in zip(df.columns.tolist(), df.dtypes)
            if "date" in str(date)
        ]
        if self.write_driver == "mysql":
            for column, date in c:
                df[column] = df[column].astype("str")
                df.replace("NaT", None, inplace=True)
        else:
            for column, date in c:
                df[column] = df[column].astype("object")

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
            if self.mode in ("wr", "rw") and self.save_path is None and self.df is None:
                shutil.rmtree(self.file_path)
        if self.show_progress:
            if self.tqdm is not None:
                self.tqdm.update(self.count - self.tqdm.n)
            if self.tqdm_w is not None:
                self.tqdm_w.update(self.task_count - self.tqdm_w.n)
        sys.stdout.flush()
        self.db_pool.dispose()
