import traceback
from DBUtils.PooledDB import PooledDB
import cx_Oracle
import pymysql
import sqlalchemy
from fast_sql.utils.exception import DB_Exceptions


class DB_Pool:

    def __init__(self, con, init_num=0, num=15, encoding=None):
        db_engine = self.get_db_api(con)
        self.db_type = db_engine[0]
        self.db_config = db_engine[1]

        if "oracle" in con.driver:
            try:
                self.db_pool = PooledDB(
                    self.db_type,
                    **self.db_config,
                    mincached=init_num,
                    maxcached=num,
                    maxshared=num,
                    maxconnections=num,
                    blocking=True,
                    encoding=encoding)
            except Exception as e:
                try:
                    self.db_config['dsn'] = cx_Oracle.makedsn(
                        self.db_config['dsn'])
                    self.db_pool = PooledDB(
                        self.db_type,
                        **self.db_config,
                        mincached=init_num,
                        maxcached=num,
                        maxshared=num,
                        maxconnections=num,
                        blocking=True,
                        encoding=encoding)
                except BaseException:
                    raise DB_Exceptions('DB_CONNECT:')
            self.driver = 'oracle'

        else:
            self.db_pool = PooledDB(
                self.db_type,
                **self.db_config,
                mincached=init_num,
                maxcached=num,
                maxshared=num,
                maxconnections=num,
                blocking=True,
                charset=encoding)
            self.driver = 'mysql'

    def get_db_api(self, con):
        if isinstance(con, pymysql.connections.Connection):
            db_engine = {i: con.__dict__.get(i) for i in self.mysql_url}
            return pymysql, db_engine

        elif isinstance(con, sqlalchemy.engine.base.Engine):
            return self.classification(con)
        else:
            raise DB_Exceptions("DB_CONNECT:")

    def classification(self, con):
        if "oracle" in con.driver:
            _url = con.url
            dsn = str(_url).split('@')[1]
            db_engine = {
                "user": _url.__dict__.get('username'),
                'password': _url.__dict__.get('password_original'),
                'dsn': dsn}
            return cx_Oracle, db_engine

        elif 'mysql' in con.driver:
            _url = con.url.__dict__
            print(_url)
            db_engine = {
                "user": _url.get('username'),
                "password": _url.get("password_original"),
                "port": _url.get("port"),
                "host": _url.get("host"),
                "db": _url.get("database")}
            return pymysql, db_engine

    def close_db(self, con):
        con.close()

    def get_db(self):
        con = self.db_pool.connection()
        return con


def collection_error(fun):
    def hander(*args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            raise e

    return hander
