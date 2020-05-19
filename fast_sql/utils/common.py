import re
import traceback
from DBUtils.PooledDB import PooledDB
import cx_Oracle
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from fast_sql.utils.exception import DB_Exceptions


class DB_Pool:

    mysql_url = ['user', 'password', 'port', 'host', 'db']
    def __init__(self, con, init_num=0, num=15, encoding=None):
        self.con = con
        db_engine = self.get_db_api(self.con)
        self.db_type = db_engine[0]
        self.db_config = db_engine[1]
        self.config_dict = {'mincached':init_num,'maxcached':num,'maxshared':num,'maxconnections':num,
                            'blocking':True,'encoding':encoding}

        if self.db_type == cx_Oracle:
            try:
                self.db_pool = PooledDB(
                    self.db_type,
                    **self.db_config,**self.config_dict)
            except Exception:
                raise DB_Exceptions('DB_CONNECT:')

            self.driver = 'oracle'

        else:
            self.config_dict['charset'] = self.config_dict.pop('encoding')
            self.db_pool = PooledDB(
                self.db_type,**self.config_dict,
                **self.db_config,
               )
            self.driver = 'mysql'

    def get_db_api(self, con):
        if isinstance(con, pymysql.connections.Connection):
            db_engine = {i: con.__dict__.get(i) for i in self.mysql_url}
            return pymysql, db_engine

        elif isinstance(con, sqlalchemy.engine.base.Engine):
            return self.classification(con)

        elif isinstance(con,str):
            self.con = create_engine(con)
            return self.classification(self.con)

        else:
            raise DB_Exceptions("DB_CONNECT:")

    def make_dsn(self,dsn):
        if 'DESCRIPTION' in dsn.upper():
            return dsn
        else:
            _dsn = re.search(r'(.+):(\d+)/(\w+)',dsn)
            return cx_Oracle.makedsn(*_dsn.groups())


    def classification(self, con):
        if "oracle" in con.driver:
            _url = con.url
            dsn = str(_url).split('@')[1]
            db_engine = {
                "user": _url.username,
                'password': _url.password_original,
                'dsn': self.make_dsn(dsn)}
            return cx_Oracle, db_engine

        elif 'mysql' in con.driver:
            _url = con.url
            db_engine = {
                     "user": _url.username,
                     "password": _url.password_original,
                     "port": _url.port,
                     "host": _url.host,
                     "db": _url.database}
            return pymysql, db_engine


    def close_db(self, con):
        con.close()

    def get_db(self):
        con = self.db_pool.connection()
        return con


class Sqlalchemy_Pool(DB_Pool):

    def __init__(self, con,num=15, encoding='utf8'):
        self.con = con
        db_engine = self.get_db_api(self.con)
        self.db_type = db_engine[0]
        self.db_config = db_engine[1]

        try:
            self.db_pool =  create_engine(
                            self.db_config,
                            max_overflow = 0,
                            pool_size = num,
                            pool_timeout = 10,
                            pool_recycle = -1,
                            encoding = encoding,
                    )
        except Exception as e:
            raise DB_Exceptions('DB_CONNECT:')

        if self.db_type == cx_Oracle:
            self.driver = 'oracle'
        else:
            self.driver = 'mysql'

    def classification(self, con):
        if "oracle" in con.driver:
            _url = con.url
            dsn = str(_url).split('@')[1]
            user = _url.username
            password = _url.password_original
            dsn =  self.make_dsn(dsn)
            return cx_Oracle, f'oracle+cx_oracle://{user}:{password}@{dsn}'

        elif 'mysql' in con.driver:
            _url = con.url
            user = _url.username
            password = _url.password_original
            port = _url.port
            host = _url.host
            db = _url.database
            return pymysql, f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"

    def close_db(self, con):
        pass

    def get_db(self):
        con = self.db_pool
        return con


def collection_error(fun):
    def hander(*args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            raise e

    return hander

