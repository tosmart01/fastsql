
class DB_Exceptions(Exception):
    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    def __str__(self):
        error_format = {"DB_CONNECT:":
                        f'''Currently only the following objects are supported
                                Oracle | mysql ->sqlalchemy.create_engine object, or
                                pymysql -> pymysql.connect object
                            ''',
                        "DB_EMPTY:": ''' The query result is empty. ''',
                        }
        return error_format.get(self.name)


class FILEPATH_Exceptions(DB_Exceptions):

    def __str__(self):

        return f'''{self.name}'[Errno 2] No such file or directory:{self.value}'''


class TYPE_Exception(DB_Exceptions):

    def __str__(self):
        return f''' The {self.name} must be an {self.value} object '''


class MODE_Exception(DB_Exceptions):

    def __str__(self):
        return f'''{self.name}{self.value}'''
