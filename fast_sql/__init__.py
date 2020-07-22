import os
import cx_Oracle
from copy import deepcopy
connect_copy = deepcopy(cx_Oracle.connect)

def connects(*args,**kwargs):
    conn = connect_copy(*args,**kwargs)
    user = kwargs.get('user') or args[0]
    password = kwargs.get('password') or args[1]
    dsn = kwargs.get('dsn') or args[2]
    conn.external_name = f"oracle+cx_oracle://{user}:{password}@{dsn}"
    return conn

cx_Oracle.connect = connects
cx_Oracle.Connection = connects

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
from fast_sql.utils.exception import FILEPATH_Exceptions, TYPE_Exception, MODE_Exception
from fast_sql.fastsql.sql import to_sql as to_SQL
from fast_sql.fastsql.sql import to_csv as to_CSV
from fast_sql.fastsql.sql import Read_sql
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

__version__ = '1.3.0'

def read_sql(sql, con, thread_num=15, encoding='utf8', show_progress=True,
             index_col=None, coerce_float=True, params=None,chunksize=20000,
             parse_dates=None, columns=None,desc='read the scheduler'
             ):
    """
    Read SQL query or database table into a DataFrame.

    This function is a convenience wrapper around ``read_sql_table`` and
    ``read_sql_query`` (for backward compatibility). It will delegate
    to the specific function depending on the provided input. A SQL query
    will be routed to ``read_sql_query``, while a database table name will
    be routed to ``read_sql_table``. Note that the delegated function might
    have more specific notes about their functionality not listed here.

    Parameters
    ----------
    sql : string or SQLAlchemy Selectable (select or text object)
        SQL query to be executed or a table name.
    con : Currently only the following objects are supported
        Oracle | mysql ->sqlalchemy.create_engine object, or
        pymysql -> pymysql.connect object
    thread_num: int, default: 15
        Read the number of threads used, recommended based on memory allocation.
    encoding : str, default UTF8.
    show_progress : bool ,defalut True, Whether to display progress bar
    index_col : string or list of strings, optional, default: None
        Column(s) to set as index(MultiIndex).
    coerce_float : boolean, default True
        Attempts to convert values of non-string, non-numeric objects (like
        decimal.Decimal) to floating point, useful for SQL result sets.
    params : list, tuple or dict, optional, default: None
        List of parameters to pass to execute method.  The syntax used
        to pass parameters is database driver dependent. Check your
        database driver documentation for which of the five syntax styles,
        described in PEP 249's paramstyle, is supported.
        Eg. for psycopg2, uses %(name)s so use params={'name' : 'value'}
    parse_dates : list or dict, default: None
        - List of column names to parse as dates.
        - Dict of ``{column_name: format string}`` where format string is
          strftime compatible in case of parsing string times, or is one of
          (D, s, ns, ms, us) in case of parsing integer timestamps.
        - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
          to the keyword arguments of :func:`pandas.to_datetime`
          Especially useful with databases without native Datetime support,
          such as SQLite.
    columns : list, default: None
        List of column names to select from SQL table (only used when reading
        a table).

    Returns
    -------
    DataFrame

    See also
    --------
    read_sql_table : Read SQL database table into a DataFrame.
    read_sql_query : Read SQL query into a DataFrame.

    """

    assert isinstance(
        thread_num, int), TYPE_Exception(
        'thread_num', value='int')
    assert isinstance(sql, str), TYPE_Exception('sql', value='str')
    assert isinstance(
        show_progress, bool), TYPE_Exception(
        'show_progress', value='bool')

    Fastsql_builder = Read_sql(
        sql,
        con,
        thread_num=thread_num,
        encoding=encoding,
        show_progress=show_progress,chunksize=chunksize,
        desc=desc)

    return Fastsql_builder.read_sql(
        index_col=index_col, coerce_float=coerce_float, params=params,
        parse_dates=parse_dates, columns=columns,
    )


def to_csv(
    sql,
    con,
    path_or_buf=None,
    mode='w',
    encoding='utf8',
    thread_num=15,
    show_progress=True,
    sep=",",
    na_rep='',
    float_format=None,
    columns=None,
    header=True,
    index=True,
    index_label=None,
    compression=None,
    quoting=None,
    quotechar='"',
    line_terminator='\n',
    chunksize=None,
    date_format=None,
    doublequote=True,
    escapechar=None,
    decimal='.',
    desc = 'write the csv'
):
    r"""Write DataFrame to a comma-separated values (csv) file

    Parameters
    ----------
    sql : string or SQLAlchemy Selectable (select or text object)
        SQL query to be executed or a table name.
    con : Currently only the following objects are supported
        Oracle | mysql ->sqlalchemy.create_engine object, or
        pymysql -> pymysql.connect object
    path_or_buf : string or file handle, default None
        File path or object, if None is provided the result is returned as a string.
    thread_num: int, default: 15
        Read the number of threads used, recommended based on memory allocation.
    encoding : str, default UTF8.
    mode : str
        Python write mode, default 'w'
    show_progress : bool ,defalut True, Whether to display progress bar
    sep : character, default ','
        Field delimiter for the output file.
    na_rep : string, default ''
        Missing data representation
    float_format : string, default None
        Format string for floating point numbers
    columns : sequence, optional
        Columns to write
    header : boolean or list of string, default True
        Write out the column names. If a list of strings is given it is
        assumed to be aliases for the column names
    index : boolean, default True
        Write row names (index)
    index_label : string or sequence, or False, default None
        Column label for index column(s) if desired. If None is given, and
        `header` and `index` are True, then the index names are used. A
        sequence should be given if the DataFrame uses MultiIndex.  If
        False do not print fields for index names. Use index_label=False
        for easier importing in R
    compression : string, optional
        A string representing the compression to use in the output file.
        Allowed values are 'gzip', 'bz2', 'zip', 'xz'. This input is only
        used when the first argument is a filename.
    line_terminator : string, default ``'\n'``
        The newline character or character sequence to use in the output
        file
    quoting : optional constant from csv module
        defaults to csv.QUOTE_MINIMAL. If you have set a `float_format`
        then floats are converted to strings and thus csv.QUOTE_NONNUMERIC
        will treat them as non-numeric
    quotechar : string (length 1), default '\"'
        character used to quote fields
    doublequote : boolean, default True
        Control quoting of `quotechar` inside a field
    escapechar : string (length 1), default None
        character used to escape `sep` and `quotechar` when appropriate
    chunksize : int or None
        rows to write at a time
    date_format : string, default None
        Format string for datetime objects
    decimal: string, default '.'
        Character recognized as decimal separator. E.g. use ',' for
        European data

    """
    assert path_or_buf,MODE_Exception(name='PATH_ERROR:',value='The path_or_buf cannot be empty')

    assert isinstance(
        thread_num, int), TYPE_Exception(
        'thread_num', value='int')
    assert isinstance(sql, str), TYPE_Exception('sql', value='str')
    assert isinstance(
        show_progress, bool), TYPE_Exception(
        'show_progress', value='bool')

    Fastsql_builder = to_CSV(
        sql,
        con,
        thread_num=thread_num,
        encoding=encoding,
        show_progress=show_progress,
        desc= desc)

    return Fastsql_builder.build_csv(
        path_or_buf=path_or_buf,
        sep=sep,
        na_rep=na_rep,
        float_format=float_format,
        columns=columns,
        header=header,
        index=index,
        index_label=index_label,
        mode=mode,
        encoding=encoding,
        compression=compression,
        quoting=quoting,
        quotechar=quotechar,
        line_terminator=line_terminator,
        chunksize=chunksize,
        date_format=date_format,
        doublequote=doublequote,
        escapechar=escapechar,
        decimal=decimal,
    )


def to_sql(
        sql,
        from_db=None,
        to_db=None,
        if_exists='delete',
        to_table=None,
        file_path=None,
        mode='rw',
        thread_num=15,
        thread_w=3,
        encoding='utf8',
        show_progress=True,
        to_columns=None,
        chunksize=20000,
        save_path=None,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        delete_cache=True,
        data_processing=None,
        delete_sql=None,
        desc = 'rsync the scheduler',
        **kwargs):
    """
    Read SQL query or database table into a DataFrame.

    This function is a convenience wrapper around ``read_sql_table`` and
    ``read_sql_query`` (for backward compatibility). It will delegate
    to the specific function depending on the provided input. A SQL query
    will be routed to ``read_sql_query``, while a database table name will
    be routed to ``read_sql_table``. Note that the delegated function might
    have more specific notes about their functionality not listed here.

    Parameters
    ----------
    sql : string or SQLAlchemy Selectable (select or text object)
        SQL query to be executed or a table name.
    from_db: The source database
        Currently only the following objects are supported
        Oracle | mysql ->sqlalchemy.create_engine object, or
        pymysql -> pymysql.connect object
    to_db : Target database
        Currently only the following objects are supported
        Oracle | mysql ->sqlalchemy.create_engine object, or
        pymysql -> pymysql.connect object
    if_exists : str -> delete append other
        delete > Delete the target library data
        append > Additional data
        other > target library data don't do processing
    to_table : str Table name to insert
        default is The original name of the table
    file_path : str The path imported from the local
    mode : str > like w or wr or r
        w > Import from local,File_path cannot be null if imported locally.
        wr > Import from database.
        r > Read data is written locally. like a.pkl
    to_columns : list If the target library column name is different, you can specify the column name manually.
    delete_cache : bool default False
        Whether to delete cached files during migration
    thread_num: int, default: 15
        Read the number of threads used, recommended based on memory allocation.
    encoding : str, default UTF8.
    show_progress : bool ,defalut True, Whether to display progress bar

    index_col : string or list of strings, optional, default: None
        Column(s) to set as index(MultiIndex).
    coerce_float : boolean, default True
        Attempts to convert values of non-string, non-numeric objects (like
        decimal.Decimal) to floating point, useful for SQL result sets.
    params : list, tuple or dict, optional, default: None
        List of parameters to pass to execute method.  The syntax used
        to pass parameters is database driver dependent. Check your
        database driver documentation for which of the five syntax styles,
        described in PEP 249's paramstyle, is supported.
        Eg. for psycopg2, uses %(name)s so use params={'name' : 'value'}
    parse_dates : list or dict, default: None
        - List of column names to parse as dates.
        - Dict of ``{column_name: format string}`` where format string is
          strftime compatible in case of parsing string times, or is one of
          (D, s, ns, ms, us) in case of parsing integer timestamps.
        - Dict of ``{column_name: arg dict}``, where the arg dict corresponds
          to the keyword arguments of :func:`pandas.to_datetime`
          Especially useful with databases without native Datetime support,
          such as SQLite.
    columns : list, default: None
        List of column names to select from SQL table (only used when reading
        a table).
    thread_num: int, default: 15
        Read the number of threads used, recommended based on memory allocation.
    encoding : str, default UTF-8

    Returns
    -------
    DataFrame

    See also
    --------
    read_sql_table : Read SQL database table into a DataFrame.
    read_sql_query : Read SQL query into a DataFrame.

    """

    assert isinstance(
        thread_num, int), TYPE_Exception(
        'thread_num', value='int')
    assert isinstance(
        show_progress, bool), TYPE_Exception(
        'show_progress', value='bool')

    if to_table is not None:
        assert isinstance(to_table, str), TYPE_Exception('to_table', value='str')

    assert isinstance(if_exists, str), TYPE_Exception('if_exists', value='str')
    assert isinstance(
        mode, str), TYPE_Exception(
        'mode', value='str like w | rw | r')
    assert if_exists in ('delete', 'append', 'other'), MODE_Exception(
        'if_exists ERROR:', value="The if_exists must be is 'delete' or 'append' or 'other'")

    if to_columns is not None:
        assert isinstance(
            to_columns, list), TYPE_Exception(
            'to_columns', value='str,None')

    if mode.lower() == 'w':
        assert file_path is not None, MODE_Exception(
            'mode ERROR: ', value='if mode == w or W ,The file_path must exist')
        assert os.path.exists(file_path), FILEPATH_Exceptions(
            'FileNotFoundError', file_path)
    if mode.lower() in ['wr','rw','r']:
        assert isinstance(sql, str), TYPE_Exception('sql', value='str')

    Fastsql_builder = to_SQL(
        sql,
        from_db=from_db,
        to_db=to_db,
        to_table=to_table,
        to_columns=to_columns,
        if_exists=if_exists,
        thread_num=thread_num,
        encoding=encoding,
        mode=mode,
        chunksize=chunksize,
        delete_cache=delete_cache,
        show_progress=show_progress,
        file_path=file_path,
        save_path=save_path,
        thread_w=thread_w,
        data_processing= data_processing,
        delete_sql = delete_sql,
        desc = desc,
        **kwargs)

    return Fastsql_builder.rsync_db(
        index_col=index_col, coerce_float=coerce_float, params=params,
        parse_dates=parse_dates, columns=columns,
    )
