"""Collection of I/O Database functionality."""
import json
import errno
import codecs
import sqlite3
import contextlib
from typing import List, Any

import pandas as pd

from stem_center_analytics.utils import os_lib


@contextlib.contextmanager
def connect_to_db(db_path: str) -> sqlite3.Connection:
    """Handles sqlite3 connection at given path fails if invalid/non-existent path.

    Notes
    -----
    * If file wasn't present, this function will NOT create the file,
      in contrast to sqlite3.connect(db_path) (which creates file almost always).
    """
    db_path_ = os_lib.normalize_path(db_path)
    db_existed_before_connection = os_lib.is_existent_file(db_path_)
    try:
        ensure_sql_file_exists_and_is_not_corrupt(db_path_)
        yield sqlite3.connect(db_path_)             # may raise in specific, rare cases
    except Exception:
        if not db_existed_before_connection:        # if db file wasn't present before connection
            os_lib.remove_file(db_path_)  # remove faultily auto-created file if exists
        raise ConnectionRefusedError(errno.ECONNREFUSED,
                                     '\'{}\' cannot be reached'.format(os_lib.get_basename(db_path)))


def read_database_table_as_df(con: sqlite3.Connection, table_name: str,
                              datetime_format: str=None) -> pd.DataFrame:
    """Retrieve table of given name from database as a df."""
    index_name = select_column_names(con, table_name)[0]
    return pd.read_sql(sql='SELECT * FROM ' + table_name, con=con, index_col=index_name,
                       parse_dates={index_name: datetime_format})


def write_df_to_database(con: sqlite3.Connection, df: pd.DataFrame,
                         new_table_name: str, if_exists: str='fail') -> None:
    """Write contents of DataFrame to sql.

    Notes
    -----
    action_if_exists : {'fail', 'replace', 'append'}, default='fail'.
    """
    normalized_name = new_table_name.replace('-', '').replace(' ', '').replace('_', '')
    if not normalized_name.isalnum():  # contains only letters, digits, '_', ' ', '-'
        raise ValueError('Given table name \'{}\' is invalid - only letters, digits, spaces,'
                         ' dashes, and underscores are allowed.'.format(new_table_name))

    table_is_in_db = is_table_in_database(con, new_table_name)
    actions = ('fail', 'append', 'replace')
    if if_exists not in actions:
        raise ValueError('`action_if_exists` must be in {\'fail\', \'replace\', \'append\'}.')
    try:
        df.to_sql(name=new_table_name, con=con, if_exists=if_exists)
    except Exception as e:
        if isinstance(e, ValueError):  # catch if_exists='fail' exception
            pass
        else:
            raise IOError('Internal error: DataFrame could not be imported to database'
                          'present at \'{}\'.'.format(con)) from None

    # generate a report for the database update
    deciding_factors = (if_exists, table_is_in_db)
    outcomes = {
        ('fail', True): 'Table \'{}\' cannot be created - if_exists=\'fail\'',
        ('fail', False): 'Table \'{}\' successfully created',
        ('append', True): 'Table \'{}\' successfully appended to',
        ('append', False): 'Cannot append to non-existent table \'{}\' - created instead',
        ('replace', True): 'Table \'{}\' successfully replaced',
        ('replace', False): 'Cannot replace non-existent table \'{}\' - created instead'
    }
    outcome = outcomes[deciding_factors].format(new_table_name)
    print('\nLatest changes to db @ {}...'
          '\n   {} ({:,} row changes).'
          .format(con, outcome, con.total_changes))


def create_sql_file(sql_file_path: str, replace_if_exists: bool=False) -> None:
    """Create SQL file if path doesn't exist in existent directory, and connection is successful."""
    sql_path_ = os_lib.normalize_path(sql_file_path)
    if os_lib.get_basename(sql_path_, with_ext=True) == sql_path_:  # if only basename given, raise
        raise ValueError('Valid file\'s are considered only with explicit file path.')
    if replace_if_exists:
        os_lib.remove_file(sql_path_)

    os_lib.ensure_file_is_creatable(sql_path_, valid_file_types=['sql'])
    try:
        sqlite3.connect(sql_path_)  # establish file path
        connect_to_db(sql_path_)
        ensure_sql_file_exists_and_is_not_corrupt(sql_path_)
    except:
        os_lib.remove_file(sql_path_)  # remove faultily auto-created file if exists
        raise ValueError('Cannot create sql file - '
                         'valid connection to \'{}\' cannot be established.'
                         .format(sql_path_))


def ensure_sql_file_exists_and_is_not_corrupt(sql_file_path: str) -> None:
    """Raise if invalid new path, else raise if invalid existent file.

    Notes
    -----
    * Valid creatable file: available sql file path in an existing directory.
    * Valid existent file: existing, non-corrupt, recognizable SQL file.

    References
    ----------
    For more details on method used to determine corruption/validity of sql
    file, go to section 'magic header string' on the official sqlite website,
    'http://www.sqlite.org/fileformat.html'.
    """
    sql_path_ = os_lib.normalize_path(sql_file_path)
    if os_lib.get_basename(sql_path_, with_ext=True) == sql_path_:
        raise ValueError('Valid file\'s are considered only with explicit file path.') from None
    os_lib.ensure_file_exists(file_path=sql_file_path, valid_file_types=['sql'])
    try:
        with codecs.open(os_lib.normalize_path(sql_file_path), 'r', 'UTF-8') as sql_file:
            if codecs.encode(sql_file.read(16)) == '53514c69746520666f726d6174203300':
                raise OSError('SQL file \'{}\' cannot be read - file corrupted.') from None
    except (UnicodeError, LookupError):
        raise UnicodeError('Only UTF-8 sql file encodings are supported.') from None


def is_table_in_database(con: sqlite3.Connection, table_name: str) -> bool:
    """Return True if  ValueError if exact given table_name is not in con."""
    try:
        ensure_table_is_in_database(con, table_name)
        return True
    except ValueError:
        return False


def ensure_table_is_in_database(con: sqlite3.Connection, table_name: str) -> None:
    """Raise ValueError if exact given table_name is not in con."""
    table_query = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?'
    is_table_in_database_ = bool(con.execute(table_query, [table_name]).fetchone())
    if not is_table_in_database_:
        raise ValueError('Table does not exist in database currently connected to.'.format(con))


def select_column_names(con: sqlite3.Connection, table_name: str) -> List[str]:
    """Return column names from table in db con."""
    ensure_table_is_in_database(con, table_name)
    cursor = con.execute("SELECT * FROM " + table_name)
    return tuple([col[0] for col in cursor.description])


def read_json_file(file_path: str) -> Any:
    """Read json file from given path, empty dict if empty file."""
    file_path_ = os_lib.normalize_path(file_path)
    os_lib.ensure_file_exists(file_path_, valid_file_types=['json'])
    if os_lib.is_empty_file(file_path):
        return {}
    with open(os_lib.normalize_path(file_path), 'r') as json_file:
        return json.load(json_file)


def write_json_file(file_path: str, contents: object) -> None:
    """Write given contents to json file.

    Any combination of list and dict (or OTHER_SUBJECTS json supported data types),
    are allowed. File is overwritten if exists.
    """
    with open(os_lib.normalize_path(file_path), 'w') as json_file:
        json_file.write(json.dumps(contents))


def read_flat_file_as_df(file_path: str, datetime_format: str=None) -> pd.DataFrame:
    """Fetch flat file from given path as a pandas DF. Supported: .csv, and .json.

    Infers if `datetime_format` (eg '%Y-%m-%d %H:%M:%S'), is not given.
    """
    dt_format = datetime_format.strip(' ') if datetime_format else None
    date_unit = 's' if dt_format and dt_format.endswith('%S') else 'ms'  # infer type from format
    file_to_df_mappings = {
        '.csv': lambda: pd.read_csv(file_path, index_col=0, parse_dates=True,
                                    date_parser=lambda d: pd.to_datetime(d, format=datetime_format),
                                    encoding='utf8', infer_datetime_format=True),
        '.json': lambda: pd.read_json(file_path, date_unit=date_unit)
    }

    # if completely empty file, return completely empty dataframe
    os_lib.ensure_file_exists(file_path, valid_file_types=file_to_df_mappings.keys())
    if os_lib.is_empty_file(file_path):
        return pd.DataFrame()
    return file_to_df_mappings[os_lib.get_extension(file_path)]()  # call corresponding reader


def write_df_to_flat_file(file_path: str, df: pd.DataFrame, replace_if_exists: bool = False,
                          datetime_format: str = None) -> None:
    """Write given DF to flat file at given location. Supported: .csv, and .json.

    Infers if `datetime_format` (eg '%Y-%m-%d %H:%M:%S'), is not given.
    """
    dt_format = datetime_format.strip(' ') if datetime_format else None
    date_unit = 's' if dt_format and dt_format.endswith('%S') else 'ms'  # infer type from format
    df_to_file_mappings = {
        '.csv': lambda: df.to_csv(file_path, date_format=dt_format),
        '.json': lambda: df.to_json(file_path, date_format=dt_format, date_unit=date_unit)
    }

    if replace_if_exists and os_lib.is_existent_file(file_path):
        os_lib.remove_file(file_path)
    os_lib.ensure_file_is_creatable(file_path, valid_file_types=df_to_file_mappings.keys())

    # if completely empty dataframe, create completely empty file
    if df.empty:
        with open(os_lib.normalize_path(file_path), 'w'):
            pass
    else:
        return df_to_file_mappings[os_lib.get_extension(file_path)]()  # call corresponding writer


# todo: modify sql related functions to handle empty dataframes..
