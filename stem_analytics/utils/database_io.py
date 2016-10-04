"""Collection of I/O Database functionality."""
import errno
import codecs
import sqlite3
import contextlib
from typing import List

import pandas as pd

from stem_analytics.utils import strings, paths


@contextlib.contextmanager
def connect_to_db(db_path: str) -> sqlite3.Connection:
    """Handles sqlite3 connection at given path fails if invalid/non-existent path.

    Notes:
        If file wasn't present, this function will NOT create the file,
        in contrast to sqlite3.connect(db_path) (which creates file almost always).
    """
    db_path_ = paths.normalize_path(db_path)
    db_existed_before_connection = paths.file_exists(db_path_)
    try:
        ensure_sql_file_exists_and_is_not_corrupt(db_path_)
        yield sqlite3.connect(db_path_)             # may raise in specific, rare cases
    except Exception:
        if not db_existed_before_connection:        # if db file wasn't present before connection
            paths.remove_file_if_present(db_path_)  # remove faultily auto-created file if exists
        raise ConnectionRefusedError(errno.ECONNREFUSED,
                                     '\'{}\' cannot be reached'.format(paths.get_basename(db_path)))


def read_database_table_as_df(con: sqlite3.Connection, table_name: str,
                              datetime_format: str=None) -> pd.DataFrame:
    """Retrieve table of given name from database as a df."""
    index_name = select_column_names(con, table_name)[0]
    return pd.read_sql(sql='SELECT * FROM ' + table_name, con=con, index_col=index_name,
                       parse_dates={index_name: datetime_format})


def write_df_to_database(con: sqlite3.Connection, df: pd.DataFrame,
                         new_table_name: str, if_exists: str='fail') -> None:
    """Write contents of DataFrame to sql.

    Notes:
        - action_if_exists : {'fail', 'replace', 'append'}, default='fail'.
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
    sql_path_ = paths.normalize_path(sql_file_path)
    if paths.get_basename(sql_path_, with_ext=True) == sql_path_:  # if only basename given, raise
        raise ValueError('Valid file\'s are considered only with explicit file path.')
    if replace_if_exists:
        paths.remove_file_if_present(sql_path_)

    paths.ensure_creatable_file_path(sql_path_, valid_file_types=['sql'])
    try:
        sqlite3.connect(sql_path_)  # establish file path
        connect_to_db(sql_path_)
        ensure_sql_file_exists_and_is_not_corrupt(sql_path_)
    except:
        paths.remove_file_if_present(sql_path_)  # remove faultily auto-created file if exists
        raise ValueError('Cannot create sql file - '
                         'valid connection to \'{}\' cannot be established.'
                         .format(sql_path_))


def ensure_sql_file_exists_and_is_not_corrupt(sql_file_path: str) -> None:
    """Raise if invalid new path, else raise if invalid existent file.

    Notes:
        Valid creatable file: available sql file path in an existing directory.
        Valid existent file: existing, non-corrupt, recognizable SQL file.
    See Also:
        For more details on method used to determine corruption/validity of sql file, go to
        section 'magic header string' on 'http://www.sqlite.org/fileformat.html'.
    """
    sql_path_ = paths.normalize_path(sql_file_path)
    if paths.get_basename(sql_path_, with_ext=True) == sql_path_:
        raise ValueError('Valid file\'s are considered only with explicit file path.') from None
    paths.ensure_file_path_exists(file_path=sql_file_path, valid_file_types=['sql'])
    try:
        with codecs.open(paths.normalize_path(sql_file_path), 'r', 'UTF-8') as sql_file:
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
