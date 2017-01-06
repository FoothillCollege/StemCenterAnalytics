"""Collection of I/O Database functionality.

Notes
-----
* All database IO functions, unless otherwise specified in its docs,
  raise ValueError if table does not exist.
* Minimal usage of neighboring functions in this module reduce call-stack
  complexity.
"""
import json
import errno
import email
import codecs
import imaplib
import sqlite3
from functools import partial
from typing import Sequence, Union, List, Any

import pandas as pd

from stem_center_analytics.utils import os_lib


def connect_to_db(db_path: str) -> sqlite3.Connection:
    """Return sqlite3 connection at given path.

    Parameters
    ----------
    db_path : str
        File path to sqlite database

    Return
    ------
    `sqlite3.Connection`
        sqlite connection to database located at given path

    Raises
    ------
    ConnectionRefusedError
        * If database file is not a sqlite corrupt or does not exist
        * If `sqlite3.Connection` object instantiation fails

    Notes
    -----
    * If file wasn't present, this function will NOT create the file,
      in contrast to sqlite3.connect(db_path), which creates file almost always
    """
    db_path_ = os_lib.normalize_path(db_path)
    db_existed_before_connection = os_lib.is_existent_file(db_path_)

    # in the case of a corrupt or non-existent sqlite file,
    # re-raise as ConnectionRefusedError and remove db file if created during failed connection
    try:
        ensure_valid_sqlite_file(db_path_)
        return sqlite3.Connection(db_path_)
    except Exception:
        if not db_existed_before_connection:
            os_lib.remove_file(db_path_)
        raise ConnectionRefusedError(errno.ECONNREFUSED,
                                     'SQLite file \'{}\' cannot be reached'
                                     .format(os_lib.get_basename(db_path)))


def create_sql_file(sql_file_path: str, replace_if_exists: bool=False) -> None:
    """Create SQL file if path is available and connection is successful."""
    sql_file_path_ = os_lib.normalize_path(sql_file_path)
    os_lib.ensure_path_is_absolute(sql_file_path_)
    file_exists_before_creation = os_lib.is_existent_file(sql_file_path_)

    if file_exists_before_creation and replace_if_exists:
        os_lib.remove_file(sql_file_path_, ignore_errors=False)

    os_lib.ensure_file_is_creatable(sql_file_path_, valid_file_types=['sql'])
    try:
        sqlite3.connect(sql_file_path_)  # establish file path
        connect_to_db(sql_file_path_)
        ensure_valid_sqlite_file(sql_file_path_)
    except:
        os_lib.remove_file(sql_file_path_)  # remove faultily auto-created file if exists
        raise ValueError('Cannot create database - '
                         'valid connection to \'{}\' cannot be established.'
                         .format(sql_file_path_))

    # generate a report for the database update
    action_taken = 'replaced' if file_exists_before_creation else 'created'
    print('Database \'{}\' was successfully {}.'.format(os_lib.get_basename(sql_file_path), action_taken))


def ensure_valid_sqlite_file(sql_file_path: str) -> None:
    """Ensure absolute path to an existing, non-corrupt SQLite file.

    Path is considered valid - and thus no errors are raised - if absolute
    path with .sql extension, leading to a UTF-8 encoded SQLite file.

    Parameters
    ----------
    sql_file_path : str
        Absolute file path to a SQLite database

    Raises
    ------
    ValueError
        * If only file name is given, since absolute file paths are required
        * If file name does not end with a single .sql extension
    FileNotFoundError
        * If file does not exist
    OSError
        * If file at path is corrupt or cannot be recognized as a SQLite file,
          as determined by it's header string
    UnicodeError
        * If file is not encoded with UTF-8

    References
    ----------
    * For more details on method used to determine corruption/validity of sql
      file, go to section 'magic header string' on the official sqlite website,
      'http://www.sqlite.org/fileformat.html'
    """
    sql_file_path_ = os_lib.normalize_path(sql_file_path)
    os_lib.ensure_path_is_absolute(sql_file_path_)
    os_lib.ensure_file_exists(file_path=sql_file_path_, valid_file_types=['sql'])
    try:
        with codecs.open(sql_file_path_, 'r', 'UTF-8') as sql_file:
            if codecs.encode(sql_file.read(16)) == '53514c69746520666f726d6174203300':
                raise OSError('File \'{}\' is either corrupted or not a recognizable '
                              'SQLite file.'.format(sql_file_path_)) from None
    except (UnicodeError, LookupError):
        raise UnicodeError('Only UTF-8 sql file encodings are supported.') from None


def is_table_in_database(con: sqlite3.Connection, table_name: str) -> bool:
    """Return True if  ValueError if exact given table_name is not in con."""
    try:
        ensure_table_is_in_database(con, table_name)
        return True
    except ValueError:
        return False


def ensure_table_is_in_database(con: sqlite3.Connection, table_name: str,
                                columns_to_select: Sequence[str]=None) -> None:
    """Raise ValueError if exact given table_name is not in con. If columns_to_select check for existence."""
    table_query = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?'
    is_table_in_database_ = bool(con.execute(table_query, [table_name]).fetchone())
    if not is_table_in_database_:
        raise ValueError('Table \'{}\' does not exist @ database \'{}\'.'.format(table_name, con))

    if columns_to_select:
        cursor = con.execute("SELECT * FROM " + table_name)
        columns_in_table = [col[0] for col in cursor.description]
        set_of_columns = set(columns_to_select)
        if len(columns_to_select) != len(set_of_columns) or not set_of_columns.issubset(columns_in_table):
            raise ValueError('{} is invalid - `columns_to_select` are not a '
                             'unique subset of the columns {} in the table \'{}\'.'
                             .format(tuple(columns_to_select), tuple(columns_in_table), table_name))


def get_all_columns_in_table(con: sqlite3.Connection, table_name: str) -> List[str]:
    """Return column names from table in db con."""
    ensure_table_is_in_database(con, table_name)
    cursor = con.execute("SELECT * FROM " + table_name)
    return [col[0] for col in cursor.description]


def write_df_to_database(con: sqlite3.Connection, df: pd.DataFrame,
                         table_name: str, if_exists: str='fail') -> None:
    """Write contents of DataFrame to sqlite table.

    Parameters
    ----------
    con : sqlite3.Connection
        Database connection object for sqlite3
    df : pd.DataFrame
        DataFrame to write to sql table
    table_name : str
        Name of table to update or create
    if_exists : str of {'fail', 'replace', 'append'}, default 'fail'
        * fail: if table exists, do nothing else create table
        * replace: if table exists drop it and recreate table else create table
        * append: if table exists, insert at end of table, else do nothing

    Notes
    -----
    * `if_exists` options do not raise any errors, rather they dictate the
      corresponding message that is logged/printed to console
    * Action taken and total number of row changes are logged accordingly
    * Note that `if_exists` options follow different semantics than the
      parameter of the same name in the method `DataFrame.to_sql`
    """
    normalized_name = table_name.replace('-', '').replace(' ', '').replace('_', '')
    if not normalized_name.isalnum():  # contains only letters, digits, '_', ' ', '-'
        raise ValueError('Given table name \'{}\' is invalid - only letters, digits, spaces,'
                         ' dashes, and underscores are allowed.'.format(table_name))

    table_is_in_db = is_table_in_database(con, table_name)
    actions = ('fail', 'append', 'replace')
    if if_exists not in actions:
        raise ValueError('`action_if_exists` must be in {\'fail\', \'replace\', \'append\'}.')
    if if_exists == 'append':
        ensure_table_is_in_database(con, 'tutor_requests')

    try:
        df.to_sql(name=table_name, con=con, if_exists=if_exists)
    except Exception as e:
        if isinstance(e, ValueError):  # catch if_exists='fail' exception
            pass
        else:
            raise IOError('DataFrame failed to be imported as table \'{}\' in the database '
                          'present at \'{}\'.'.format(table_name, con))

    # generate a report for the database update
    deciding_factors = (if_exists, table_is_in_db)
    outcomes = {
        ('fail', True): 'Table \'{}\' cannot be created - if_exists=\'fail\'',
        ('fail', False): 'Table \'{}\' successfully created',
        ('append', True): 'Table \'{}\' successfully appended to',
        ('append', False): 'Cannot append to non-existent table \'{}\' - append failed',
        ('replace', True): 'Table \'{}\' successfully replaced',
        ('replace', False): 'Cannot replace non-existent table \'{}\' - created instead'
    }
    outcome = outcomes[deciding_factors].format(table_name)
    print('\nLatest changes to db @ {}...'
          '\n   {} ({:,} row changes).'
          .format(con, outcome, con.total_changes))


def read_database_table_as_df(con: sqlite3.Connection, table_name: str, index: str,
                              as_unique: bool, columns_to_use: Sequence[str]=None,
                              date_columns: Sequence[str]=None) -> pd.DataFrame:
    """Retrieve table of given name from database as a df.

    Takes first columns_to_use element as index.
    columns_to_use: subset of columns_to_use to retrieve
    parse_dates: mapping of column names to inferred datetime formats
    """
    # todo: complete docstring...
    ensure_table_is_in_database(con, table_name, columns_to_use)
    index = [index] if index else None

    query = 'SELECT DISTINCT ' if as_unique else 'SELECT '
    query += ', '.join(columns_to_use) if columns_to_use else '*'
    query += ' FROM ' + table_name
    return pd.read_sql(sql=query, con=con, index_col=index, parse_dates=date_columns)


def write_df_to_flat_file(file_path: str, df: pd.DataFrame, replace_if_exists: bool=False,
                          datetime_format: str=None) -> None:
    """Write given DF to flat file at given location. Supported: .csv, and .json.

    Infers if `datetime_format` (eg '%Y-%m-%d %H:%M:%S'), is not given.
    """
    # todo: improve json reading to be more flexible, and finish docstring
    dt_format = datetime_format.strip(' ') if datetime_format else None
    date_unit = 's' if dt_format and dt_format.endswith('%S') else 'ms'  # infer type from format
    df_to_file_mappings = {
        '.csv': partial(df.to_csv, path_or_buf=file_path, date_format=dt_format),
        '.json': partial(df.to_json, path_or_buf=file_path, date_format=dt_format, date_unit=date_unit)
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



def read_database_table_as_df_(con: sqlite3.Connection, table_name: str, index_column: str,
                               select_distinct: bool,
                               columns_to_use: Sequence[str]=None,
                               date_columns: Sequence[str]=None) -> pd.DataFrame:
    pass


def read_flat_file_as_df(file_path: str, date_columns: Sequence[Union[str, int]]=None) -> pd.DataFrame:
    """Fetch flat file from given path as a pandas DF. Supported: .csv, and .json.

    `columns_to_use`: huge speedup for csv, no noticeable effect for json
    index inferred from first column of `columns_to_use`
    """
    file_to_df_mappings = {
        '.csv': partial(pd.read_csv, filepath_or_buffer=file_path, parse_dates=date_columns,
                        encoding='utf8', infer_datetime_format=True),
        '.json': partial(pd.read_json, path_or_buf=file_path, convert_dates=date_columns)
    }
    # if completely empty file, return completely empty DataFrame
    os_lib.ensure_file_exists(file_path, valid_file_types=file_to_df_mappings.keys())
    if os_lib.is_empty_file(file_path):
        return pd.DataFrame()
    return file_to_df_mappings[os_lib.get_extension(file_path)]()  # call corresponding reader


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


def connect_to_imap_server(server_host: str, user_name: str,
                           user_password: str) -> imaplib.IMAP4_SSL:
    """Connect to an IMAP server - note, the server must have app security settings lowered."""
    # note: use con.uid for dealing with unique email ids (like in this project), and
    # put the command (eg STORE) in quotes as the first argument like con.uid('STORE', ...)
    # instead of con.store(..), in the case of a non unique (sequential) id
    # todo: add further docs, detailing assumptions, requirements, imap, gmail, etc.
    connection_client = imaplib.IMAP4_SSL(server_host)
    connection_client.login(user_name, user_password)
    connection_client.select()
    return connection_client


def download_all_email_attachments(imap_connection: imaplib.IMAP4_SSL, email_uid: str,
                                   output_dir: str) -> List[str]:
    """Download all attachment files for a given unique email id.

    Return downloaded file paths, whether the download(s) were successful or not.
    """
    # todo: add further docs, detailing assumptions, requirements, imap, gmail, etc.
    file_paths = []
    email_body = imap_connection.uid('FETCH', email_uid, '(RFC822)')[1][0][1]  # read the message
    message = email.message_from_bytes(email_body)
    for part in message.walk():
        if part.get_content_maintype() != 'MULTIPART' and part.get('CONTENT-DISPOSITION'):
            file_path = os_lib.join_path(output_dir, part.get_filename())
            file_paths.append(file_path)
            with open(file_path, 'wb') as output_file:
                output_file.write(part.get_payload(decode=True))
    return file_paths


def get_unread_email_uids(imap_connection: imaplib.IMAP4_SSL, sender: str='',
                          subject: str='') -> List[str]:
    """Return list of unique ids from newest to oldest matching given sender and subject."""
    sender_ = 'FROM ' + sender if sender else None
    subject_ = 'SUBJECT ' + subject if subject else None
    email_uid = imap_connection.uid('SEARCH', sender_, subject_, 'UNSEEN')[1][0].split()
    return email_uid


# todo: modify sql related functions to handle empty dataframes...
