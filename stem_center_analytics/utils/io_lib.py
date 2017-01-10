"""Collection of I/O Database functionality.

Notes
-----
* All database IO functions, unless otherwise specified in its docs,
  raise ValueError if table does not exist.
* Minimal usage of neighboring functions in this module reduce call-stack
  complexity.
* All paths are ensured and error checked according to their IO function
  (write, create, read, etc), as well as checked to be absolute paths.
  This is all done in an effort to reduce mistaken replacements, and generally
  increase IO robustness.
"""
import csv
import json
import errno
import email
import codecs
import imaplib
import sqlite3
from typing import Sequence, Union, List, Any

import numpy as np
import pandas as pd

from stem_center_analytics.utils import os_lib


# ------------------------------------------- HELPERS ----------------------------------------------
def _prepare_file_for_creation(file_path: str, replace_if_exists: bool) -> None:
    """If absolute path, remove file if exists, then check if path is creatable."""
    os_lib.ensure_path_is_absolute(file_path)
    if replace_if_exists:
        os_lib.remove_file(file_path)
    # error if not replace_if_exists AND file already exists
    os_lib.ensure_file_is_creatable(file_path)


def _prepare_file_for_reading(file_path: str, extension: str, encoding: str) -> None:
    """Valid if path is absolute, exists with `extension`, and encoded with `encoding`."""
    os_lib.ensure_path_is_absolute(file_path)
    os_lib.ensure_valid_file_type(file_path, extension)
    os_lib.ensure_file_exists(file_path)
    os_lib.ensure_valid_file_encoding(file_path, encoding)


# -------------------------------------------- CSV IO ----------------------------------------------
def create_csv_file(file_path: str, data: Union[np.ndarray, pd.Series, pd.DataFrame],
                    replace_if_exists: bool=False) -> None:
    """Write given data to flat file at given location.

    Infers dates as per ISO-8601 datetime format (eg '%Y-%m-%d %H:%M:%S').
    """
    file_path_ = os_lib.normalize_path(file_path)
    # convert to DataFrame
    if isinstance(data, pd.DataFrame):
        df = data
    elif isinstance(data, np.ndarray):
        df = pd.DataFrame(data)
    elif isinstance(data, pd.Series):
        df = pd.DataFrame(data).T
    else:
        raise ValueError('Given data is invalid - only Pandas Series, '
                         'Pandas DataFrame, and NumPy ndarray are supported.')

    _prepare_file_for_creation(file_path_, replace_if_exists)
    # if completely empty dataframe, create completely empty file
    if df.empty:
        with open(file_path_, 'x'):
            pass
    else:
        return df.to_csv(path_or_buf=file_path_, mode='x', encoding='utf-8')


def read_csv_file(file_path: str, date_columns: Sequence[Union[str, int]]=()) \
        -> Union[np.ndarray, pd.DataFrame]:
    """Fetch flat file from given path as a pandas DF.

    Take first column as index
    `columns_to_use`: huge speedup for csv, no noticeable effect for json
    index inferred from first column of `columns_to_use`
    """
    date_columns_ = None if not date_columns else date_columns
    file_path_ = os_lib.normalize_path(file_path)
    _prepare_file_for_reading(file_path_, extension='.csv', encoding='utf-8')
    # if completely empty file, return completely empty DataFrame
    if os_lib.is_empty_file(file_path):
        return pd.DataFrame()

    data = pd.read_csv(filepath_or_buffer=file_path_, index_col=0, squeeze=True,
                       parse_dates=date_columns_, encoding='utf-8', infer_datetime_format=True)
    return data.values if isinstance(data, pd.Series) else data


# -------------------------------------------- JSON IO ---------------------------------------------
def create_json_file(file_path: str, contents: object, replace_if_exists: bool=True) -> None:
    """Write given contents to json file.

    Any combination of list and dict (or other json supported data types),
    are allowed. DataFrames are NOT supported.
    """
    file_path_ = os_lib.normalize_path(file_path)

    if replace_if_exists and os_lib.is_existent_file(file_path_):
        os_lib.remove_file(file_path_)
    _prepare_file_for_creation(file_path_, replace_if_exists)
    with open(file_path_, mode='x') as json_file:
        json_file.write(json.dumps(contents))


def read_json_file(file_path: str) -> Any:
    """Read json file from given path, empty dict if empty file."""
    file_path_ = os_lib.normalize_path(file_path)
    _prepare_file_for_reading(file_path_, extension='.json', encoding='utf-8')
    if os_lib.is_empty_file(file_path_):
        return {}
    with open(file_path_, 'r') as json_file:
        return json.load(json_file)


# ------------------------------------------ DATABASE IO -------------------------------------------
def create_sqlite_file(file_path: str, replace_if_exists: bool=False) -> None:
    """Create SQL file if path is available and connection is successful.

    Notes
    -----
    * File is created only if `connect_to_sqlite_database` succeeds after creation
    """
    file_path_ = os_lib.normalize_path(file_path)
    os_lib.ensure_path_is_absolute(file_path_)
    file_exists_before_creation = os_lib.is_existent_file(file_path_)

    _prepare_file_for_creation(file_path_, replace_if_exists)
    try:
        sqlite3.connect(file_path_)  # establish file path
        connect_to_sqlite_database(file_path_)
    except:
        os_lib.remove_file(file_path_)  # remove faultily auto-created file if exists
        raise ValueError('Cannot create database - '
                         'valid connection to \'{}\' cannot be established.'
                         .format(file_path_))

    # generate a report for the database update
    action_taken = 'replaced' if file_exists_before_creation else 'created'
    print('Database \'{}\' was successfully {}.'.format(os_lib.get_basename(file_path), action_taken))


def read_sqlite_table(con: sqlite3.Connection, table_name: str,
                      as_unique: bool, columns_to_use: Sequence[str]=(),
                      date_columns: Sequence[str]=()) -> Union[np.ndarray, pd.DataFrame]:
    """Retrieve table of given name from database as a df.

    Notes
    -----
    * If single column, return as a numpy array.
    * Use columns_to_use if not None, otherwise use all columns in table, in both
      cases the columns are queried in order given, with first column as index
    """
    ensure_table_is_in_database(con, table_name, columns_to_use)
    index = [columns_to_use[0]] if columns_to_use else [get_all_columns_in_table(con, table_name)[0]]

    query = 'SELECT DISTINCT ' if as_unique else 'SELECT '
    query += ', '.join(columns_to_use) if columns_to_use else '*'
    query += ' FROM ' + table_name

    # if the queried df has no columns, than
    df = pd.read_sql(sql=query, con=con, index_col=index, parse_dates=date_columns)
    return df if len(df.columns) != 0 else df.index.values


def write_to_sqlite_table(con: sqlite3.Connection, data: pd.DataFrame,
                          table_name: str, if_table_exists: str='fail') -> None:
    """Write contents of DataFrame to sqlite table.

    Parameters
    ----------
    con : sqlite3.Connection
        Database connection object for sqlite3
    data : pd.DataFrame
        DataFrame to write to sql table
    table_name : str
        Name of table to update or create
    if_table_exists : str of {'fail', 'replace', 'append'}, default 'fail'
        * fail: if table exists, do nothing else create table
        * replace: if table exists drop it and recreate table else create table
        * append: if table exists, insert at end of table, else do nothing

    Notes
    -----
    * `if_table_exists` options do not raise any errors, rather they dictate the
      corresponding message that is logged/printed to console
    * Action taken and total number of row changes are logged accordingly
    * Note that `if_table_exists` options follow different semantics than the
      parameter of 'if_exists' parameter in the method `DataFrame.to_sql`
    """
    normalized_name = table_name.replace('-', '').replace(' ', '').replace('_', '')
    if not normalized_name.isalnum():  # contains only letters, digits, '_', ' ', '-'
        raise ValueError('Given table name \'{}\' is invalid - only letters, digits, spaces, '
                         'dashes, and underscores are allowed.'.format(table_name))

    table_is_in_db = is_table_in_database(con, table_name)
    actions = ('fail', 'append', 'replace')
    if if_table_exists not in actions:
        raise ValueError('`action_if_exists` must be in {\'fail\', \'replace\', \'append\'}.')
    if if_table_exists == 'append':
        ensure_table_is_in_database(con, 'tutor_requests')

    try:
        data.to_sql(name=table_name, con=con, if_exists=if_table_exists)
    except Exception as e:
        if isinstance(e, ValueError):  # catch if_table_exists='fail' exception
            pass
        else:
            raise IOError('DataFrame failed to be imported as table \'{}\' in the database '
                          'present at \'{}\'.'.format(table_name, con))

    # generate a report for the database update
    deciding_factors = (if_table_exists, table_is_in_db)
    outcomes = {
        ('fail', True): 'Table \'{}\' cannot be created - if_table_exists=\'fail\'',
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


def connect_to_sqlite_database(file_path: str) -> sqlite3.Connection:
    """Return sqlite3 connection if file_path leads to a valid SQLite file.

    Parameters
    ----------
    file_path : str
        File path to sqlite database - considered valid if it's an absolute,
        existing, UTF-8 encoded, SQLite file

    Return
    ------
    `sqlite3.Connection`
        sqlite connection to database located at given path

    Raises
    ------
    ValueError
        * If file path is not an absolute path (eg only file name is given)
        * If file name does not end with a single .sql extension
    FileNotFoundError
        * If file does not exist
    OSError
        * If file at path is corrupt or cannot be recognized as a SQLite file,
          as determined by it's header string
    UnicodeDecodeError
        * If file is not encoded with UTF-8
    ConnectionRefusedError
        * If `sqlite3.Connection` object instantiation fails

    References
    ----------
    * For more details on method used to determine corruption/validity of sql
      file, go to section 'magic header string' on the official sqlite website,
      'http://www.sqlite.org/fileformat.html'
    """
    file_path_ = os_lib.normalize_path(file_path)
    _prepare_file_for_reading(file_path_, extension='.sql', encoding='utf-8')
    # ensure byte encoding indicates file is of type SQLite
    with codecs.open(file_path_, 'r', 'utf-8') as file:
        if codecs.encode(file.read(16)) == '53514c69746520666f726d6174203300':
            raise UnicodeDecodeError('File \'{}\' is either corrupted or not a '
                                     'recognizable SQLite file.'.format(file_path_)) from None

    try:
        return sqlite3.Connection(file_path_)
    except Exception:
        raise ConnectionRefusedError(errno.ECONNREFUSED,
                                     'SQLite file \'{}\' cannot be reached'
                                     .format(os_lib.get_basename(file_path_)))


def get_all_columns_in_table(con: sqlite3.Connection, table_name: str) -> List[str]:
    """Return column names from table in db con."""
    ensure_table_is_in_database(con, table_name)
    cursor = con.execute('SELECT * FROM ' + table_name)
    return [col[0] for col in cursor.description]


def is_table_in_database(con: sqlite3.Connection, table_name: str) -> bool:
    """Return True if ValueError if exact given table_name is not in con."""
    try:
        ensure_table_is_in_database(con, table_name)
        return True
    except ValueError:
        return False


def ensure_table_is_in_database(con: sqlite3.Connection, table_name: str,
                                columns_to_select: Sequence[str] = None) -> None:
    """Raise ValueError if exact given table_name is not in con. If columns_to_select check for existence."""
    table_query = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?'
    is_table_in_database_ = bool(con.execute(table_query, [table_name]).fetchone())
    if not is_table_in_database_:
        raise ValueError('Table \'{}\' does not exist @ database \'{}\'.'.format(table_name, con))

    if columns_to_select:
        cursor = con.execute("SELECT * FROM " + table_name)
        columns_in_table = [col[0] for col in cursor.description]
        set_of_columns = set(columns_to_select)
        if len(columns_to_select) != len(set_of_columns) or not set_of_columns.issubset(
                columns_in_table):
            raise ValueError('{} is invalid - `columns_to_select` are not a '
                             'unique subset of the columns {} in the table \'{}\'.'
                             .format(tuple(columns_to_select), tuple(columns_in_table), table_name))


# ----------------------------------------- IMAP SERVER IO -----------------------------------------
def download_all_email_attachments(imap_connection: imaplib.IMAP4_SSL, email_uid: str,
                                   output_dir: str) -> List[str]:
    """Download all attachment files for a given unique email id.

    Return downloaded file paths, whether the download(s) were successful or not.
    """
    # todo: add temporary file option
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


def get_unread_email_uids(imap_connection: imaplib.IMAP4_SSL, sender: str='',
                          subject: str='') -> List[str]:
    """Return list of unique ids from newest to oldest matching given sender and subject."""
    sender_ = 'FROM ' + sender if sender else None
    subject_ = 'SUBJECT ' + subject if subject else None
    email_uid = imap_connection.uid('SEARCH', sender_, subject_, 'UNSEEN')[1][0].split()
    return email_uid
