"""Collection of I/O Database functionality.

Notes
-----
* All IO functions in io_lib assume UTF-8 encoding ONLY
* All output functions are made flexible such that empty data structures are
  still written to disk without error. Likewise, empty files return their
  corresponding empty data structure. It's important to note that works
  contrary to the behavior in most of `pandas.io` functions.
* Like the rest of the source, datetime parsing uses pandas.to_datetime() at
  its core, and parses to ISO 8601 format: 'YYYY-MM-DD HH:MM:SS', or in
  python convention, '%Y-%m-%d %H:%M:%S'
* All database IO functions, unless otherwise specified in its docs,
  raise ValueError if table does not exist.
* All paths are ensured and error checked according to their IO function
  (write, create, read, etc), as well as checked to be absolute paths.
  This is all done in an effort to reduce mistaken replacements, and generally
  increase IO robustness.
"""
import json
import errno
import email
import codecs
import imaplib
import sqlite3
from typing import Sequence, Mapping, Union, List, Any

import numpy as np
import pandas as pd

from stem_center_analytics.utils import os_lib


# ------------------------------------------- HELPERS ----------------------------------------------
def _prepare_file_for_creation(file_path: str, extension: str, replace_if_exists: bool) -> None:
    """If absolute path, remove file if exists, then check if path is creatable."""
    os_lib.ensure_path_is_absolute(file_path)
    os_lib.ensure_valid_file_type(file_path, extension)
    if replace_if_exists and os_lib.is_existent_file(file_path):
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
    """Create CSV file containing given data.

    Parameters
    ----------
    file_path : string
        File path to create or replace as a utf-8 encoded CSV file
    data : NumPy array, Pandas Series, or Pandas DataFrame
        Contents to write to CSV, with date formats inferred per
        ISO-8601 standards
    replace_if_exists : boolean, default True
        * If True remove file if present, creating a new one either way
        * If False create only if a file is not present otherwise raise OSError

    Notes
    -----
    * Internally data is converted to DataFrame format before converting to csv
    * Unlike `pandas.write_csv`, empty DataFrames create any empty file

    See Also
    --------
    * See Python online documentation of the more generalized function used
      here for the raw IO, `pandas.DataFrame.write_csv` for more details
      on errors and limitations.
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

    _prepare_file_for_creation(file_path_, '.csv', replace_if_exists)
    # if completely empty dataframe, create completely empty file
    if df.empty:
        with open(file_path_, mode='x', encoding='utf-8'):
            pass
    else:
        return df.to_csv(path_or_buf=file_path_, mode='x', encoding='utf-8')


def read_csv_file(file_path: str, num_rows: int=None, date_columns: Sequence[Union[str, int]] = ()) \
        -> Union[np.ndarray, pd.DataFrame]:
    """Retrieve contents of a csv file.

    Parameters
    ----------
    file_path : string
        File path to read as utf-8 encoded CSV
    num_rows : int
        Number of rows to read from csv, all rows read if None
    date_columns : array-like of strings, default ()
        * Columns to parse to datetime, as per ISO-8601 datetime standards

    Returns
    -------
    NumPy array
        If only a single column is present
    Pandas DataFrame
        If no columns are present, as an empty DataFrame
    Pandas DataFrame
        If more than one column is retrieved from csv, with the first column
        taken as index

    See Also
    --------
    * See Pandas online documentation of the more generalized function used
      here for the raw IO, `pandas.read_csv` for more details on errors
      and limitations.
    """
    date_columns_ = date_columns if date_columns else None
    file_path_ = os_lib.normalize_path(file_path)
    _prepare_file_for_reading(file_path_, extension='.csv', encoding='utf-8')
    # if completely empty file, return completely empty DataFrame
    if os_lib.is_empty_file(file_path):
        return pd.DataFrame()

    data = pd.read_csv(filepath_or_buffer=file_path_, index_col=0, squeeze=True, nrows=num_rows,
                       parse_dates=date_columns_, encoding='utf-8', infer_datetime_format=True)
    return data.values if isinstance(data, pd.Series) else data


# -------------------------------------------- JSON IO ---------------------------------------------
def create_json_file(file_path: str, contents: Any, replace_if_exists: bool=True) -> None:
    """Create JSON file containing given data.

    Parameters
    ----------
    file_path : string
        File path to create or replace as a utf-8 encoded JSON file at
    contents : Any
        Contents to write to JSON in the form of any combination of lists
        or dictionaries.
    replace_if_exists : boolean, default True
        * If True remove file if present, creating a new one either way
        * If False create only if a file is not present otherwise raise OSError

    Notes
    -----
    * Since this function ONLY supports writing JSON supported data structures
      to disk, Pandas DataFrames are NOT supported

    See Also
    --------
    * See Python online documentation of the more generalized function used
      here for the raw IO, `json.dumps` for more details on errors and
      limitations.
    """
    file_path_ = os_lib.normalize_path(file_path)
    _prepare_file_for_creation(file_path_, '.json', replace_if_exists)
    with open(file_path_, mode='x', encoding='utf-8') as json_file:
        json_file.write(json.dumps(contents))


def read_json_file(file_path: str) -> Any:
    """Retrieve contents of JSON file.

    Parameters
    ----------
    file_path : string
        File path to read as utf-8 encoded JSON file

    Returns
    -------
    contents : Any
        Any JSON supported data structure, such as any combination of lists
        or dictionaries.

    Notes
    -----
    * Since this function ONLY supports writing JSON supported data structures
      to disk, Pandas DataFrames are NOT supported
    * Contrary to most other functions starting with 'read' in io_lib,
      NO extra manipulation such as parsing or format inference is done
      to any of the contents

    See Also
    --------
    * See Python online documentation of the more generalized function used
      here for the raw IO, `json.load` for more details on errors and
      limitations.
    """
    file_path_ = os_lib.normalize_path(file_path)
    _prepare_file_for_reading(file_path_, extension='.json', encoding='utf-8')
    if os_lib.is_empty_file(file_path_):
        return {}
    with open(file_path_, mode='r', encoding='utf-8') as json_file:
        return json.load(json_file)


# ------------------------------------------ DATABASE IO -------------------------------------------
def create_sqlite_file(file_path: str, replace_if_exists: bool=False) -> None:
    """Create SQL file if path is available and connection is successful.

    Parameters
    ----------
    file_path : string
        File path to create or replace as a utf-8 encoded SQLite file.
        The file is created as an empty SQLite database with no tables.
    replace_if_exists : boolean, default True
        * If True remove file if present, creating a new one either way
        * If False create only if a file is not present otherwise raise OSError

    Notes
    -----
    * The fact that the function `sqlite3.connect` indiscriminately creates a
      SQLite file is relied upon here to establish the database
    * If the file creation is successful, then the file path and the status of
      'replaced' or 'created' is logged via `print`

    Raises
    ------
    ValueError
        * If error during file creation or a connection to the newly created
          database fails to establish a connection, then the file is completely
          removed

    See Also
    -------
    * See docstring of `connect_to_sqlite_database` for more information
      regarding what is considered a valid SQLite file.
    """
    file_path_ = os_lib.normalize_path(file_path)
    os_lib.ensure_path_is_absolute(file_path_)
    file_exists_before_creation = os_lib.is_existent_file(file_path_)

    _prepare_file_for_creation(file_path_, '.sql', replace_if_exists)
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
                      datetime_columns: Sequence[str]=()) -> Union[np.ndarray, pd.DataFrame]:
    """Retrieve contents of table in a sqlite database.

    Parameters
    ----------
    con : sqlite3.Connection
        SQLite3 connection to database containing table to read
    table_name : string
        Name of table to read from SQLite database
    as_unique : boolean
        * If True, only distinct rows are retrieved from the table. In the case
          that all columns are retrieved and the table contains no duplicates,
          then the value of `as_unique` makes no difference in the return value
    columns_to_use : array-like of strings, default ()
        * Use `columns_to_use` if non-empty, otherwise use all columns in table,
          in both cases the columns are queried in order given, with first
          column as index.
    datetime_columns : array-like of strings, default ()
        * Columns to parse to datetime

    Returns
    -------
    NumPy array
        If only a single `columns_to_use` is given
    DataFrame
        If more than one column is retrieved from table

    Notes
    -----
    * Through grabbing only the columns you need via `columns_to_use`,
      you can get massive performance increases, especially when it can be
      combined with `as_unique`.
    * Expanding upon the above note, in the specific use case that only unique
      values from a single column are needed the performance can approach the
      retrieval of precomputed values from a csv file!

    See Also
    --------
    * See Pandas online documentation of the more generalized function used
      here for the raw IO, `pandas.read_sql` for more details on errors and
      limitations.
    """
    ensure_table_is_in_database(con, table_name, columns_to_use)
    index = [columns_to_use[0]] if columns_to_use else [get_all_columns_in_table(con, table_name)[0]]

    query = 'SELECT DISTINCT ' if as_unique else 'SELECT '
    query += ', '.join(columns_to_use) if columns_to_use else '*'
    query += ' FROM ' + table_name

    # if the queried df has no columns,get_tutor_request_data than
    df = pd.read_sql(sql=query, con=con, index_col=index, parse_dates=datetime_columns)
    return df if len(df.columns) != 0 else df.index.values


def write_to_sqlite_table(con: sqlite3.Connection, data: pd.DataFrame,
                          table_name: str, if_table_exists: str='fail',
                          data_types: Mapping[str, type]=None) -> None:
    """Write contents of DataFrame to sqlite table.

    Parameters
    ----------
    con : sqlite3.Connection
        SQLite3 connection to database containing table to write to
    data : pandas.DataFrame
        Pandas DataFrame to write to sqlite table
    table_name : string
        Name of table in SQLite database to write to
    if_table_exists : str of {'fail', 'replace', 'append'}, default 'fail'
        Action to take upon an existing table
        * fail: if table exists, do nothing else create table
        * replace: if table exists drop it and recreate table else create table
        * append: if table exists, insert at end of table, else do nothing
    data_types : Mapping of string to type, default None
        Column name to data type mapping. Any unspecified columns have their
        data types inferred by pandas. Note that for inferred data types,
        unintended formatting issues may occur during writing.
        For example, datetime.time with a string representation of 'HH:MM:SS'
        is written in the format 'HH:MM:SS.000000' when no data type is given

    Notes
    -----
    * Assuming valid table name and `if_table_exists` options, no errors
      are raised during writing. Instead, a message is logged to describing:
          * action taken: replaced, appended, failed, or created
          * total number of row changes during the transaction

    See Also
    --------
    * See Pandas online documentation of the more generalized function used
      here for the raw IO, `pandas.DataFrame.to_sql` on their for
      more details on errors and limitations.
    """
    # fixme: datatypes with str and fractional timestamp doesn't always resolve correctly
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
        data.to_sql(name=table_name, con=con, if_exists=if_table_exists, dtype=data_types)
    except Exception as e:
        if isinstance(e, ValueError):  # catch if_table_exists='fail' exception
            pass
        else:
            raise IOError('DataFrame failed to be imported as table \'{}\' in the database '
                          'present at \'{}\'.'.format(table_name, con))

    # generate a report for the database update
    deciding_factors = (if_table_exists, table_is_in_db)
    outcomes = {
        ('fail', True): 'Table \'{}\' cannot be created',
        ('fail', False): 'Table \'{}\' successfully created',
        ('append', True): 'Table \'{}\' successfully appended to',
        ('append', False): 'Cannot append to non-existent table \'{}\' - append failed',
        ('replace', True): 'Table \'{}\' successfully replaced',
        ('replace', False): 'Cannot replace non-existent table \'{}\' - created instead'
    }
    outcome = outcomes[deciding_factors].format(table_name)
    outcome += ' (`if_table_exists`=\'{}\').'.format(if_table_exists)
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

    See Also
    --------
    * See the section 'magic header string' under the web page 'fileformat'
      at the online documentation for SQLite, for more information on the
      method used to determine what constitutes a valid SQLite file.
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
                                columns_to_select: Sequence[str]=None) -> None:
    """Valid if table in database present at connection.

    Raises
    ------
    ValueError
        * If `table_name` is not in the database connected to via `con`
        * If `columns_to_select` are not a unique subset of columns
          that exist in the database table. That is, no duplicates
          are allowed, all the column names must be present in the table
    """
    table_query = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?'
    is_table_in_database_ = bool(con.execute(table_query, [table_name]).fetchone())
    if not is_table_in_database_:
        raise ValueError('Table \'{}\' does not exist @ database \'{}\'.'.format(table_name, con))

    if columns_to_select:
        cursor = con.execute("SELECT * FROM " + table_name)
        columns_in_table = [col[0] for col in cursor.description]
        set_of_columns = set(columns_to_select)
        if (len(columns_to_select) != len(set_of_columns) or not
                set_of_columns.issubset(columns_in_table)):
            raise ValueError('{} is invalid - `columns_to_select` are not a '
                             'unique subset of the columns {} in the table \'{}\'.'
                             .format(tuple(columns_to_select), tuple(columns_in_table), table_name))


# ----------------------------------------- IMAP SERVER IO -----------------------------------------
def connect_to_imap_server(server_host: str, user_name: str,
                           user_password: str) -> imaplib.IMAP4_SSL:
    """Connect to an IMAP enabled email server.

    Parameters
    ----------
    server_host : string
        IMAP server hostname to use
        'imap.gmail.com' -> gmail
    user_name : string
        Email address as a username for server login
    user_password : string
        User password for server login

    Returns
    -------
    imaplib.IMAP4_SSL
        SSL connection to an IMAP enabled email server

    Notes
    -----
    * Currently only tested with gmail
    * For using the connection, ``con.uid`` is recommended as it denotes unique
      identifiers, whereas ``con.id`` does not. General syntax for
      common operations:
        * ``con.uid('STORE', ...)`` -> for mutating information
        * ``con.uid('FETCH', ...)`` -> for retrieving information, etc

    See Also
    --------
    * `download_all_email_attachments`
    * `get_unread_email_uids`
    * `imaplib.IMAP4_SSL`
    """
    # todo: add more to notes and references
    connection_client = imaplib.IMAP4_SSL(server_host)
    connection_client.login(user_name, user_password)
    connection_client.select()
    return connection_client


def download_all_email_attachments(imap_connection: imaplib.IMAP4_SSL, email_uid: str,
                                   output_dir: str) -> List[str]:
    """Download all file attachments present at unique email id.

    Parameters
    ----------
    imap_connection : imaplib.IMAP4_SSL object
        SSL connection to an IMAP enabled email server
    email_uid : string
        Unique email ID to download all attachments from
    output_dir : string
        Directory to download all files to.
        Attachment names determine filenames to use

    Returns
    -------
    list of string
        File paths of downloaded attachments.
        All file paths are returned, regardless of whether the downloads
        were successful or not

    Notes
    -----
    * Currently only tested with gmail
    * 'RFC822' is the protocol used, so all files are marked as read

    See Also
    --------
    * `connect_to_imap_server`
    * `get_unread_email_uids`
    * `imaplib.IMAP4_SSL`
    """
    # todo: add temporary file option (or not an option and return temp dir containing files)
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
    """Retrieve unique email ids for each unread message matching given criteria.

    Parameters
    ----------
    imap_connection : imaplib.IMAP4_SSL object
        SSL connection to an IMAP enabled server
    sender : string, default ''
        Desired sender email address to filter messages by
    subject : string, default ''
        Desired subject to filter messages by

    Returns
    -------
    list of strings
        Unique email IDs of all unread emails matching `sender` and `subject`
        present at the IMAP server. IDs are ordered from newest to oldest

    Notes
    -----
    * Currently only tested with gmail
    """
    sender_ = 'FROM ' + sender if sender else None
    subject_ = 'SUBJECT ' + subject if subject else None
    email_uid = imap_connection.uid('SEARCH', sender_, subject_, 'UNSEEN')[1][0].split()
    return email_uid
