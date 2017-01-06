"""Abstracted IO and file handling functionality for project specific data sources.

Notes
-----
* Data assumptions and definitions to be added here later.
"""
import sqlite3
from collections import namedtuple
from typing import Sequence, List, Dict, Set

import pandas as pd

from stem_center_analytics.utils import io_lib, os_lib

# create a struct-like mapping for the three main data-source file paths
WAREHOUSE_DIR = os_lib.normalize_path(os_lib.get_parent_dir(__file__))
DATA_FILE_PATHS = namedtuple('FilePaths', 'COURSE_RECORDS,QUARTER_DATES,DATABASE')(
    COURSE_RECORDS=os_lib.join_path(WAREHOUSE_DIR, 'course_records.json'),
    QUARTER_DATES=os_lib.join_path(WAREHOUSE_DIR, 'quarter_dates.csv'),
    DATABASE=os_lib.join_path(WAREHOUSE_DIR, 'stem_center_db.sql'),
)
# ensure files and database connections are good to go
os_lib.ensure_file_exists(DATA_FILE_PATHS.QUARTER_DATES, valid_file_types=['csv'])
os_lib.ensure_file_exists(DATA_FILE_PATHS.COURSE_RECORDS, valid_file_types=['json'])
with io_lib.connect_to_db(DATA_FILE_PATHS.DATABASE):
    pass


def connect_to_stem_center_db() -> sqlite3.Connection:
    """Context manager for connection to database containing cleaned/training data."""
    return io_lib.connect_to_db(DATA_FILE_PATHS.DATABASE)


def get_quarter_dates() -> pd.DataFrame:
    """Return DataFrame of all (manually entered) quarter start, end dates."""
    return io_lib.read_flat_file_as_df(DATA_FILE_PATHS.QUARTER_DATES, date_columns=[1, 2])


def get_tutor_request_data(columns_to_use: Sequence[str]=(), as_unique: bool=False) -> pd.DataFrame:
    """Return DF of all tutor requests (uncleaned from external csv, cleaned from internal db).

    If `as_unique` is True, only distinct rows of the given columns are selected.

    Note that for the above, this has no difference when reading entire DF, because by
    design, only distinct rows are allowed in the database in the first place.
    """
    with io_lib.connect_to_db(DATA_FILE_PATHS.DATABASE) as con:
        if 'time_of_request' in columns_to_use:
            index_column, date_columns = 'time_of_request', ['time_of_request']
        else:
            index_column, date_columns = None, None
        return io_lib.read_database_table_as_df(con, 'tutor_requests', index_column,
                                                as_unique, columns_to_use, date_columns)


def get_student_login_data(columns_to_use: Sequence[str]=(), as_unique: bool=False) -> pd.DataFrame:
    """Return DF of all student logins (uncleaned from external csv, cleaned from internal db).
    Note that `columns_to_use` must include index.
    """
    with io_lib.connect_to_db(DATA_FILE_PATHS.DATABASE) as con:
        if 'time_of_request' in columns_to_use:
            index_column, date_columns = 'time_of_login', ['time_of_login']
        else:
            index_column, date_columns = None, None
        return io_lib.read_database_table_as_df(con, 'student_logins', index_column,
                                                as_unique, columns_to_use, date_columns)


def get_course_records() -> Dict[str, List[str]]:
    """Return course records (from json file) as a dict of lists."""
    return io_lib.read_json_file(file_path=DATA_FILE_PATHS.COURSE_RECORDS)


def get_set_of_all_courses() -> Set[str]:
    """Return set of all courses (all possible: subject, subject+number, subject+number+section)."""
    def extract_course_components(string: str) -> pd.Series:
        number_end = string.rfind(' ') - 1
        number_start = string.rfind(' ', 0, number_end) + 1
        return pd.Series(
            (string[0:number_start-1], string[number_start:number_end], string[number_end+2:])
        )
    df = get_tutor_request_data(columns_to_use=['course'], as_unique=True)
    courses = df['course'].apply(extract_course_components)
    subjects, numbers, sections = courses[0], courses[1], courses[2]
    courses_without_section = subjects + ' ' + numbers
    courses_with_section = courses_without_section + ' ' + sections
    return set(subjects) | set(courses_with_section) | set(courses_without_section)
