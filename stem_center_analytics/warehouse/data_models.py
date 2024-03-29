"""Abstracted IO and file handling functionality for project specific data sources.

Notes
-----
* This module contains only data retrieval since data is not intended to be
  written inside the source directory, only read. Instead, updating the core
  data sources is intended to occur in the script directory (containing the
  automated data pipeline) only, via importing DATA_FILE_PATHS and io_lib,
  respectively.
* This allows all code within the source to assume 'perfect' data, with the
  following assumptions:
  * ??
  * ??
"""
import sqlite3
from collections import namedtuple
from typing import Sequence, Tuple, Union, Dict, List, Set

import numpy as np
import pandas as pd

from stem_center_analytics.utils import io_lib, os_lib


# create a struct-like mapping for the three main data-source file paths
WAREHOUSE_DIR = os_lib.normalize_path(os_lib.get_parent_dir(__file__))
DATA_FILE_PATHS = namedtuple('FilePaths', 'COURSE_RECORDS,QUARTER_DATES,DATABASE')(
    COURSE_RECORDS=os_lib.join_path(WAREHOUSE_DIR, 'course_records.json'),
    QUARTER_DATES=os_lib.join_path(WAREHOUSE_DIR, 'quarter_dates.csv'),
    DATABASE=os_lib.join_path(WAREHOUSE_DIR, 'stem_center_db.sql'),
)


def connect_to_stem_center_db() -> sqlite3.Connection:
    """Context manager for connection to database containing cleaned/training data."""
    return io_lib.connect_to_sqlite_database(DATA_FILE_PATHS.DATABASE)


def get_quarter_dates() -> pd.DataFrame:
    """Return DataFrame of all (manually entered) quarter start, end dates."""
    return io_lib.read_csv_file(DATA_FILE_PATHS.QUARTER_DATES, num_rows=None, date_columns=[1, 2])


def get_tutor_request_data(columns_to_use: Sequence[str]=(), as_unique: bool=False) \
        -> Union[pd.DataFrame, np.ndarray]:
    """Return DataFrame of tutor requests from the database.

    Notes
    -----
    * Columns:
        time_of_request, wait_time, course, quarter, week_in_quarter, day_in_week
        with the first two as datetime-index and datetime.time respectively, and the rest as strings
        eg: 2013-09-25 09:45:00, 01:21:25, Computer Science 1C 1W, Fall 2013, 1, 4

    * In the case that all columns are retrieved, as_unique has no difference on
      the result, since only distinct rows are allowed in the database table
    """
    date_columns = []
    if 'time_of_request' in columns_to_use or not columns_to_use:
        date_columns.append('time_of_request')

    with io_lib.connect_to_sqlite_database(DATA_FILE_PATHS.DATABASE) as con:
        data = io_lib.read_sqlite_table(con, 'tutor_requests', as_unique, columns_to_use, date_columns)
        if isinstance(data, pd.DataFrame) and 'wait_time' in data.columns:
            data['wait_time'] = pd.to_datetime(
                data['wait_time'].astype(str), format='%H:%M:%S', exact=True
            ).dt.time
        return data


def get_student_login_data(columns_to_use: Sequence[str]=(), as_unique: bool=False) \
        -> Union[pd.DataFrame, np.ndarray]:
    """Return DataFrame of student logins from the database.

    Notes
    -----
    * In the case that all columns are retrieved, as_unique has no difference on
      the result, since only distinct rows are allowed in the database table
    """
    date_columns = []
    if 'time_of_login' in columns_to_use or not columns_to_use:
        date_columns.append('time_of_login')

    with io_lib.connect_to_sqlite_database(DATA_FILE_PATHS.DATABASE) as con:
        data = io_lib.read_sqlite_table(con, 'tutor_requests', as_unique, columns_to_use, date_columns)
        if isinstance(data, pd.DataFrame) and 'time_in_center' in data.columns:
            data['wait_time'] = pd.to_datetime(data['time_in_center'], format='%H:%M:%S', exact=True).dt.time
        return data


def get_course_records() -> Dict[str, List[str]]:
    """Return course records (from json file) as a dict of lists."""
    return io_lib.read_json_file(file_path=DATA_FILE_PATHS.COURSE_RECORDS)


def get_set_of_all_courses() -> Set[str]:
    """Return set of all courses (all possible: subject, subject+number, subject+number+section)."""
    def extract_course_components(course: str) -> Tuple[str, str, str]:
        """For given course_name, add its three permutations to set_of_all_courses."""
        number_end = course.rfind(' ')
        number_start = course.rfind(' ', 0, number_end) + 1

        subject, number = course[0:number_start - 1], course[number_start:number_end]
        return subject, subject + ' ' + number, course

    set_of_all_courses = set()
    courses = get_tutor_request_data(columns_to_use=['course'], as_unique=True)
    for course_name in courses:
        set_of_all_courses.update(extract_course_components(course_name))
    return set_of_all_courses
