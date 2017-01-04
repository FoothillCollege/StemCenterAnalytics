"""Abstracted IO and file handling functionality for project specific data sources.

Notes
-----
* Data assumptions and definitions to be added here later.
"""
import sqlite3
from typing import Iterable, List, Dict, Set

import pandas as pd

from stem_center_analytics.utils import io_lib, os_lib
from stem_center_analytics import EXTERNAL_DATASETS_DIR, INTERNAL_DATASETS_DIR


def _create_data_path_mappings(dir_path: str, file_names: Iterable[str]) -> Dict[str, str]:
    """Helper for creating file mappings for given resource directory."""
    file_mappings = {}
    for file_name in file_names:
        file_path = os_lib.join_path(dir_path, file_name)
        if os_lib.get_extension(file_path, with_dot=False) == 'sql':
            with io_lib.connect_to_db(file_path):  # ensures valid connection (raises if issue)
                pass
        else:
            os_lib.ensure_file_exists(file_path, valid_file_types=['json', 'csv'])
        file_mappings[file_name.split('.')[0]] = file_path
    return file_mappings


# in the process of build the mappings, all files are checked to exist
_EXTERNAL_DATASETS_PATH_MAP = _create_data_path_mappings(
    dir_path=EXTERNAL_DATASETS_DIR,
    file_names=('unclean_student_logins.csv', 'unclean_tutor_requests.csv')
)
_INTERNAL_DATASETS_PATH_MAP = _create_data_path_mappings(
    dir_path=INTERNAL_DATASETS_DIR,
    file_names=('course_records.json', 'quarter_dates.csv', 'stem_center_db.sql')
)


def connect_to_stem_center_db() -> sqlite3.Connection:
    """Context manager for connection to database containing cleaned/training data."""
    return io_lib.connect_to_db(_INTERNAL_DATASETS_PATH_MAP['stem_center_db'])


def get_quarter_dates() -> pd.DataFrame:
    """Return DataFrame of all (manually entered) quarter start, end dates."""
    return io_lib.read_flat_file_as_df(_INTERNAL_DATASETS_PATH_MAP['quarter_dates'], date_columns=[1, 2])


def get_tutor_request_data(as_clean: bool=True) -> pd.DataFrame:
    """Return DF of all tutor requests (uncleaned from external csv, cleaned from internal db)."""
    if as_clean:
        with io_lib.connect_to_db(_INTERNAL_DATASETS_PATH_MAP['stem_center_db']) as con:
            return io_lib.read_database_table_as_df(con, 'tutor_requests')
    # otherwise read the tutor requests in from csv
    return io_lib.read_flat_file_as_df(_EXTERNAL_DATASETS_PATH_MAP['unclean_tutor_requests'])


def get_student_login_data(as_clean: bool=True) -> pd.DataFrame:
    """Return DF of all student logins (uncleaned from external csv, cleaned from internal db)."""
    if as_clean:
        with io_lib.connect_to_db(_INTERNAL_DATASETS_PATH_MAP['stem_center_db']) as con:
            return io_lib.read_database_table_as_df(con, 'student_logins')
    # otherwise read the student logins in from csv
    return io_lib.read_flat_file_as_df(_EXTERNAL_DATASETS_PATH_MAP['unclean_student_logins'])


def get_course_records() -> Dict[str, List[str]]:
    """Return course records (from json file) as a dict of lists."""
    return io_lib.read_json_file(file_path=_INTERNAL_DATASETS_PATH_MAP['course_records'])


def get_set_of_all_courses() -> Set[str]:
    """Return set of all courses (all possible: subject, subject+number, subject+number+section)."""
    def extract_course_components(string: str) -> pd.Series:
        number_end = string.rfind(' ') - 1
        number_start = string.rfind(' ', 0, number_end) + 1
        return pd.Series(
            (string[0:number_start-1], string[number_start:number_end], string[number_end+2:])
        )
    df = get_tutor_request_data()
    courses = df['course'].apply(extract_course_components)
    subjects, numbers, sections = courses[0], courses[1], courses[2]
    courses_without_section = subjects + ' ' + numbers
    courses_with_section = courses_without_section + ' ' + sections
    return set(subjects) | set(courses_with_section) | set(courses_without_section)
