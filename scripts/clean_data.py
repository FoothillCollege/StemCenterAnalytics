"""Temporary data cleaning script: takes the 'cleaned' csv and cleans further, and builds db.

Notes
-----
* To run as script from project dir in terminal, first set python path (export PYTHONPATH='.'):
    $ python scripts/clean_data.py
* Profiling in IPython:
    %run -t -N1 scripts\clean_data.py
"""
from typing import NamedTuple, Union, Tuple

import numpy as np
import pandas as pd

from stem_center_analytics import warehouse, PROJECT_DIR
from stem_center_analytics.core import input_validation
from stem_center_analytics.utils import io_lib, os_lib

def _validate_tutor_request_data(df: pd.DataFrame):
    """Ensure the data to enter the database is valid."""
    def select_wait_times_in_range(t1: str, t2: str) -> pd.DataFrame:
        """Return subset of DataFrame such that t1 <= wait_time <= t2, where t in format HH:MM:SS."""
        t1_ = input_validation.parse_time(t1, as_time_object=True)
        t2_ = input_validation.parse_time(t2, as_time_object=True)
        return df[(df['wait_time'] >= t1_) & (df['wait_time'] <= t2_)]

    select_wait_times_in_range('00:00:00', '00:00:00')
    df = select_wait_times_in_range(warehouse.get_tutor_request_data(), '00:00:00')
    pass


def _extract_elapsed_time(t1: pd.Timestamp, t2: pd.Timestamp, as_datetime=True) -> Union[str, pd.Timestamp]:
    """Return result of t2 - t1, with all times of string format 'HH:MM:SS'."""
    wait_time = pd.Timedelta(t2 - t1).components
    h, m, s = wait_time.hours, wait_time.minutes, wait_time.seconds
    return input_validation.parse_time('{}:{}:{}'.format(h, m, s), as_time_object=as_datetime)


def _extract_time(dt: str) -> pd.Timestamp.time:
    """Return time component of string containing datetime or time."""
    try:
        return input_validation.parse_time(dt, as_time_object=True)
    except Exception:
        return input_validation.parse_datetime(dt, as_timestamp_object=True).time()


QUARTERS = warehouse.get_quarter_dates()
def _determine_week_in_quarter(date: pd.Timestamp, quarter_term: str) -> int:
    """Return week in quarter for given date and quarter term, assuming quarter term is correct."""
    if not quarter_term:
        return np.NaN
    start_of_qtr = QUARTERS.ix[quarter_term]['start_date']
    return date.weekofyear - start_of_qtr.weekofyear + 1


def _determine_quarter(date: pd.Timestamp) -> str:
    """Return quarter (eg Fall 2013) corresponding to the given date, empty string if no match."""
    date_range_mask = np.logical_and(QUARTERS['start_date'] <= date, date <= QUARTERS['end_date'])
    matched_rows = QUARTERS[date_range_mask].index.values
    return str(matched_rows[0]) if matched_rows else ''


def build_new_tutor_request_row(old_row: NamedTuple) -> \
        Union[Tuple, Tuple[Union[str, pd.datetime], str, str, str, int, int]]:
    """Map values from an old row to a cleaned sequence of values.

    Parameters
    ----------
    old_row : NamedTuple
        Column to value mapping of uncleaned dataframe with the following keys,
        with the first three datetime-like, and the last two as strings:
        [Index, time_of_request, time_of_service, course_name, course_section]

    Returns
    -------
    new_row: Tuple of the form (datetime-like, str, str, int, int, str)
        Sequence of cell values corresponding to the following keys:
        [time_of_request, wait_time, quarter, week_in_quarter, day_in_week, course]

    Examples
    --------
    >>> from collections import namedtuple
    >>> OldRow = namedtuple('Row', 'Index,time_of_request,time_of_service,course_name,course_section')
    >>> build_new_tutor_request_row(OldRow(*'9/25/2013,9/25/2013 10:03,11:05:32 AM,CHEM F030A,1'.split(',')))
    ('2013-09-25 10:03:00', '01:02:32', 'Chemistry 30A 1', 'Fall 2013', 1, 4)
    >>> build_new_tutor_request_row(OldRow(*'10/4/2016,10:08:26 AM,10:09:09 AM,MATH F001D,1'.split(',')))
    ()
    >>> # note the above row has a date that is not present in any archived quarter

    Notes
    -----
    * If an old row has a date that does not fall within any quarters, then it is
      not added to the new row.
    """
    date = input_validation.parse_date(str(old_row.Index), as_date_object=True)
    time_of_request = pd.Timestamp.combine(
        date, _extract_time(old_row.time_of_request)
    )
    time_of_service = pd.Timestamp.combine(
        date, _extract_time(old_row.time_of_service)
    )

    # check it early
    quarter = _determine_quarter(time_of_request)
    if not quarter:
        return ()
    wait_time = _extract_elapsed_time(time_of_request, time_of_service)
    course = input_validation.parse_course(old_row.course_name + ' ' + old_row.course_section)

    week_in_quarter = _determine_week_in_quarter(time_of_request, quarter)
    day_in_week = date.toordinal() % 7 + 1  # weekday: sun=1, sat=7

    return time_of_request, str(wait_time), course, quarter, week_in_quarter, day_in_week


def process_tutor_request_data(if_exists: str, replace_db: bool=False) -> None:
    """Clean data and add to database, if_exists: {'replace', 'append', 'fail'}.

    Note that any row with dates falling outside the range of quarters
    specified in 'stem_center_analytics/warehouse/quarter_dates.csv'
    will NOT be added to the dataframe that is written to the database
    """
    new_column_names = ('time_of_request', 'wait_time', 'course', 'quarter', 'week_in_quarter', 'day_in_week')
    old_df = io_lib.read_csv_file(
        os_lib.join_path(PROJECT_DIR, 'external_datasets', 'unclean_tutor_requests.csv')
    )

    new_rows = []
    for row in old_df.itertuples():
        new_row = build_new_tutor_request_row(row)
        if new_row:
            new_rows.append(new_row)

    new_df = pd.DataFrame.from_records(data=new_rows, index=new_column_names[0], columns=new_column_names)
    new_df = new_df.groupby(new_df.index).first()  # todo: replace with a single second increment for duplicates
    new_df.sort_index(axis=0, ascending=True, inplace=True)

    if replace_db:
        io_lib.create_sqlite_file(warehouse.DATA_FILE_PATHS.DATABASE, replace_if_exists=True)
    with warehouse.connect_to_stem_center_db() as con:
        io_lib.write_to_sqlite_table(con, new_df, table_name='tutor_requests',
                                     if_table_exists=if_exists, data_types={'wait_time': str})


if __name__ == '__main__':
    # todo: add validation checking for each column
    # todo: change below to support CLI script options
    option = 'rebuild'

    if option == 'append':
        # fixme: append option is currently broken
        process_tutor_request_data(if_exists='append')

    if option == 'rebuild':
        process_tutor_request_data(if_exists='replace')
        #print(warehouse.get_tutor_request_data())


# todo: enhance performance: try benchmarking with shorter, abbreviated names, both in writing and reading
