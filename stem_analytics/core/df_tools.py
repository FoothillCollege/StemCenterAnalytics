"""Contains the core filtering functionality.

Note, these functions assume perfect input, given that they form the core
underlying filtering mechanisms. For more robust filtering, see 'client layer'
wrapper.py.
"""
from typing import Iterable, List

import pandas as pd

# todo: due to simplicity in filtering, integrate into stats.py


def config_pandas_display_size(max_rows: int=50, max_cols: int=20, max_width: int=500) -> None:
    """Configure Pandas display settings, to allow pretty-printing for console output."""
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.max_columns', max_cols)
    pd.set_option('display.width', max_width)


def filter_by_date(sc_data: pd.DataFrame, start_date: pd.datetime=None,
                   end_date: pd.datetime=None) -> pd.DataFrame:
    """Filter (slice) given df by start/end dates.
    Return copy of given df filtered (sliced) by start and end dates.

    Examples:
        works: i.e.: filter_by_date(df, '2013-09-20', '2013-09-25')
        doesn't work: works: i.e.: filter_by_date(df, 2013, '2013-09-25')
    Parameters:
        sc_data (Pandas DataFrame): df to filter
        start_date (DateTime-like-object): initial inclusive (YYYY-MM-DD)
            date to filter by.
        end_date (DateTime-like-object) - Optional: final inclusive (YYYY-MM-DD)
            date to filter by.
    Returns:
        if no start and no end date is given, return original df.
        if no startPandas DataFrame filtered by year
        if no end date is given, return df filtered by all dates past the start date.
        Otherwise, return df filtered by range of start and end date(s).
    """
    return sc_data.ix[start_date:] if end_date is None else sc_data.ix[start_date:end_date]


def filter_by_time_of_day(sc_data: pd.DataFrame,
                          time_range: Iterable(str)) -> pd.DataFrame:
    """Return copy of df filtered by start and end times (as given in time range str).

    Examples:
        works: filter_by_time_of_day(df, '8am', '10pm')
        doesn't work: filter_by_time_of_day(df, 8, 20)
    Args:
        sc_data (Pandas DataFrame): df to filter by
        time_range (tuple(str)): the time of day range (inclusive)
    Returns:
        Copy of given Pandas DataFrame filtered by time_def of day.
    """
    return sc_data.between_time(start_time=time_range[0], end_time=time_range[-1],
                                include_start=True, include_end=True)


def filter_by_year(sc_data: pd.DataFrame, years: List[int]) -> pd.DataFrame:
    """Return copy of df filtered by given by list of (ordinal) year(s).

    Examples:
        works: filter_by_year(df, [2014, 2015])
        doesn't work: filter_by_ordinal_year(df, 13, 14)
    Args:
        sc_data (Pandas DataFrame): df to filter by.
        years (list[int]): list of ordinal years to filter by.
    Returns:
        Copy of given Pandas DataFrame filtered by ordinal year(s).
    """
    pass


def filter_by_quarter(sc_data: pd.DataFrame, quarters: List[str]) -> pd.DataFrame:
    """Filter (slice) df according to given start and end (YYYY-MM-DD) dates.

    Examples:
        works: filter_by_quarter(df, ['Fall', 'Winter', 'Spring'])
        doesn't work: filter_by_quarter(df, 'Fa', 'Wi')
    Args:
        sc_data (Pandas DF): df to filter.
        quarters (list): quarters to filter by
        assume perfect input as quarter (fall, winter, spring,
        summer)
    Returns:
        Copy of DF filtered by weeks in quarter(s).
        filtered_sc_data (Pandas DF): df containing rows within given quarter.
    """
    return sc_data[sc_data['quarter'].isin(quarters)]


def filter_by_week_in_quarter(sc_data: pd.DataFrame,
                              weeks_in_quarter: List[str]) -> pd.DataFrame:
    """Filter df by week of year (ex: 2014-2015).

    Examples:
        works: filter_by_week_in_quarter(df, [1, 2, 3])
        doesn't work: filter_by_week_in_quarter(df, [0, 15])
    Args:
        sc_data (Pandas DF): df to filter.
        weeks_in_quarter (list[int]): assume perfect input, i.e.: ints 1-12
    Returns:
        Copy of DF filtered by week(s) in quarter.
    """
    return sc_data[sc_data['interval_option'].isin(weeks_in_quarter)]


def filter_by_course(sc_data: pd.DataFrame, subject: List[str]=None, name: List[str]=None,
                     section: List[str]=None) -> pd.DataFrame:
    """Filter df by course, as specified by: subject, name, section.

    Parameters
    ----------
        sc_data (Pandas DataFrame): df to filter.
        subject (Optional - str): course subject to filter by (i.e.: math, etc.)
        name (Optional - str): course subject (i.e.: 2a, 1a, etc.)
        section (Optional - str): course section (i.e.: 2, 3W, 1Y etc.)
    Returns
    -------
        Copy of DF filtered by given course subject, name, and section.
    Examples
    --------
        works: filter_by_course(df, ['cs'], ['2a'], ['2W'])
        doesn't work: filter_by_course(df, [0, 15])
    """
    pass


def filter_by_day(sc_data: pd.DataFrame, days: List[str]) -> pd.DataFrame:
    """Filter df by week of year (ex: 2014-2015).

    Note: assumes that sc_data contains single letter day get_full_names.
    Examples:
        works: filter_by_day(df, ['monday', 'tuesday'])
        doesn't work: filter_by_day(df, 'mon', 'tue')
    Args:
        sc_data (Pandas DataFrame): df to filter.
        days (List[str]): day of week to filter by (i.e.: M - W)
    Returns:
        Copy of DF filtered by given day(s) of weeks.
    """
    return sc_data[sc_data['day'].isin(days)]


# todo: make generic types for sc_data and datetime like, for use in type hinting
# todo: take action on below possibilities/notes
# --------------------------------------------------------------------------------------------------
# possible solution - alias corresponds to column number with corresponding assumed type
#    - example aliases: col1 = time_of_request, col2=quarter, col3=week_in_quarter, etc
#    - if it doesn't fit col description -- flag error
#    - if it fits criteria find format and column to map it to (in case csv output order changes)
# --------------------------------------------------------------------------------------------------
