"""The client layer for stem_analytics - essentially the 'official' project API.

In short, this module allows gluing together of core functionality,
input handling, and DF(s) (Pandas DataFrame).

Notes
-----
Unlike their 'core' counterparts that assume perfect input, methods in these
wrapper classes provide:
- parsing and error handling on the function args before being passed to core
  functionality (i.e.: _filtering, averaging)
- corresponding 'immutable' cleaned/formatted DF (i.e.: TutorLog
  contains the Pandas DataFrame entire_tutor_log).
- corresponding 'mutable' Pandas DataFrame, containing the latest state
  (i.e.: if you filtered all mondays on TutorLog, then the corresponding DF
  remembers it's last modified state.
"""
from typing import Iterable, Tuple, List

import pandas as pd

from stem_analytics.warehouse import _data_models
from stem_analytics.core import input_validation, df_tools


# --------------------------------------------------------------------------------------------------
# fixme: reconfigure methods to accomodate latest changes to project structure

# todo: implement smart error handling, as to hide internal stack trace w/ convenient messages
# todo: implement robust, flexible argument-extraction function in form of decorator
# todo: perhaps add 'dropping outside quarter' and similar abilities to sc_data?
# todo: determine filter design - filter by individual item? or general category instead (time/etc)?

# todo: add csv writing/etc functionality to _SC_Data
# todo: determine adequate docstring distribution (wrap from filter methods/something else..)???
# todo: add data reporting tools such custom csv/excel workbook generation/etc

# todo: transition bellow function to that of a decorator for SCData methods
# --------------------------------------------------------------------------------------------------

def extract_parsing_args(args) -> List:
    """Process given tuple to a list, with values extracted if single element.

    Args:
        args (tuple): unpacked variable arguments to process.
    Returns:
        if tuple has more than one element -- return list of the original args
        else if string -- return it.
        else if iterable (eg: tuple/list/set/etc.) -- return it as a list.
    Raises:
        ParsingError if non of the above return conditions are met.
    """
    if not isinstance(args, tuple):
        raise ValueError('Given arguments cannot be processed.')
    if len(args) != 1:
        return list(args)
    first_arg = args[0]
    if isinstance(first_arg, str):
        return first_arg
    if isinstance(first_arg, Iterable):
        return list(first_arg)
    raise ValueError('Given arguments cannot be processed.')

# todo: add a condensed view option that shows abbreviated vals in dataframe.


class _SCWrapper(object):

    """Wrapper for DataFrame of assumed form.

    Attributes:
        df (Pandas DataFrame): the core stem center data_samples to operate on.
    """

    def __init__(self, df: pd.DataFrame):
        self._entire_df = df
        self.data = df   # allows mutability  -- saves the current 'filtered' state

    def __repr__(self):
        return repr(self.data)

    def __str__(self):
        return repr(self.data)

    @property
    def all_data(self) -> pd.DataFrame:
        return self._entire_df.copy()

    def reset_data(self):
        """Reset `data` to `all_data`."""
        self.data = self._entire_df.copy()
        return self

    def filter_by_date(self, date_range):
        start, end = input_validation.parse_datetimes(date_range)
        self.data = self.data.ix[start:end]
        return self

    def filter_by_course(self, subject, name, section):
        self.data = df_tools.filter_by_course(self.data, subject, name, section)
        return self

    def filter_by_year(self, *years):
        extracted_args = extract_parsing_args(years)
        year_names = input_validation.parse_years(extracted_args)
        self.data = df_tools.filter_by_year(self.data, year_names)
        return self

    def filter_by_quarter(self, *quarters):
        extracted_args = extract_parsing_args(quarters)
        qtr_names = input_validation.parse_quarters(extracted_args)
        self.data = df_tools.filter_by_quarter(self.data, qtr_names)
        return self

    def filter_by_week_in_quarter(self, *weeks_in_quarter):
        extracted_args = extract_parsing_args(weeks_in_quarter)
        week_in_qtr_names = input_validation.parse_weeks_in_quarter(extracted_args)
        self.data = df_tools.filter_by_week_in_quarter(self.data, week_in_qtr_names)
        return self

    def filter_by_day(self, *days: Tuple[str]):
        extracted_args = extract_parsing_args(days)
        day_names = input_validation.parse_days(extracted_args)
        self.data = df_tools.filter_by_day(self.data, day_names)
        return self

    def filter_by_time_of_day(self, time_of_day_range: str):
        """Filter df by week of year (ex: 2014-2015).

        Examples:
            - filter_by_time_of_day('M-') ->
              DF containing days Mondays through Fridays
            - filter_by_time_of_day('W F') ->DF containing only Wednesdays/Fridays
        Args:
            time_of_day_range (str): string containing days of week to filter by.
            i.e.: acceptable day formats include 'M'/'Mon'/'Monday', separated by
            commas or spaces. A range of days can be included via dash as well; 'm - w'
        Returns:
            filtered_sc_data (Pandas DF): df containing rows within given academic year.
        """
        time_range = input_validation.parse_time_range(time_of_day_range)
        self.data = df_tools.filter_by_time_of_day(self.data, time_range)
        return self

    def average(self):
        """Average by given criteria.."""
        # include median/outlier options
        return self

    def generate_excel_report(self, include_stats: bool=True, update_current_file: bool=False):
        """Generate a report in excel, with stats, and the data, etc."""
        return self

    def _dump_to_excel(self, dump_all: bool=False):
        """Generate an excel file.."""
        # if dump_all, write all_data, else write 'current' data
        return self

    def _dump_to_csv(self, dump_all: bool=False) -> None:
        """Dump all to a csv file."""
        # if dump_all, write all_data, else write 'current' data
        return self

    def condense(self):
        """Condenses internal DataFrame via abbreviation of all fields/labels/values in df."""
        return self


class TutorLog(_SCWrapper):

    """Extended version of SCData for tutor_log."""

    def __init__(self):
        super().__init__(_data_models.get_student_login_data(as_clean=True))


class LoginData(_SCWrapper):

    """Extended version of SCData for login_data."""

    def __init__(self):
        super().__init__(_data_models.get_tutor_request_data(as_clean=True))


if __name__ == '__main__':
    tlog = TutorLog()
    print(tlog)
