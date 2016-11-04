"""The client layer for stem_analytics - essentially the 'official' project API.

In short, this module allows gluing together of core functionality,
input handling, and DF(s) (Pandas DataFrame).

Notes
-----
Unlike their 'core' counterparts that assume perfect input, methods in these
wrapper classes provide:
* Parsing and error handling on the function args before being passed to core
  functionality (i.e.: _filtering, averaging)
* Corresponding 'immutable' cleaned/formatted DF (i.e.: TutorLog
  contains the Pandas DataFrame entire_tutor_log).
* Corresponding 'mutable' Pandas DataFrame, containing the latest state
  (i.e.: if you filtered all mondays on TutorLog, then the corresponding DF
  remembers it's last modified state.
"""
from typing import Iterable, Sequence, Union, List

import pandas as pd

from stem_center_analytics.warehouse import _data_models
from stem_center_analytics.core import input_validation as prs


# WARNING -- THIS MODULE IS AN UNSTABLE WORK IN PROGRESS!
# --------------------------------------------------------------------------------------------------
# todo: important - define generic type hints dashed string, str collection, comma-delimited string,
# in input validation, and import them here to use in API documentation
# fixme: reconfigure methods to accomodate latest changes to project structure

# todo: implement smart error handling, as to hide internal stack trace w/ convenient messages
# todo: implement robust, flexible argument-extraction function in form of decorator
# todo: perhaps add 'dropping outside quarter' and similar abilities to sc_data?
# todo: determine filter design - filter by individual item? or general category instead (time/etc)?

# todo: add csv writing/etc functionality to _SC_Data
# todo: determine adequate docstring distribution (wrap from filter methods/something else..)???
# todo: add data reporting tools such custom csv/excel workbook generation/etc
# todo: add a condensed view option that shows abbreviated vals in dataframe.
# --------------------------------------------------------------------------------------------------


class _SCWrapper(object):

    """Wrapper for DataFrame of assumed form.

    Attributes:
        df (Pandas DataFrame): the CORE_SUBJECTS stem center data_samples to operate on.
    """

    def __init__(self, df: pd.DataFrame):
        self._entire_df = df
        print(self._entire_df)
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

    def filter_by_date(self, dates: Union[str, Sequence[str]]):
        dates_ = prs.parse_datetime_range(dates)
        if ' - ' in dates:
            start, end = dates
            self.data = self.data.ix[start:end]
        else:
            pass
            # figure out an 'isin' for indices
            # self.data = self.data[self.data[]]
        return self

    def filter_by_course(self, course_names: Union[str, Sequence[str]]):
        course_names_ = prs.parse_courses(course_names, as_tuple=True)
        # filter by course gos here . . .
        # sc_data[sc_data['course'].isin(weeks_in_quarter)]
        # figure it out..complete names, all 3,etc...
        return self

    def filter_by_year(self, years: Union[str, Sequence[str]]):
        years_ = prs.parse_years(years)
        # filter by year here...
        return self

    def filter_by_quarter(self, quarters: Union[str, Sequence[str]]):
        quarters_ = prs.parse_quarters(quarters)
        self.data = self.data[self.data['quarter'].isin(quarters_)]
        return self

    def filter_by_week_in_quarter(self, weeks: Sequence):
        weeks_ = prs.parse_weeks_in_quarter(weeks)
        self.data = self.data[self.data['week_in_quarter'].isin(weeks_)]
        return self

    def filter_by_day(self, days: Union[str, int, Sequence[Union[str, int]]]):
        days_ = prs.parse_days(days)
        self.data = self.data[self.data['day'].isin(days_)]
        return self

    def filter_by_time_of_day(self, time_of_day_range: str):
        start_time, end_time = prs.parse_time_range(time_of_day_range)
        self.data = self.data.between_time(start_time, end_time,
                                           include_start=True, include_end=True)
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
        """Dump all contents to a csv file."""
        # if dump_all, write all_data, else write 'current' data
        return self

    def condense(self):
        """Condenses internal DataFrame via abbreviation of all fields/labels/values in df."""
        return self


class TutorLog(_SCWrapper):

    """Extended version of SCData for tutor_log."""

    def __init__(self):
        super().__init__(_data_models.get_tutor_request_data(as_clean=True))


class LoginData(_SCWrapper):

    """Extended version of SCData for login_data."""

    def __init__(self):
        super().__init__(_data_models.get_student_login_data(as_clean=True))


if __name__ == '__main__':
    tlog = TutorLog()
    print(tlog.all_data)
