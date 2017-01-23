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
import datetime
from functools import partial
from typing import Iterable, Sequence, Union, List

import pandas as pd

from stem_center_analytics import warehouse
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
        return self._entire_df.copy(deep=True)

    def reset_data(self):
        """Reset `data` to `all_data`."""
        self.data = self._entire_df.copy()
        return self

    def filter_by_datetime(self, dates: Union[str, Sequence[str]]):
        """Parse dashed string representing a range of datetimes.

        Parameters
        ----------
        dates : str or array-like of str
            Inclusive start and end date of form
            'YY-MM-DD HH:MM:SS - YY-MM-DD HH:MM:SS' where H:M:S is optional

        Returns
        -------
        tuple of str
            Start/end dates of format '%Y-%m-%d %H:%M:%S' with H:M:S zero by default
        list of str
            Parsed datetimes of above format

        Raises
        ------
        ParsingError if incorrect dashed-string format or individual times
            cannot be parsed to a 24 hour 'HH:MM' format.
        ParsingError if value 'start_date' >= 'end_time', as no range can be generated.

        Examples
        --------
        >>> parse_datetime_range('2011/03/20')
        ['2011-03-20 00:00:00']
        >>> parse_datetime_range('2011-12-25 00:00  -  2012/12/25 00:00:01')
        ('2011-12-25 00:00:00', '2012-12-25 00:00:01')
        >>> parse_datetime_range('1999/01/01 0:0:0 - 2000/11/11 23:00')
        ('1999-01-01 00:00:00', '2000-11-11 23:00:00')
        >>> parse_datetime_range('2010/10/10 01:01:01 - 2010/10/11 10:10:10')
        ('2010-10-10 01:01:01', '2010-10-11 10:10:10')
        >>> parse_datetime_range('2015/08/01 - 2015/08/02 3:00:42')
        ('2015-08-01 00:00:00', '2015-08-02 03:00:42')
        """
        def parse_datetime_(dt):
            return pd.to_datetime(prs.parse_date(dt))

        date_objects = prs.parse_user_input(user_input=dates, mapping_func=parse_datetime_)
        dates_ = [datetime.datetime.strftime(date, '%Y-%m-%d') for date in date_objects]

        if ' - ' in dates:
            start, end = dates_
            self.data = self.data.ix[start:end]
        else:
            pass
            # figure out an 'isin' for indices
            # self.data = self.data[self.data[]]
        return self

    def filter_by_year(self, years: Union[str, Sequence[str]]):
        years_ = prs.parse_user_input(
            user_input=years,
            mapping_func=lambda yr: prs.TIME_UNIT_VALUES.YEARS.lookup_by_alias(alias=str(yr)),
            values_to_slice=prs.TIME_UNIT_VALUES.YEARS.keys()
        )
        # filter by year here...
        return self


    def filter_by_course(self, course_names: Union[str, Sequence[str]]):
        if ' - ' in course_names:
            raise prs.ParsingError('\'{}\' is invalid - course input cannot be dashed.')
        course_names_ = prs.parse_user_input(
            user_input=course_names,
            mapping_func=partial(prs.parse_course, as_tuple=True, check_records=True)
        )
        # filter by course gos here . . .
        # sc_data[sc_data['course'].isin(weeks_in_quarter)]
        # figure it out..complete names, all 3,etc...
        return self

    def filter_by_quarter(self, quarters: Union[str, Sequence[str]], with_years: bool=True):
        """Filter by quarter with year (fall 13, dash supported) or by name (fall, dash not supported)."""
        if isinstance(quarters, str) and ' - ' in quarters:
            left, _, right = quarters.partition(' - ')
            if len(left.split()) != 2 or len(right.split()) != 2:
                raise prs.ParsingError('\'{}\' is invalid - only quarters with years can be '
                                       'parsed as dashed input.')

        keys = (prs.TIME_UNIT_VALUES.QUARTERS_WITH_YEARS.keys() if with_years else
                prs.TIME_UNIT_VALUES.QUARTERS.keys())
        quarters_ = prs.parse_user_input(
            user_input=quarters,
            mapping_func=prs.parse_quarter,
            values_to_slice=keys
        )
        self.data = self.data[self.data['quarter'].isin(quarters_)]
        return self

    def filter_by_week_in_quarter(self, weeks: Sequence):
        weeks_ = prs.parse_user_input(
            user_input=weeks,
            mapping_func=prs.TIME_UNIT_VALUES.WEEKS_IN_QUARTER.lookup_by_alias,
            values_to_slice=prs.TIME_UNIT_VALUES.WEEKS_IN_QUARTER.keys()
        )
        self.data = self.data[self.data['week_in_quarter'].isin(weeks_)]
        return self

    def filter_by_day(self, days: Union[str, int, Sequence[Union[str, int]]]):
        days_ = prs.parse_user_input(
            user_input=days,
            mapping_func=prs.TIME_UNIT_VALUES.WEEKDAYS.lookup_by_alias,
            values_to_slice=prs.TIME_UNIT_VALUES.WEEKDAYS.keys()
        )
        self.data = self.data[self.data['day'].isin(days_)]
        return self

    def filter_by_time_of_day(self, time_range: str):
        """Parse string representing time of day to two time strings of format='HH[:MM:SS]'.

        Parameters
        ----------
        time_range : str
            Dashed string of format 'start_time - end_time' representing the time
            range, where the value of 'start_time' is <= the value of 'end_time'.

        Returns
        -------
        * If parse_to_datetime is False : tuple of str
            Representing a start-time, end-time pair (format HH:MM) of desired time range.
        * If parse_to_datetime is True : tuple of two datetime-objects
            Pair of start/end times as datetime objects. Note that the year is
            arbitrary, and only the hour and minute fields are relevant.

        Raises
        ------
        ParsingError
            * If incorrect dashed-string format or individual times cannot be
              parsed to a 24 hour 'HH:MM' format.
            * If value 'start_time' >= 'end_time', as no range can be generated.

        Examples
        --------
        >>> parse_time_range('12:00am - 00:30')
        ('00:00:00', '00:30:00')
        >>> parse_time_range('9am - 9pm')
        ('09:00:00', '21:00:00')
        >>> parse_time_range('2:00 - 4')
        ('02:00:00', '04:00:00')
        >>> parse_time_range('6:17 pm - 18:20')
        ('18:17:00', '18:20:00')
        >>> parse_time_range('15 - 16')
        ('15:00:00', '16:00:00')
        >>> parse_time_range('0:01 - 23:59')
        ('00:01:00', '23:59:00')

        See Also
        --------
        Visit the docstring of `parse_time` for more detailed information
        on what individual times (on either side of the given dash) are valid.
        """
        if not isinstance(time_range, str) or ' - ' not in time_range:
            raise prs.ParsingError('\'{}\' is invalid - only dashed input is allowed.'.format(time_range))
        start_time, end_time = prs.parse_user_input(
            user_input=time_range,
            mapping_func=prs.parse_time,
            values_to_slice=()
        )
        self.data = self.data.between_time(start_time, end_time, include_start=True, include_end=True)
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
        super().__init__(warehouse.get_tutor_request_data())


class LoginData(_SCWrapper):

    """Extended version of SCData for login_data."""

    def __init__(self):
        super().__init__(warehouse.get_student_login_data())


if __name__ == '__main__':
    tlog = TutorLog()
    print(tlog.all_data)
