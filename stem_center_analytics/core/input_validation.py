"""Provides input validation functionality specific to project needs.

Notable Contents
----------------
InvalidInputError : Class, Subclass of ValueError
    Core exception intended to be used anywhere invalid input is encountered
AliasTable : Class, Subclass of object
    Lookup table for user-defined name mappings, with two lookup methods
    `lookup_by_alias` and `lookup_by_ordering`
AliasTable instances : AliasTable object
    Global constant instances of `AliasTable`, ie `COLUMN_NAMES`
Functions with names that start with 'parse'
    Specific parsing functions with non-trivial implementation, ie `parse_course`
parse_input : (string, function, optional Sequence) -> Sequence
    Workhorse function for applying string mapping or parsing functions to
    multiple types of user input. Its primary use case is for the aforementioned
    lookup methods of `AliasTable` instances and 'parse____' functions.

Notes
-----
* Any function returning Tuple[str, str], indicates that the values to the left
  and right of the dash are parsed, as it doesn't make sense to return
  all the immediate values (eg `parse_datetimes`). Thus, such functions
  only parse ranges (dashed input) and nothing more.
"""
import re
import types
import datetime
import collections
from typing import Sequence, Union, Tuple, List, Set

import numpy as np
import pandas as pd

from stem_center_analytics import warehouse


class InvalidInputError(ValueError):
    """Base exception raised for value-related errors encountered while parsing a string."""
    def __init__(self, value: object, reason: str):
        super().__init__(f'\'{value}\' is invalid - {reason}')


class AliasTable(object):

    """Lookup table for name and aliases.

    Essentially a lookup table for user-defined name mappings.
    The table is intended to be immutable, and thus rows can only be
    created once, at instantiation.

    Parameters
    ----------
    args : positional arguments, each as a tuple with a string and set of strings
        Each argument creates a corresponding row in the table according to:
        * relative position of argument -> ordering (aka 1-based index)
        * first item in pair (string) -> name (aka official name)
        * second item in pair (set of strings) -> acceptable aliases (aka alternate names)

    Attributes
    ----------
    names : tuple of strings
        Sequence of names within the table

    Methods
    --------
    lookup_by_ordering(ordering, raise_if_not_found=True) -> matched_name or None
        Search collection for a name corresponding to given alias
    lookup_by_alias(alias, raise_if_not_found=True) -> matched_name or None
        Search collection for a name corresponding to given alias

    Raises
    ------
    ValueError
        If any given (key, value) positional argument of constructor is:
        * Not a tuple of exactly two items
        * Item to be taken as key is not a string
        * Item to be taken as value is not a set of strings
        * Not a tuple consisting of a string, and a set of string
        * If any string, whether as a key or as a member of value
          contains a character other than a letter, digit, space, or underscore

    Notes
    -----
    * The values in the table are stored internally in an Ordered Dictionary

    Examples
    --------
    >>> a0 = ('joe', {'joseph', 'jose', 'joe', 'jo'})    # joseph referred by any of the right names
    >>> a1 = ('sally',  {'sally'})                       # sally referred to only by sally
    >>> a2 = ('thomas', {'tom'})                         # thomas referred to only by tom
    >>> a3 = ('zoe',    {'zoe', 'zo'})                   # zoe referred to by zoe or zo
    >>> ORDERED_NAMES = AliasTable(a0, a1, a2, a3)
    >>> print(ORDERED_NAMES)
    **************** Alias Table ****************
    | ORDERING |  NAME  |        ALIASES        |
    |    1     |  joe   | jo, joe, jose, joseph |
    |    2     | sally  |         sally         |
    |    3     | thomas |          tom          |
    |    4     |  zoe   |        zo, zoe        |
    |___________________________________________|
    >>> ORDERED_NAMES.lookup_by_alias('joseph', raise_if_not_found=False)
    'joe'
    >>> ORDERED_NAMES.lookup_by_alias('jo', raise_if_not_found=False)
    'joe'
    >>> ORDERED_NAMES.lookup_by_ordering('1', raise_if_not_found=False)
    'joe'
    >>> ORDERED_NAMES.lookup_by_ordering(2, raise_if_not_found=False)
    'sally'
    >>> ORDERED_NAMES.lookup_by_ordering('joseph', raise_if_not_found=False)

    """

    def __init__(self, *args: Tuple[str, Set[str]]):
        """Initialize collection with any number of string, set of string pairs."""
        for pair in args:
            if (len(pair) != 2 or not isinstance(pair, tuple) or not
                    isinstance(pair[0], str) or not isinstance(pair[1], set)):
                raise ValueError('Cannot construct AliasTable object - '
                                 'ALL arguments must be a two-item tuple '
                                 'consisting of a string and set of strings.')
            if any(not isinstance(entry, str) or not entry.replace('_', '').replace(' ', '').isalnum()
                   for entry in {pair[0]} | pair[1]):
                raise ValueError('Cannot construct AliasTable object - '
                                 'ALL strings in the collection can only contain '
                                 'letters, digits, spaces, and underscores.')
        self._ordered_mapping = collections.OrderedDict(args)
        self.names = tuple(self._ordered_mapping.keys())

    def __str__(self):
        """Return table with each row containing ordering, name, and aliases."""
        # build 2D sequence of strings, with aliases separated by commas in increasing length order
        rows = [('ORDERING', 'NAME', 'ALIASES')]
        for ordering, name in enumerate(self.names):
            aliases = sorted(self._ordered_mapping[name], key=lambda item: (len(item), item))
            description = ', '.join(aliases) if aliases else '--'
            rows.append((ordering + 1, name, description))
        # compute desired column width by finding length of widest cell in each column
        grid = [[str(col).strip(' ') for col in row] for row in rows]
        cell_widths = [max([len(v) for v in col]) for col in np.array(grid).T]
        table_width = 3 * len(grid[0]) + sum(cell_widths) + 1

        def build_row(row):
            return '|'.join([' ' + cell.center(width) + ' '
                             for cell, width in zip(row, cell_widths)])

        return (str.center(' Alias Table ', table_width, '*') + '\n' +
                '\n'.join(['|' + build_row(row) + '|' for row in grid]) +
                '\n|' + ((table_width - 2) * '_') + '|')

    def lookup_by_ordering(self, ordering: Union[int, str],
                           raise_if_not_found: bool=True) -> Union[None, str]:
        """Lookup name in table according to its relative ordering.

        Parameters
        ----------
        ordering : integer or string
            Relative ordering corresponding to a name in the table.
        raise_if_not_found : boolean, default True
            Determines whether to raise an `InvalidInputError` in the
            case of an invalid row number or to return None instead

        Returns
        -------
        None or string

        Raises
        ------
        `InvalidInputError`

        See Also
        --------
        * Documentation's 'Examples' section of containing class `AliasTable`
        """
        try:
            ordering = int(ordering) - 1  # convert from 1 based to 0 based indexing
            return self.names[ordering]
        except (TypeError, ValueError, IndexError):
            if not raise_if_not_found:
                return None
            raise InvalidInputError(ordering, f'ordering must fall between 1 and {len(self.names)}.')

    def lookup_by_alias(self, alias: str, raise_if_not_found: bool=True) -> Union[None, str]:
        """Lookup name in table according to its alias.

        Parameters
        ----------
        alias : string
            Alias corresponding to a name in the table
            Relative ordering as an integer value between one and
            the number of entries in the table
        raise_if_not_found : boolean, default True
            Determines whether to raise an `InvalidInputError` in the
            case of an invalid alias or to return None instead

        Returns
        -------
        None or string

        Raises
        ------
        `InvalidInputError`

        See Also
        --------
        * Documentation's 'Examples' section of containing class `AliasTable`
        """
        for token in self._ordered_mapping.keys():
            if alias in self._ordered_mapping[token]:
                return token

        if not raise_if_not_found:
            return None
        valid_names = (self.names[:2] + ('...',) + self.names[-2:]) if len(self.names) > 4 else self.names
        raise InvalidInputError(alias, f'cannot be recognized as one of {valid_names}')


#region Name Mapping Definitions for column, other unit, and time unit label names
COLUMN_NAMES = AliasTable(
    ('date',            {'date', 'date_of_request'}),
    ('time_of_request', {'time_of_request', 'time of request', 'start_time', 'start'}),
    ('wait_time',       {'wait_time', 'wait time', 'waittime'}),
    ('course_name',     {'course_name', 'course name'}),
    ('course_section',  {'course_section', 'course section', 'section', 'sec'})
)
OTHER_UNIT_NAMES = AliasTable(
    ('wait_time', {'wait_time', 'wait time', 'waittime'}),
    ('demand',    {'demand'})
)
TIME_UNIT_NAMES = AliasTable(
    ('hour',            {'hour', 'hours', 'hourly', 'hr', 'hrs'}),
    ('day_in_week',     {'day_in_week', 'day in week', 'day in wk', 'days in week',
                         'days in wk', 'day', 'days', 'daily', 'weekday', 'weekdays',
                         'wk days', 'wk day', 'week day', 'week days', 'date', 'dates'}),
    ('week_in_quarter', {'week_in_quarter', 'week in quarter', 'week in qtr', 'wk in qtr',
                         'wk in quarter', 'week', 'weeks', 'weekly', 'wk', 'wks'}),
    ('month',           {'month', 'months', 'monthly'}),
    ('quarter',         {'quarter', 'quarters', 'quarterly', 'qtr', 'qtrs'}),
    ('year',            {'year', 'years', 'yearly', 'yr', 'yrs'})
)
# endregion

# region Name Mapping Definitions for column, other unit, and time unit value
TIME_UNIT_VALUES = types.SimpleNamespace(
    # generate args i.e.: [('0', {'0'}), ..., ('23', {'23'})]
    HOURS=AliasTable(
        *[(str(hr), {str(hr)}) for hr in range(0, 24)]
    ),
    WEEKDAYS=AliasTable(
        ('Monday',    {'m', 'mo', 'mon'}),
        ('Tuesday',   {'t', 'tu', 'tue', 'tues'}),
        ('Wednesday', {'w', 'we', 'wed'}),
        ('Thursday',  {'r', 'th', 'thu', 'thur', 'thurs'}),
        ('Friday',    {'f', 'fr', 'fri'}),
        ('Saturday',  {'s', 'sa', 'sat'}),
        ('Sunday',    {'u', 'su', 'sun'})
    ),
    WEEKS_IN_SUMMER_QUARTER=AliasTable(
        *[(str(wk), {str(wk)}) for wk in range(1, 7)]
    ),
    # generate args i.e.: [('1', {'1'}), ..., ('12', {'12'})]
    WEEKS_IN_QUARTER=AliasTable(
        *[(str(wk), {str(wk)}) for wk in range(1, 13)]
    ),
    MONTHS=AliasTable(
        ('January',   {'jan', 'january'}),
        ('February',  {'feb', 'february'}),
        ('March',     {'mar', 'march'}),
        ('April',     {'apr', 'april'}),
        ('May',       {'may'}),
        ('June',      {'jun', 'june'}),
        ('July',      {'jul', 'july'}),
        ('August',    {'aug', 'august'}),
        ('September', {'sep', 'september'}),
        ('October',   {'oct', 'october'}),
        ('November',  {'nov', 'november'}),
        ('December',  {'dec', 'december'}),
    ),
    QUARTERS=AliasTable(
        ('Fall',   {'f', 'fa', 'fall'}),
        ('Winter', {'w', 'wi', 'win', 'winter'}),
        ('Spring', {'s', 'sp', 'spr', 'spring'}),
        ('Summer', {'u', 'su', 'sum', 'summer'})
    ),
    # generate args i.e.: [('2000', {'2000', '00'}), ..., ('2099', {'99', '2099'})]
    YEARS=AliasTable(
        *[(str(yr), {str(yr), str(yr)[-2:]}) for yr in range(2000, 2100)]
    ),
    # generate current format (as stored in db) possibilities, i.e.: 'F 2013', 'W 2014', ...
    QUARTERS_WITH_YEARS=AliasTable(
        *[((qtr_name + ' ' + str(qtr_yr)), {qtr_name + ' ' + str(qtr_yr)})
          for qtr_yr in range(2000, 2100)
          for qtr_name in ('Winter', 'Spring', 'Summer', 'Fall')]
    )
)
# endregion

# region Name Mapping Definitions for course subjects
# official course subject names were referenced
CORE_SUBJECTS = AliasTable(
    ('Mathematics',      {'mat', 'math', 'mathematics'}),
    ('Physics',          {'phy', 'phys', 'physics'}),
    ('Biology',          {'bio', 'biol', 'biology'}),
    ('Chemistry',        {'che', 'chem', 'chemistry'}),
    ('Engineering',      {'eng', 'engr', 'engi', 'engineering'}),
    ('Computer Science', {'cs', 'com', 'c s', 'comp', 'comp sci', 'computer science'})
)
OTHER_SUBJECTS = AliasTable(
    ('Accounting',              {'acc', 'actg', 'accounting'}),
    ('Astronomy',               {'ast', 'astr', 'astro', 'astronomy'}),
    ('Anthropology',            {'ant', 'anth', 'anthro', 'anthropology'}),
    ('Business',                {'bus', 'busi', 'business'}),
    ('Economics',               {'eco', 'econ', 'economics'}),
    ('Non Credit Basic Skills', {'non', 'ncbs', 'non credit basic skills'}),
    ('Psychology',              {'psy', 'psyc', 'psych', 'psychology'}),
    ('English',                 {'engl', 'english'}),
    ('History',                 {'history', 'hist'})
)
ALL_SUBJECTS = AliasTable(
    *(list(CORE_SUBJECTS._ordered_mapping.items()) + list(OTHER_SUBJECTS._ordered_mapping.items()))
)
# endregion


def parse_user_input(user_input: Union[str, Sequence[str]], mapping_func: callable(str),
                     values_to_slice: Sequence[Union[str, object]]=()) \
        -> Union[List[Union[str, object]], Tuple[str, str], Tuple[object, object]]:
    """Maps each token of a dashed, comma-delimited, or sequence of strings.

    Parameters
    ----------
    user_input : string or array-like of string
        Three distinct types of user input are recognized
        * sequence of strings -> array-like of strings
        * dashed string -> string containing dash
        * delimited string -> string containing values separated by commas,
          or no comma at all
    mapping_func : one str arg function
        The function in which each string element extracted from `user_input`
        is mapped with.
    values_to_slice : array-like of str, default ()
        Assumed as either empty, or a list of all possible mapped values, in
        which is sliced in the case of a dashed string.

    Returns
    -------
    array-like of strings or object:
        * If array-like return as list of lookup_by_alias values
        * If string containing dash, lookup_by_alias left and right values:
        * If `values_to_slice` is empty, then left and right
        * If `values_to_slice` is not empty, then list sliced by left and right
          values, inclusive
        * Else values taken between commas and mapped to list. This includes
          the case of a single string without commas.

    Raises
    ------
    ValueError
        * If not a string or sequence of strings
        * If input contains a single dash without surrounding spaces
        * If input cannot be parsed as either a sequence or delimited string,
          which means that no components could be successfully mapped
        * If string containing dash and the left mapped value exceeds the
          right, as no range can be generated.
        * ANY OTHER_SUBJECTS error raise while invoking `mapping_func`

    Examples
    --------
    >>> parse_user_input('foo, bar, baz', str.capitalize)
    ['Foo', 'Bar', 'Baz']
    >>> parse_user_input('fOo,  bar, BAz', str.capitalize)
    ['Foo', 'Bar', 'Baz']
    >>> parse_user_input('foo - baz', str.capitalize, ['Foo', 'Bar', 'Baz'])
    ['Foo', 'Bar', 'Baz']
    >>> parse_user_input('Foo', str.capitalize, ['Foo', 'Bar', 'Baz'])
    ['Foo']
    """
    if isinstance(user_input, Sequence) and not isinstance(user_input, str):
        return [mapping_func(' '.join(s.split())) for s in user_input]
    if not isinstance(user_input, str):
        raise ValueError('Only a collection of strings (list/tuple/etc) OR a '
                         'single (eg: delimited/dashed string) can be parsed.')

    user_input_ = ' '.join(user_input.split())
    if ' - ' not in user_input:
        return [mapping_func(string.strip(' ')) for string in user_input.split(',')]

    # if a dash is in input, attempt to parse as a dashed string
    if user_input.count('-') == 1 and ' - ' not in user_input:
        raise ValueError('Dashed input must be separated by \' - \' (including spaces).')
    left_token, _, right_token = ' '.join(user_input_.split()).partition(' - ')
    left_value, right_value = mapping_func(left_token), mapping_func(right_token)
    if not values_to_slice:  # endpoints only (no order checking here)
        return left_value, right_value

    # otherwise we return sliced list
    values = list(values_to_slice)
    left_index, right_index = values.index(left_value), values.index(right_value)
    if left_index < right_index:
        return values[left_index: right_index + 1]
    raise ValueError('Dashed strings must be of the form \'LHS - RHS\' where LHS < RHS.')


def parse_date(date: str, as_date_object: bool=False) -> Union[str, datetime.date]:
    """Parse string representing a date.

    Parameters
    ----------
    date : string
        String representing a date. Formatting options are flexible enough
        to cover all the most common data representations (see examples)
    as_date_object : boolean, False
        Determines whether to return a `datetime.date` object
        or a string of the form 'YY-MM-DD'

    Returns
    -------
    string or `datetime.date`

    Raises
    ------
    `InvalidInputError`

    See Also
    --------
    * `core.input_validation.parse_time`
    * `core.input_validation.parse_datetime`
    * `pandas.to_datetime`

    Examples
    --------
    >>> parse_date('2021-02-22', as_date_object=False)
    '2021-02-22'
    >>> parse_date('2021-02-22', as_date_object=False)
    '2021-02-22'
    >>> parse_date('2021/02/22', as_date_object=False)
    '2021-02-22'
    >>> parse_date('2021-02-22', as_date_object=False)
    '2021-02-22'
    >>> parse_date('2011.12.25', as_date_object=False)
    '2011-12-25'
    >>> parse_date('9/25/2013', as_date_object=False)
    '2013-09-25'
    >>> parse_date('2015-2-2', as_date_object=True)
    datetime.date(2015, 2, 2)
    """
    date, _, time = date.partition(' ')
    if time.replace(' ', '') != '':
        raise InvalidInputError(date, 'only date-like strings can be parsed.')

    try:
        dt_ = pd.to_datetime(date, infer_datetime_format=True).date()
        return dt_ if as_date_object else dt_.strftime('%Y-%m-%d')
    except Exception:
        raise InvalidInputError(date, 'cannot be recognized as a date.')


def parse_time(time: str, as_time_object: bool=False) -> Union[str, datetime.time]:
    """Parse string representing a time of day.

    Parameters
    ----------
    time : string
        Represents time of day in format 'hour[:minutes[:seconds][am/pm]'.
        Hour is required, 24 hour format assumed, unless time ends with am/pm.
        Minutes and seconds are optional, assumed to be 0 if not given
    as_time_object : bool, default False
        Determines whether to return a `datetime.time` object
        or a string of the form 'HH:MM:SS'

    Returns
    -------
    string or `datetime.time`

    Raises
    ------
    `InvalidInputError`

    See Also
    --------
    * `core.input_validation.parse_date`
    * `core.input_validation.parse_datetime`
    * `pandas.to_datetime`

    Examples
    --------
    >>> parse_time('12:00am', as_time_object=False)
    '00:00:00'
    >>> parse_time('12:03:02 AM', as_time_object=False)
    '00:03:02'
    >>> parse_time('9am', as_time_object=False)
    '09:00:00'
    >>> parse_time('10:31', as_time_object=False)
    '10:31:00'
    >>> parse_time('6:17 pm', as_time_object=False)
    '18:17:00'
    >>> parse_time('15', as_time_object=False)
    '15:00:00'
    >>> parse_time('1:01:59 pm', as_time_object=False)
    '13:01:59'
    >>> parse_time('1:01:59 PM', as_time_object=False)
    '13:01:59'
    >>> parse_time('00:31:00', as_time_object=True)
    datetime.time(0, 31)
    """
    time_string = time.lower().replace(' ', '')
    is_twelve_hour_time = time_string.endswith('am') or time_string.endswith('pm')
    inferred_format = '%I' if is_twelve_hour_time else '%H'
    if ':' in time_string:
        inferred_format += ':%M' if time_string.count(':') == 1 else ':%M:%S'
    if is_twelve_hour_time:
        inferred_format += '%p'

    try:
        time_object = pd.to_datetime(
            time_string, format=inferred_format, exact=True, infer_datetime_format=True
        ).time()
        return time_object if as_time_object else str(time_object)
    except ValueError:
        raise InvalidInputError(time, 'cannot be recognized as a time.')


def parse_datetime(dt: str, as_timestamp_object: bool=False) -> Union[str, pd.Timestamp]:
    """Parse string representing a datetime.

    Parameters
    ----------
    dt : string
        String representation of a datetime. If not all time units are given,
        then any missing hours, minutes, or seconds are inferred as 0
    as_timestamp_object : boolean, default False
        Determines whether to return a `pandas.Timestamp` object
        or a string of the form 'YY-MM-DD HH:MM:SS'

    Returns
    -------
    string or `pandas.Timestamp`

    Raises
    ------
    `InvalidInputError`

    See Also
    --------
    * `core.input_validation.parse_time`
    * `core.input_validation.parse_date`
    * `pandas.to_datetime`

    Examples
    --------
    >>> parse_datetime('2021-02-22 00:00:00', as_timestamp_object=False)
    '2021-02-22 00:00:00'
    >>> parse_datetime('2021-02-22 00:00', as_timestamp_object=False)
    '2021-02-22 00:00:00'
    >>> parse_datetime('2021/02/22 22:2:1', as_timestamp_object=False)
    '2021-02-22 22:02:01'
    >>> parse_datetime('2021-02-22', as_timestamp_object=False)
    '2021-02-22 00:00:00'
    >>> parse_datetime('2011.12.25 00:00:01', as_timestamp_object=False)
    '2011-12-25 00:00:01'
    >>> parse_datetime('9/25/2013 11:48 am', as_timestamp_object=False)
    '2013-09-25 11:48:00'
    >>> parse_datetime('2015-2-2 00:21:00', as_timestamp_object=True)
    Timestamp('2015-02-02 00:21:00')
    """
    date, _, time = ' '.join(dt.split()).partition(' ')
    try:
        date_ = parse_date(date, as_date_object=True) if date else None
        time_ = parse_time(time, as_time_object=True) if time else datetime.time(0, 0, 0)
        dt_ = pd.Timestamp.combine(date_, time_)
        return dt_ if as_timestamp_object else dt_.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        raise InvalidInputError(dt, 'must consist of a valid date followed by an optional valid time.')


def parse_quarter(quarter: str, with_year: bool=True) -> str:
    """Parse string representing an academic quarter.

    Parameters
    ----------
    quarter : string
        String representing an academic quarter.
        Any given quarter's name always starts with an academic term
        corresponding to one of Fall, Winter, Spring, or Summer.
    with_year : boolean, default True
        Determines whether or not to expect a year following the academic term.
        In other words, if True then a quarter must contain a term AND year,
        otherwise only a term is acceptable.

    Returns
    -------
    string

    Raises
    ------
    `InvalidInputError`

    Examples
    --------
    >>> parse_quarter('Fall 2013')
    'Fall 2013'
    >>> parse_quarter('F 13')
    'Fall 2013'
    >>> parse_quarter('Fall 2013')
    'Fall 2013'
    >>> parse_quarter('sp 2012')
    'Spring 2012'
    """
    quarter_name, _, quarter_year = quarter.lower().partition(' ')
    map_term, map_year = TIME_UNIT_VALUES.QUARTERS.lookup_by_alias, TIME_UNIT_VALUES.YEARS.lookup_by_alias
    try:
        return (map_term(quarter_name) + ' ' + map_year(quarter_year) if with_year else
                map_term(quarter_name))
    except InvalidInputError:
        message = f'quarter name must correspond to on of {TIME_UNIT_VALUES.QUARTERS.names}'
        message += ' followed by a 2 or 4 digit year in current century.' if with_year else '.'
        raise InvalidInputError(quarter, message) from None


def parse_course(course_name: str, check_records: bool=False) -> str:
    """Parse string representing a Foothill College course.

    Parameters
    ----------
    course_name : string
        course name consisting of up to three components:
        * subject, required: type of course subject (eg math)
        * number, optional: course code (eg 1A)
        * section, optional: course section number (eg 01)
    check_records : bool, default False
        Determines whether the parsed course exists in any request
        within the STEM Center's historical `tutor_request_data`.
        If True then errors are raised in the case of a course not on record,
        otherwise no errors can be raised as the given course is only cleaned

    Returns
    -------
    string

    Raises
    ------
    `InvalidInputError`

    Notes
    -----
    The algorithm goes as follows, for each course name given...:
    1) Remove all excess space and convert to lower case
    2) Breaks string into three components:
        * Subject: substring spanning until first found digit (exclusive)
        * Number: substring spanning from first found digit to space (exclusive)
        * Section: substring spanning from character after space to end of string
    3) Remove anomalies for each component:
        * Period at end of course subject and number (eg math. -> math)
        * F and up to three zeros from beginning of course number (eg F01A -> 1A)
        * 0 or o at beginning of course section (eg 01W -> 1)
    4) Maps course subject (eg: abbreviation)
    5) Upper case course number and section
    6) Check membership in a set of all possible course combinations,
       as taken from the most recently updated tutor request data
    7) If present in the records, return the parsed string

    Examples
    --------
    >>> parse_course('computer science ')
    'Computer Science'
    >>> parse_course('MATH 1A 01W')
    'Mathematics 1A 1W'
    >>> parse_course('Mathematics   1d  02 ')
    'Mathematics 1D 2'
    >>> parse_course('phys 4A 01')
    'Physics 4A 1'
    >>> parse_course('MATH F022. 02')
    'Mathematics 22 2'
    >>> parse_course('chem. 1A. 5')
    'Chemistry 1A 5'
    >>> parse_course('math F0022. 2')
    'Mathematics 22 2'
    >>> parse_course('Comp Sci 1B')
    'Computer Science 1B'
    """
    course_name_ = ' '.join(course_name.split()).lower()
    first_digit_position = next((k for k, char in enumerate(course_name_) if char.isdigit()), -1)
    if first_digit_position == -1:  # assume subject if input has no digits
        subject, number, section = course_name.strip(' '), '', ''
    else:  # otherwise, segment into three components (still works if section not present!)
        subject = course_name_[0:first_digit_position].strip(' ')
        number, _, section = course_name_[first_digit_position:].upper().partition(' ')

    # an 'f' padding number will be at the end of subject string since input sliced at 1st digit
    subject = re.sub('(\. f|\.| f)?$', '', subject)  # remove trailing '.', ' f', '. f'
    number = re.sub('^0{,2}|(\.)?$', '', number)     # remove up to 3 leading 0s and 1 trailing '.'
    section = re.sub('^0|o', '', section)            # remove leading occurrence of o or 0
    if not check_records:
        subject_ = ALL_SUBJECTS.lookup_by_alias(subject)       # let it raise
        return ' '.join([subject_, number, section]).strip(' ')

    # --------- otherwise, check records, and if not available report the reason for missing
    set_of_all_courses = warehouse.get_set_of_all_courses()
    try:
        subject = ALL_SUBJECTS.lookup_by_alias(subject)
        if subject not in set_of_all_courses:
            raise InvalidInputError
    except InvalidInputError:
        raise InvalidInputError(course_name_, f'subject \'{subject}\' is not on record.') from None

    full_course_name = ' '.join([subject, number, section]).strip(' ')
    if full_course_name in set_of_all_courses:
        return full_course_name

    # full course name not record: check if it's due to unavailable course section/number
    course_name_without_section = ' '.join([subject, number]).strip(' ')
    if course_name_without_section in set_of_all_courses:
        raise InvalidInputError(course_name_, f'course \'{course_name_without_section}\' '
                                              f'has no section \'{section}\' on record.')
    if subject in set_of_all_courses:
        raise InvalidInputError(course_name_, f'subject \'{subject}\' has no course number '
                                              f'\'{number}\' on record.')
    raise InvalidInputError(course_name_, 'course name requires a recognizable subject, followed by '
                                          'either an existing course\'s number OR its number and section.')
