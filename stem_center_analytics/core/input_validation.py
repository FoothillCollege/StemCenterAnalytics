"""Provides input/data validation functionality.

todo: explicitly define dashed, comma-delimited, and sequence of string inputs.

Notes
-----
* Token value start from zero, so make sure to add one to parsed value for
  any client facing code.
* Any function ing Tuple[str, str], indicates that the values to the left
  and right of the dash are parsed, as it doesn't make sense to return
  all the immediate values (eg `parse_datetimes`). Thus, such functions
  only parse ranges (dashed input) and nothing more.
"""
import re
import types
import datetime
from collections import OrderedDict
from typing import Sequence, Union, Tuple, List, Set

import pandas as pd

from stem_center_analytics import warehouse


class ParsingError(ValueError):
    """Base exception raised for value-related errors encountered while parsing a string."""
    pass


class ParserDict(OrderedDict):

    """Create a key that maps to a set of it's acceptable aliases.

    Examples
    --------
    will raise error: TokenMappings(('monday', ('mon', 'm')))
    won't raise error: TokenMappings(('monday', {'mon', 'm'}))
    """

    def __init__(self, *args: Tuple[str, Set[str]]):
        """Creates mappings (1st item in pair str, 2nd item must support membership testing)."""
        # in case you think the below is performance bloat, %timeit reveals a mere 5 nanosecond
        # difference (~52 vs ~47 without the validation code) == that's 5 billionths of a second!
        for pair in args:
            message = 'Cannot construct {} object - '.format(self.__class__.__name__)
            if len(pair) != 2 or not isinstance(pair[1], set):
                raise ValueError(message + 'ParserDict arguments must be of form Tuple[str, set].')
            if not isinstance(pair[0], str):
                raise ValueError(message + 'keys can contain only contain '
                                           'letters, digits, spaces, and underscores.')
            if (not isinstance(pair[0], str) or not
                    pair[0].replace('_', '').replace(' ', '').isalnum()):
                raise ValueError(message + 'keys can contain only contain '
                                           'letters, digits, spaces, and underscores.')
            if any((not isinstance(s, str) or not s.replace('_', '').replace(' ', '').isalnum())
                   for s in pair[1]):
                raise ValueError(message + 'ParserDict strings in each set can only contain '
                                           'letters, digits, spaces, and underscores.')
        super().__init__(args)

    def parse(self, string: str, raise_if_not_found: bool=True) -> str:
        """Map string to token, raising if not found (or return '')."""
        tokens = self.keys()
        for token in tokens:  # check each parser set for membership
            if string in self.__getitem__(token):
                return token

        if not raise_if_not_found:
            return ''
        tokens_ = tuple(tokens)
        tokens_ = (tokens_[:2] + ('...',) + tokens_[-2:]) if len(tokens) > 4 else tokens_
        raise ParsingError('\'{}\' cannot be recognized as one of the following - {}'
                           .format(string, tokens_))


COL_NAMES = ParserDict(
    ('date',            {'date', 'date_of_request'}),
    ('time_of_request', {'time_of_request', 'time of request', 'start_time', 'start'}),
    ('time_of_service', {'time_of_service', 'time of service', 'end_time', 'end'}),
    ('wait_time',       {'wait_time', 'wait time', 'waittime'}),
    ('course_name',     {'course_name', 'course name'}),
    ('course_section',  {'course_section', 'course section', 'section', 'sec'})
)
METRIC_LABEL_NAMES = ParserDict(
    ('wait_time', {'wait_time', 'wait time', 'waittime'}),
    ('demand',    {'demand'})
)
TIME_UNIT_LABEL_NAMES = ParserDict(
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
TIME_UNIT_VALUES = types.SimpleNamespace(
    # generate args i.e.: [('0', {'0'}), ..., ('23', {'23'})]
    HOURS=ParserDict(
        *[(str(hr), {str(hr)}) for hr in range(0, 24)]
    ),
    WEEKDAYS=ParserDict(
        ('Monday',    {'1', 'm', 'mo', 'mon', 'monday'}),
        ('Tuesday',   {'2', 't', 'tu', 'tue', 'tues', 'tuesday'}),
        ('Wednesday', {'3', 'w', 'we', 'wed', 'wednesday'}),
        ('Thursday',  {'4', 'r', 'th', 'thu', 'thur', 'thurs', 'thursday'}),
        ('Friday',    {'5', 'f', 'fr', 'fri', 'friday'}),
        ('Saturday',  {'6', 's', 'sa', 'sat', 'saturday'}),
        ('Sunday',    {'7', 'u', 'su', 'sun', 'sunday'})
    ),
    WEEKS_IN_SUMMER_QUARTER=ParserDict(
        *[(str(wk), {str(wk)}) for wk in range(1, 7)]
    ),
    # generate args i.e.: [('1', {'1'}), ..., ('12', {'12'})]
    WEEKS_IN_QUARTER=ParserDict(
        *[(str(wk), {str(wk)}) for wk in range(1, 13)]
    ),
    MONTHS=ParserDict(
        ('January',   {'1', 'jan', 'january'}),
        ('February',  {'2', 'feb', 'february'}),
        ('March',     {'3', 'mar', 'march'}),
        ('April',     {'4', 'apr', 'april'}),
        ('May',       {'5', 'may'}),
        ('June',      {'6', 'jun', 'june'}),
        ('July',      {'7', 'jul', 'july'}),
        ('August',    {'8', 'aug', 'august'}),
        ('September', {'9', 'sep', 'september'}),
        ('October',   {'10', 'oct', 'october'}),
        ('November',  {'11', 'nov', 'november'}),
        ('December',  {'12', 'dec', 'december'}),
    ),
    QUARTERS=ParserDict(
        ('Fall',   {'1', 'f', 'fa', 'fall'}),
        ('Winter', {'2', 'w', 'wi', 'win', 'winter'}),
        ('Spring', {'3', 's', 'sp', 'spr', 'spring'}),
        ('Summer', {'4', 'u', 'su', 'sum', 'summer'})
    ),
    # generate args i.e.: [('2000', {'2000', '00'}), ..., ('2099', {'99', '2099'})]
    YEARS=ParserDict(
        *[(str(yr), {str(yr), str(yr)[-2:]}) for yr in range(2000, 2100)]
    ),
    # generate current format (as stored in db) possibilities, i.e.: 'F 2013', 'W 2014', ...
    QUARTERS_WITH_YEARS=ParserDict(
        *[((qtr_name + ' ' + str(qtr_yr)), {qtr_name + ' ' + str(qtr_yr)})
          for qtr_yr in range(2000, 2100)
          for qtr_name in ('Winter', 'Spring', 'Summer', 'Fall')]
    )
)

# official course subject names were referenced
#SET_OF_ALL_COURSES = warehouse.get_set_of_all_courses()
CORE_SUBJECTS = ParserDict(
    ('Mathematics',      {'mat', 'math', 'mathematics'}),
    ('Physics',          {'phy', 'phys', 'physics'}),
    ('Biology',          {'bio', 'biol', 'biology'}),
    ('Chemistry',        {'che', 'chem', 'chemistry'}),
    ('Engineering',      {'eng', 'engr', 'engi', 'engineering'}),
    ('Computer Science', {'cs', 'com', 'c s', 'comp', 'comp sci', 'computer science'})
)
OTHER_SUBJECTS = ParserDict(
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
ALL_SUBJECTS = ParserDict(
    *(list(CORE_SUBJECTS.items()) + list(OTHER_SUBJECTS.items()))
)


def parse_input(user_input: Union[str, Sequence[str]], mapping_func: callable(str),
                values_to_slice: Sequence[Union[str, object]] = ()) \
        -> Union[List[Union[str, object]], Tuple[str, str], Tuple[object, object]]:
    """Maps each token of a dashed, comma-delimited, or sequence of strings.

    Parameters
    ----------
    user_input : str or array-like of str
        * sequence of strings - array-like of strings
        * dashed string - string containing dash
        * delimited string - string containing values separated by commas,
          or no comma at all
    mapping_func : one str arg function
        The function in which each string element extracted from `user_input`
        is mapped with.
    values_to_slice : array-like of str, default ()
        Assumed as either empty, or a list of all possible mapped values, in
        which is sliced in the case of a dashed string.

    Returns
    -------
    array-like of str or object:
        * If array-like return as list of map values
        * If string containing dash, map left and right values:
        * If `values_to_slice` is empty, then left and right
        * If `values_to_slice` is not empty, then list sliced by left and right
          values, inclusive
        * Else values taken between commas and mapped to list. This includes
          the case of a single string without commas.

    Raises
    ------
    ParsingError
        * If not a string or sequence of strings
        * If input contains a single dash without surrounding spaces
        * If input cannot be parsed as either a sequence or delimited string,
          which means that no components could be successfully mapped
        * If string containing dash and the left mapped value exceeds the
          right, as no range can be generated.
        * ANY OTHER_SUBJECTS error raise while invoking `mapping_func`

    Examples
    --------
    >>> parse_input('foo, bar, baz', str.capitalize)
    ['Foo', 'Bar', 'Baz']
    >>> parse_input('fOo,  bar, BAz', str.capitalize)
    ['Foo', 'Bar', 'Baz']
    >>> parse_input('foo - baz', str.capitalize, ['Foo', 'Bar', 'Baz'])
    ['Foo', 'Bar', 'Baz']
    >>> parse_input('Foo', str.capitalize, ['Foo', 'Bar', 'Baz'])
    ['Foo']
    """
    if isinstance(user_input, Sequence) and not isinstance(user_input, str):
        return [mapping_func(' '.join(s.split())) for s in user_input]
    if not isinstance(user_input, str):
        raise ParsingError('Only a collection of strings (list/tuple/etc) OR a '
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
    raise ParsingError('\'{}\' is invalid: dashed string must be of the form '
                       '\'LHS - RHS\' where LHS < RHS.'.format(user_input))


def parse_date(dt: str, as_date_object: bool=False) -> Union[str, datetime.date]:
    """Parse string containing datetime of format 'YY-MM-DD [HH:MM:SS]'.

    Returns
    -------
    datetime.datetime object, if `as_date_object`=True
        * If `include_time`=True return parsed result as pandas.datetime object
    str, if `as_date_object`=False
        * If `include_time`=True return string with the format of
          '%Y-%m-%d %H:%M:%S' with H:M:S zero by default

    See Also
    --------
    * `input_validation.parse_time`
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
    date, _, time = dt.partition(' ')
    if time.replace(' ', '') != '':
        raise ParsingError('\'{}\' is invalid - only date-like strings can be parsed.'.format(dt))

    try:
        dt_ = pd.to_datetime(date, infer_datetime_format=True).date()
        return dt_ if as_date_object else dt_.strftime('%Y-%m-%d')
    except Exception:
        raise ParsingError('\'{}\' is invalid - cannot be recognized as a date.'.format(date))


def parse_time(time: str, as_time_object: bool=False) -> Union[str, datetime.time]:
    """Parse string representing time of day to a format 'HH:MM:SS'.

    Parameters
    ----------
    time : string
        Represents time of day in format 'hour[:minutes[:seconds][am/pm]'.
        Hour is required, 24 hour format assumed, unless time ends with am/pm.
        Minutes (prefixed by colon) is optional, assumed to be 0 if not given.
    as_time_object : bool, default False
        Determine if value to return is string or datetime object

    Returns
    -------
    * If `as_time_object` is True : datetime.time object
        datetime.time object
    * If `as_time_object` is False : string
        Parsed time of day in format 'HH:MM:SS'

    Raises
    ------
    ParsingError if none of the above `time` conditions are met.

    See Also
    --------
    * `input_validation.parse_date`
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
        raise ParsingError('\'{}\' is invalid - cannot be recognized as a time.'.format(time))


def parse_datetime(dt: str, as_timestamp_object: bool=False) -> Union[str, pd.Timestamp]:
    """Parse string containing datetime of format 'YY-MM-DD [HH:MM:SS]'.

    Returns
    -------
    datetime.datetime object
        * If `as_datetime_object`=True
    str
        * If `as_datetime_object`=False
            * If time given, string of format '%Y-%m-%d %H:%M:%S'
            * If time not given, string of format '%Y-%m-%d 00:00:00'

    See Also
    --------
    * `input_validation.parse_time`
    * `input_validation.parse_date`
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
        raise ParsingError('\'{}\' is invalid - must consist of a valid date '
                           'followed by an optional valid time.'.format(dt))


def parse_quarter(quarter: str, with_year: bool=True) -> str:
    """Parse quarter input to a list of full quarter names (eg: Fall 2013).

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
    map_term, map_year = TIME_UNIT_VALUES.QUARTERS.parse, TIME_UNIT_VALUES.YEARS.parse
    try:
        return (map_term(quarter_name) + ' ' + map_year(quarter_year) if with_year else
                map_term(quarter_name))
    except ParsingError:
        message = ('\'{}\' is invalid - quarter name must correspond to on of {}'
                   .format(quarter, tuple(TIME_UNIT_VALUES.QUARTERS.keys())))
        message += ' followed by a 2 or 4 digit year in current century.' if with_year else '.'
        raise ParsingError(message) from None


def parse_course(course_name: str, check_records: bool=False) -> str:
    """Parse course input to cleaned components or full course name.

    Parameters
    ----------
    course_name : str
        course name consisting of up to three components:
        * subject, required: type of course subject (eg math)
        * number, optional: course code (eg 1A)
        * section, optional: course section number (eg 01)
    check_records : bool, default False
        Determines whether each parsed course should be checked in the course records

    Returns
    -------
    str
        Parsed course name with the same components of the original input

    Raises
    ------
    ParsingError
        * If the fully parsed course is not on record, according to the most
          recent tutor request data

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
    set_of_all_courses = warehouse.get_set_of_all_courses()
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
        subject_ = ALL_SUBJECTS.parse(subject)       # let it raise
        return ' '.join([subject_, number, section]).strip(' ')

    # --------- otherwise, check records, and if not available report the reason for missing
    try:
        subject = ALL_SUBJECTS.parse(subject)
        if subject not in set_of_all_courses:
            raise ParsingError
    except ParsingError:
        raise ParsingError('Subject \'{}\' is not on record.'.format(subject)) from None

    full_course_name = ' '.join([subject, number, section]).strip(' ')
    if full_course_name in set_of_all_courses:
        return full_course_name

    # full course name not record: check if it was due to unavailable course section/number
    course_name_without_section = ' '.join([subject, number]).strip(' ')
    if course_name_without_section in set_of_all_courses:
        raise ParsingError('Course \'{}\' has no section \'{}\' on record.'
                           .format(course_name_without_section, section))
    if subject in set_of_all_courses:
        raise ParsingError('Subject \'{}\' has no course number \'{}\' on record.'
                           .format(subject, number))
    raise ParsingError('\'{}\' cannot be recognized - course name requires a recognizable'
                       'subject, followed by either an existing course\'s number OR its'
                       'number and section.'.format(course_name_))
