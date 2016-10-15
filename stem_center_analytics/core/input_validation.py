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


# todo: clean up below tokens (make case consistent, simplify names, etc)
class ParserDict(OrderedDict):

    """ValidTokenMappings class.

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

    def map_to_token(self, string: str, raise_if_not_found: bool=True) -> str:
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


DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
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
        *[(str(wk), {str(wk)}) for wk in range(0, 12)]
    ),
    # generate args i.e.: [('0', {'0'}), ..., ('11', {'11'})]
    WEEKS_IN_QUARTER=ParserDict(
        *[(str(wk), {str(wk)}) for wk in range(0, 12)]
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
SET_OF_ALL_COURSES = warehouse.get_set_of_all_courses()
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
    ('Psychology',              {'psy', 'psyc', 'psych', 'psychology'})
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
          or no comma at ALL_SUBJECTS
    mapping_func : one str arg function
        The function in which each string element extracted from `user_input`
        is mapped with.
    values_to_slice : array-like of str, default ()
        Assumed as either empty, or a list of ALL_SUBJECTS possible mapped values, in
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

    # a dash is in input, attempt to parse as a dashed string
    if user_input.count('-') == 1 and ' - ' not in user_input:
        raise ValueError('Dashed input must be separated by \' - \' (including spaces).')
    # parse dashed input
    left_token, _, right_token = ' '.join(user_input_.split()).partition(' - ')
    left_value, right_value = mapping_func(left_token), mapping_func(right_token)
    if not values_to_slice:  # endpoints only (no order checking here)
        return left_value, right_value

    # OTHER_SUBJECTS wise we return sliced list
    values = list(values_to_slice)
    left_index, right_index = values.index(left_value), values.index(right_value)
    if left_index < right_index:
        return values[left_index: right_index + 1]
    raise ParsingError('\'{}\' is invalid: dashed string must be of the form '
                       '\'LHS - RHS\' where LHS < RHS.'.format(user_input))


def parse_days(days: Union[str, int, Sequence[Union[str, int]]]) -> List[str]:
    """Parse input to list of weekdays (eg: 'Mon, Fri' to ['Monday', 'Friday'])."""
    return parse_input(user_input=days,
                       mapping_func=TIME_UNIT_VALUES.WEEKDAYS.map_to_token,
                       values_to_slice=TIME_UNIT_VALUES.WEEKDAYS.keys())


def parse_weeks_in_quarter(weeks_in_quarter: Union[int, str, Sequence[Union[int, str]]],
                           is_summer: bool=False) -> List[str]:
    """Parse int(s) or str(s) corresponding to week in quarter to list of strings."""
    if is_summer:
        return parse_input(user_input=weeks_in_quarter,
                           mapping_func=TIME_UNIT_VALUES.WEEKS_IN_SUMMER_QUARTER.map_to_token,
                           values_to_slice=TIME_UNIT_VALUES.WEEKS_IN_SUMMER_QUARTER.keys())
    else:
        return parse_input(user_input=weeks_in_quarter,
                           mapping_func=TIME_UNIT_VALUES.WEEKS_IN_QUARTER.map_to_token,
                           values_to_slice=TIME_UNIT_VALUES.WEEKS_IN_QUARTER.keys())


def parse_quarters(quarters: Union[str, Sequence[str]], with_year: bool=True) -> List[str]:
    """Parse quarter input to a list of full quarter names (eg: Fall 2013).

    Examples
    --------
    >>> parse_quarters('Fall 2013 - Spring 2014')
    ['Fall 2013', 'Winter 2014', 'Spring 2014']
    >>> parse_quarters('F 13 - sp 2014')
    ['Fall 2013', 'Winter 2014', 'Spring 2014']
    >>> parse_quarters('summer 2014 - fall 2014')
    ['Summer 2014', 'Fall 2014']
    >>> parse_quarters('w 14 - w 15')
    ['Winter 2014', 'Spring 2014', 'Summer 2014', 'Fall 2014', 'Winter 2015']
    """
    def parse_quarter(quarter: str) -> str:
        try:
            quarter_name, _, quarter_year = quarter.lower().partition(' ')
            quarter_name_ = TIME_UNIT_VALUES.QUARTERS.map_to_token(quarter_name)
            if with_year:
                return quarter_name_ + ' ' + TIME_UNIT_VALUES.YEARS.map_to_token(quarter_year)
            else:
                return quarter_name_
        except ValueError:
            message = ('\'{}\' is invalid - quarter name must correspond to on of {}'
                       .format(quarter, tuple(TIME_UNIT_VALUES.QUARTERS.keys())))
            if with_year:
                raise ParsingError(message + ' followed by a 2 or 4 digit year in current century.')
            else:
                raise ParsingError(message)

    if isinstance(quarters, str) and ' - ' in quarters:
        left, _, right = quarters.partition(' - ')
        if len(left.split()) != len(left.split()):
            raise ParsingError('Quarters on each side of dash must have exact same number'
                               'of components:\n    - eg: \'F 13 - F 14\' - not \'F - F 14\').')
    return parse_input(user_input=quarters,
                       mapping_func=parse_quarter,
                       values_to_slice=TIME_UNIT_VALUES.QUARTERS_WITH_YEARS.keys())


def parse_years(years: Union[Union[str, int], Sequence[Union[str, int]]]) -> List[int]:
    """Parse string(s) or int(s) years (eg 12, 2013) to list of integers.

    Examples
    --------
    >>> parse_years('2013 - 2014')
    ['2013', '2014']
    """
    def parse_year(year: str or int) -> int:
        try:
            return TIME_UNIT_VALUES.YEARS.map_to_token(str(year))
        except Exception:
            raise ParsingError('\'{}\' is invalid: both years in the range must be in '
                               'current century with either two or four digits'.format(year))
    return parse_input(user_input=years,
                       mapping_func=parse_year,
                       values_to_slice=TIME_UNIT_VALUES.YEARS.keys())


def parse_courses(course_names: Union[str, Sequence[str]], as_tuple: bool=False) \
        -> List[Union[str, Tuple[str, str, str]]]:
    """Parse course input to list of cleaned components or course names.

    Parameters
    ----------
    course_names : str or array-like of str
        course name consisting of up to three components:
        * subject, required: type of course subject (eg math)
        * number, optional: course code (eg 1A)
        * section, optional: course section number (eg 01)
    as_tuple : bool, default False
        Determines whether each parsed course name should be broken up or not

    Returns
    -------
    array-like of 3 element string tuples
        If `as_tuple`=True
    list of str
        If `as_tuple`=False

    Raises
    ------
    ParsingError
        * If the fully parsed course is not on record, according to the most
          recent tutor request data

    Notes
    -----
    The algorithm goes as follows, for each course name given...:
    1) Remove ALL_SUBJECTS excess space and convert to lower case
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
    6) Check membership in a set of ALL_SUBJECTS possible course combinations,
       as taken from the most recently updated tutor request data.
    7) If present in the records, returns 3 element tuple if `as_tuple`=True,
       else returns string.

    Examples
    --------
    >>> parse_courses('computer science ', as_tuple=True)
    [('Computer Science', '', '')]
    >>> parse_courses('MATH 1A 01W', as_tuple=True)
    [('Mathematics', '1A', '1W')]
    >>> parse_courses('Mathematics   1d  02 ', as_tuple=True)
    [('Mathematics', '1D', '2')]
    >>> parse_courses('phys 4A 01', as_tuple=True)
    [('Physics', '4A', '1')]
    >>> parse_courses('MATH F022. 02', as_tuple=True)
    [('Mathematics', '22', '2')]
    >>> parse_courses('chem. 1A. 5', as_tuple=True)
    [('Chemistry', '1A', '5')]
    >>> parse_courses('chem. 1A. 5, math F0022. 2, math')
    ['Chemistry 1A 5', 'Mathematics 22 2', 'Mathematics']
    >>> parse_courses('cs 1a,phys 4c,mat 2a, Comp Sci 1B')
    ['Computer Science 1A', 'Physics 4C', 'Mathematics 2A', 'Computer Science 1B']
    """
    # todo: add support to return the three component tuple as three parallel lists instead
    def parse_full_course_name(course_name: str):
        course_name_ = ' '.join(course_name.split()).lower()
        first_digit_position = next((k for k, char in enumerate(course_name_) if char.isdigit()), -1)
        if first_digit_position == -1:  # assume subject if input has no digits
            subject, number, section = course_name.strip(' '), '', ''
        else:  # otherwise, segment into three components (still works if section not present!)
            subject = course_name_[0:first_digit_position].strip(' ')
            number, _, section = course_name_[first_digit_position:].upper().partition(' ')

        # an 'f' padding number will be at the end of subject string since input sliced at 1st digit
        subject = re.sub('(\. f|\.| f)?$', '', subject)  # remove trailing '.', ' f', '. f'
        number = re.sub('^0{,2}|(\.)?$', '', number)  # remove up to 3 leading 0s and 1 trailing '.'
        section = re.sub('^0|o', '', section)  # remove leading occurrence of o or 0
        try:
            subject = ALL_SUBJECTS.map_to_token(subject)
            if subject not in SET_OF_ALL_COURSES:
                raise ParsingError
        except ParsingError:
            raise ParsingError('Subject \'{}\' is not on record.'.format(subject)) from None

        full_course_name = ' '.join([subject, number, section]).strip(' ')
        if full_course_name in SET_OF_ALL_COURSES:
            return (subject, number, section) if as_tuple else full_course_name

        # full course name not record: check if it was due to unavailable course section/number
        course_name_without_section = ' '.join([subject, number]).strip(' ')
        if course_name_without_section in SET_OF_ALL_COURSES:
            raise ParsingError('Course \'{}\' has no section \'{}\' on record.'
                               .format(course_name_without_section, section))
        if subject in SET_OF_ALL_COURSES:
            raise ParsingError('Subject \'{}\' has no course number \'{}\' on record.'
                               .format(subject, number))
        raise ParsingError('\'{}\' cannot be recognized - course name requires a recognizable'
                           'subject, followed by either an existing course\'s number OR its'
                           'number and section.'.format(course_name_))

    if ' - ' in course_names:
        raise ParsingError('\'{}\' is invalid - course input cannot be dashed.')
    return parse_input(user_input=course_names, mapping_func=parse_full_course_name)


def parse_datetime_range(datetimes: Union[str]) -> Tuple[str, str]:
    """Parse dashed string representing a range of datetimes.

    Parameters
    ----------
    datetimes : str or array-like of str
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
    # after `parse_input()`, map any values to datetime as necessary, and ensure dashed ordering
    parsed_values = parse_input(user_input=datetimes, mapping_func=parse_datetime)
    parsed_values = list(map(str, parsed_values))

    if isinstance(datetimes, str) and ' - ' in datetimes:
        if parsed_values[0] < parsed_values[-1]:  # two parsed end points, return as a pair
            return tuple(parsed_values)
        raise ParsingError('\'{}\' is invalid - dashed string must be of the form '
                           '\'LHS - RHS\' where LHS < RHS.'.format(datetimes))
    return parsed_values


def parse_time_of_day(time: str) -> str:
    """Parse string representing time of day to a format 'HH:MM:SS'.

    Parameters
    ----------
    time : str
        Represents time of day in format 'hour[:minutes][am/pm]'.
        Hour is required, 24 hour format assumed, unless time ends with am/pm.
        Minutes (prefixed by colon) is optional, assumed to be 0 if not given.

    Returns
    -------
    If `parse_to_datetime` is False : str
        Parsed time of day in format 'HH:MM'.
    If `parse_to_datetime` is True : datetime-like object
        Datetime object where only the hour and minute fields are relevant.

    Raises
    ------
    ParsingError if none of the above `time` conditions are met.

    Examples
    --------
    >>> parse_time_of_day('12:00am')
    '00:00:00'
    >>> parse_time_of_day('12:03:02 AM')
    '00:03:02'
    >>> parse_time_of_day('9am')
    '09:00:00'
    >>> parse_time_of_day('10:31')
    '10:31:00'
    >>> parse_time_of_day('6:17 pm')
    '18:17:00'
    >>> parse_time_of_day('15')
    '15:00:00'
    >>> parse_time_of_day('1:01:59 pm')
    '13:01:59'
    >>> parse_time_of_day('1:01:59 PM')
    '13:01:59'
    """
    time_string = time.lower().replace(' ', '')
    is_twelve_hour_time = time_string.endswith('am') or time_string.endswith('pm')
    inferred_format = '%I' if is_twelve_hour_time else '%H'
    if ':' in time_string:
        inferred_format += ':%M' if time_string.count(':') == 1 else ':%M:%S'
    if is_twelve_hour_time:
        inferred_format += '%p'

    try:
        dt = datetime.datetime.strptime(time_string, inferred_format)
        return ':'.join([('0' if 0 <= t <= 9 else '') + str(t)
                         for t in (dt.hour, dt.minute, dt.second)])
    except ValueError:
        raise ParsingError('\'{}\' is invalid - time must be of format '
                           '\'HH[:MM:SS] [am/pm]\'.'.format(time.strip())) from None


def parse_datetime(dt: Union[str, object], as_string: bool=True) -> Union[str, pd.datetime]:
    """Parse string containing datetime of format 'YY-MM-DD [HH:MM:SS]'.

    Parameters
    ----------
    dt : str or date-like object
        Date of form 'YY-MM-DD HH:MM:SS' where H:M:S is optional
    as_string : bool, default True
        Determines format of return

    Returns
    -------
    tuple of str
        Start/end dates of format '%Y-%m-%d %H:%M:%S' with H:M:S zero by default
    list of str
        Parsed datetimes of above format

    Raises
    ------
    ParsingError
        * If `dt` cannot be cast to a str
        * If date not of the form 'YY-MM-DD' follow by an optional time
          of the form followed by a time of the format HH[:MM:SS am/pm

    See Also
    --------
    * `parse_time_of_day`
    * `pandas.to_datetime`

    Examples
    --------
    >>> parse_datetime('2021-02-22 00:00:00')
    '2021-02-22 00:00:00'
    >>> parse_datetime('2011.12.25 00:00:01')
    '2011-12-25 00:00:01'
    """
    try:
        date, _, time = ' '.join(str(dt).split()).partition(' ')
    except TypeError:
        raise ParsingError('Only strings or date-like objects can be parsed '
                           'to a datetime-string.')

    try:
        dt_string = date + ' ' + parse_time_of_day(time) if time else date
        dt = pd.to_datetime(dt_string, format=DATETIME_FORMAT)
        return str(dt) if as_string else dt
    except ValueError:
        raise ParsingError('\'{}\' is invalid - date must be of the form \'YY-MM-DD\', optionally'
                           'followed by a time of the format \'HH[:MM:SS am/pm]\''.format(dt))


def parse_time_range(time_range: str) -> Tuple[str, str]:
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
    Visit the docstring of `parse_time_of_day` for more detailed information
    on what individual times (on either side of the given dash) are valid.
    """
    if isinstance(time_range, str) and ' - ' in time_range:
        return parse_input(user_input=time_range, mapping_func=parse_time_of_day, values_to_slice=())
    raise ParsingError('\'{}\' is invalid - `time_range` is only accepted '
                       'as dashed input.'.format(time_range))


def parse_time_unit_name(time_unit_label: str) -> str:
    """Return given interval option as a normalized time unit name (eg: year, quarter, week_num).

    Examples
    --------
    >>> parse_time_unit_name('hours')
    'hour'
    >>> parse_time_unit_name('hrs')
    'hour'
    >>> parse_time_unit_name('yrs')
    'year'
    >>> parse_time_unit_name('yearly')
    'year'
    >>> parse_time_unit_name(' qtr ')
    'quarter'
    >>> parse_time_unit_name('DAYS')
    'day_in_week'
    >>> parse_time_unit_name('wk in qtr')
    'week_in_quarter'
    """
    time_unit_ = time_unit_label.strip(' ').lower()
    try:
        return TIME_UNIT_LABEL_NAMES.map_to_token(time_unit_)
    except ParsingError:
        raise ParsingError('\'{}\' is invalid - time unit label must correspond to one of {}.'
                           .format(time_unit_, TIME_UNIT_LABEL_NAMES.keys())) from None


def parse_metric_name(metric_label: str) -> str:
    """Return given interval option as a normalized time unit (demand, wait_time)."""
    metric_label_ = metric_label.strip(' ').lower()
    try:
        return METRIC_LABEL_NAMES.map_to_token(metric_label_)
    except ParsingError:
        raise ParsingError('\'{}\' is invalid - metric type must correspond to one of {}.'
                           .format(metric_label_, tuple(METRIC_LABEL_NAMES.keys()))) from None
