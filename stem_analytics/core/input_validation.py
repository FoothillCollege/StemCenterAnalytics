"""Validation of data input.

Includes specific input 'parsing' functionality...
as well as any relevant constants needed to do so.

IMPORTANT NOTE: THE TOKEN VALUES START FROM ZERO, SO ANYWHERE (LIKE CLIENT FACING)
                MAKE SURE TO ADD ONE TO THE PARSED VALUE TO START THE COUNT FROM ONE!!

NOTE: THIS IS FORE CLIENT FACING CODE< AND IS NOT MEANT TO BE AS STRICT AS THE PARSING IN THE
      clean_data.py script.
Notes:
    recall:
    strong assumptions that:
    time_of_request,quarter,week_in_quarter,day,course,wait_time,anon_stud_id
    2013-09-25 11:05:32,F 13,1,W,chem 30a 1,62,782484
    currently the individual element parsing reflect the format present in db.

    Elaborate how the requirements for a 'dashed' string are stricter and require more checking, etc.

    TODO: EXPLAIN (HERE) DASHED VS DELIMITED VS SEQUENCE INPUTS AND DEFINE TOKENIZE, ETC
    # TODO: INSERT description of dashed string versus delimited string, and how they
    # TODO: are the two integral types in this module (aka if not dashed, assume delimited, etc.)
"""
import re
import types
import datetime
import functools
from typing import Iterable, Union, Tuple, List

import pandas as pd

from stem_analytics import warehouse
from stem_analytics.utils import strings
from stem_analytics.utils.strings import ParsingError

# the above to-do would be mostly for interface reasons, to limit input validation...when possible
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
DATETIME_FORMAT = DATE_FORMAT + ' ' + TIME_FORMAT


def cast_to_dt(dt_string: str) -> pd.datetime:
    """Return datetime string of format 'YY-MM-DD [HH:MM:SS]' as a datetime object."""
    return pd.to_datetime(dt_string, format=DATETIME_FORMAT)  # note: format kw required


# todo: possibly move the below to json?
# NOTE on use of the below mappings: for numeric checking, use range or set membership tests
# instead yet the ordering/keys is still essential for easy slicing, comparison checking
label_types = types.SimpleNamespace(
    metric=strings.ParserDict(
        ('wait_time', {'wait_time', 'wait time', 'waittime'}),
        ('demand',    {'demand'})
    ),
    time_unit=strings.ParserDict(
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
)
academic_time_units = types.SimpleNamespace(
    # generate args i.e.: [('0', {'0'}), ..., ('23', {'23'})]
    hour=strings.ParserDict(
        *[(str(hr), {str(hr)}) for hr in range(0, 24)]
    ),
    weekday=strings.ParserDict(
        ('Monday',    {'1', 'm', 'mo', 'mon', 'monday'}),
        ('Tuesday',   {'2', 't', 'tu', 'tue', 'tues', 'tuesday'}),
        ('Wednesday', {'3', 'w', 'we', 'wed', 'wednesday'}),
        ('Thursday',  {'4', 'r', 'th', 'thu', 'thur', 'thurs', 'thursday'}),
        ('Friday',    {'5', 'f', 'fr', 'fri', 'friday'}),
        ('Saturday',  {'6', 's', 'sa', 'sat', 'saturday'}),
        ('Sunday',    {'7', 'u', 'su', 'sun', 'sunday'})
    ),
    week_in_summer_quarter=strings.ParserDict(
        *[(str(wk), {str(wk)}) for wk in range(0, 12)]
    ),
    # generate args i.e.: [('0', {'0'}), ..., ('11', {'11'})]
    week_in_quarter=strings.ParserDict(
        *[(str(wk), {str(wk)}) for wk in range(0, 12)]
    ),
    month=strings.ParserDict(
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
    quarter=strings.ParserDict(
        ('Fall',   {'1', 'f', 'fa', 'fall'}),
        ('Winter', {'2', 'w', 'wi', 'win', 'winter'}),
        ('Spring', {'3', 's', 'sp', 'spr', 'spring'}),
        ('Summer', {'4', 'u', 'su', 'sum', 'summer'})
    ),
    # generate args i.e.: [('2000', {'2000', '00'}), ..., ('2099', {'99', '2099'})]
    year=strings.ParserDict(
        *[(str(yr), {str(yr), str(yr)[-2:]}) for yr in range(2000, 2100)]
    )
)
# todo: Find an efficient persistent storage option for sets (might not be necessary though)
SET_OF_ALL_COURSES = warehouse.get_set_of_all_courses()

# generate current format (as stored in db) possibilities, i.e.: 'F 2013', 'W 2014', ...
academic_time_units.quarter_with_year = strings.ParserDict(
    *[((qtr_name + ' ' + qtr_yr), {qtr_name + ' ' + qtr_yr})
      for qtr_yr in academic_time_units.year
      for qtr_name in ('Winter', 'Spring', 'Summer', 'Fall')]
)
# store subject name (done)
# store subject and number
# store subject and number and section

# official course subject names were referenced
course_subject_types = types.SimpleNamespace(
    core=strings.ParserDict(
        ('Mathematics',      {'mat', 'math', 'mathematics'}),
        ('Physics',          {'phy', 'phys', 'physics'}),
        ('Biology',          {'bio', 'biol', 'biology'}),
        ('Chemistry',        {'che', 'chem', 'chemistry'}),
        ('Engineering',      {'eng', 'engr', 'engi', 'engineering'}),
        ('Computer Science', {'cs', 'com', 'c s', 'comp', 'comp sci', 'computer science'})
    ),
    other=strings.ParserDict(
        ('Accounting',              {'acc', 'actg', 'accounting'}),
        ('Astronomy',               {'ast', 'astr', 'astro', 'astronomy'}),
        ('Anthropology',            {'ant', 'anth', 'anthro', 'anthropology'}),
        ('Business',                {'bus', 'busi', 'business'}),
        ('Economics',               {'eco', 'econ', 'economics'}),
        ('Non Credit Basic Skills', {'non', 'ncbs', 'non credit basic skills'}),
        ('Psychology',              {'psy', 'psyc', 'psych', 'psychology'})
    )
)
course_subject_types.all = strings.ParserDict(
    *(list(course_subject_types.core.items()) + list(course_subject_types.other.items()))
)


def _parse_input(user_input: str or Iterable(str), mapping_func: callable(str),
                 values_to_slice: Iterable[str or object]=None) \
        -> List[str or object] or Tuple[str, str] or Tuple[object, object]:
    """Parse input (helper).

    Examples
    --------
    >>> _parse_input('foo, bar, baz', str.capitalize, None)
    ['Foo', 'Bar', 'Baz']
    >>> _parse_input('fOo,  bar, BAz', str.capitalize, None)
    ['Foo', 'Bar', 'Baz']
    >>> _parse_input('foo - baz', str.capitalize, ['Foo', 'Bar', 'Baz'])
    ['Foo', 'Bar', 'Baz']
    >>> _parse_input('Foo', str.capitalize, ['Foo', 'Bar', 'Baz'])
    ['Foo']
    """
    if not isinstance(user_input, str) and not isinstance(user_input, Iterable):
        raise ParsingError('Only a collection of strings (list/tuple/etc) OR a '
                           'single (eg: delimited/dashed string) can be parsed.')
    if isinstance(user_input, str):
        if ' - ' in user_input:
            return strings.parse_dashed_string(user_input, mapping_func, values_to_slice)
        else:
            return strings.parse_delimited_string(user_input, mapping_func)

    # user_input inferred to be an Iterable - try as collection
    return strings.parse_collection_of_strings(user_input, mapping_func)


def parse_time_range(time_range: str) -> str:
    r"""Parse string representing time of day to two time strings of format='HH[:MM:SS]'.

    Parameters
    ----------
    time_range : str
        Dashed string of format 'start_time - end_time' representing the time
        range, where the value of 'start_time' is <= the value of 'end_time'.

    Returns
    -------
    if parse_to_datetime is False : tuple of str
        Representing a start-time, end-time pair (format HH:MM) of desired time range.
    if parse_to_datetime is True : tuple of two datetime-objects
        Pair of start/end times as datetime objects. Note that the year is
        arbitrary, and only the hour and minute fields are relevant.
    Raises
    ------
    ParsingError if incorrect dashed-string format or individual times
        cannot be parsed to a 24 hour 'HH:MM' format.
    ParsingError if value 'start_time' >= 'end_time', as no range can be generated.

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
    return _parse_input(user_input=time_range, mapping_func=parse_time_of_day)


def parse_days(days: Union[str, int, Iterable[Union[str, int]]]) -> List[str]:
    """Parse str(s) corresponding to days in the week (eg: 'Mon Wed' to ['Monday', 'Wednesday').

    Notes:
        - Allows dashed and delimited strings.
        - if int: 1 denotes monday and 7 denotes sunday
    """
    return _parse_input(user_input=days,
                        mapping_func=academic_time_units.day.map_to_token,
                        values_to_slice=academic_time_units.day.keys())


def parse_weeks_in_quarter(weeks_in_quarter: Union[int, str, Iterable[Union[int, str]]],
                           is_summer: bool=False) -> List[str]:
    """Parse str(s) corresponding to weeks in quarter (eg: '1-12') to list of WeekInQuarters Enums.

    Notes:
        Allows dashed and delimited strings.

    is_summer: bool=False
    """
    if is_summer:
        return _parse_input(user_input=weeks_in_quarter,
                            mapping_func=academic_time_units.week_in_summer_quarter.map_to_token,
                            values_to_slice=academic_time_units.week_in_summer_quarter.keys())
    else:
        return _parse_input(user_input=weeks_in_quarter,
                            mapping_func=academic_time_units.week_in_quarter.map_to_token,
                            values_to_slice=academic_time_units.week_in_quarter.keys())


def parse_quarters(quarters: Union[str, Iterable[str]], with_year: bool=True) -> List[str]:
    """Parse str(s) corresponding to quarters (eg: 'F - W') to a list of Quarter Enums.

    Notes
    -----
    Allows dashed and delimited strings.
    requires year unless otherwise specified.
    as far as dashed order is concerned:
    # F 13 < W 13: FALSE!      therefore: F 13 >= W 13
    # F 13 < W 12: FALSE!      therefore: F 13 >= W 12

    Examples
    --------
    # TODO: change to quarters!
    # 'Fall 2013 - Spring 2010' should raise!
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
            quarter_name_ = academic_time_units.quarter.map_to_token(quarter_name)
            if with_year:
                return quarter_name_ + ' ' + academic_time_units.year.map_to_token(quarter_year)
            else:
                return quarter_name_
        except ValueError:
            quarter_types = strings.pretty_format_list(items=academic_time_units.quarter.keys(), conj='or')
            if with_year:
                raise ParsingError('\'{}\' is invalid - quarter name must correspond '
                                   'to either {}, follow by a two or four digit year in '
                                   'current century.'.format(quarter, quarter_types))
            else:
                raise ParsingError('\'{}\' is invalid - quarter name must correspond '
                                   'to either {}.'.format(quarter, quarter_types))

    if isinstance(quarters, str) and ' - ' in quarters:
        left, _, right = quarters.partition(' - ')
        if len(left.split()) != len(left.split()):
            raise ParsingError('Quarters on each side of dash must '
                               'have exact same number of components:'
                               '\n    - eg: \'F 13 - F 14\' - not \'F - F 14\').')
    return _parse_input(user_input=quarters,
                        mapping_func=parse_quarter,
                        values_to_slice=academic_time_units.quarter_with_year.keys())


def parse_years(years: Union[str, Iterable[str]]) -> List[int]:
    """Parse str(s) and or ints corresponding to (ordinal) years (eg 13, 2013) to list of integers.

    Notes
        Allows dashed and delimited strings.
        TODO deal with login data_samples differences in record, if necessary.
    Examples
        parse_year_range('2013-2014') - [2013, 2014]
    Args
        years (str or int or List) years
    Returns
        Parsed years as a list of integers if valid.
    Raises
        IndexError if
            Given year is not present in the quarter log (all quarters on record).
    """
    def parse_year(year: str or int) -> int:
        try:
            return academic_time_units.year.map_to_token(str(year))
        except Exception:
            raise ParsingError('\'{}\' is invalid: both years in the range must be in '
                               'current century with either two or four digits'.format(year))
    return _parse_input(user_input=years,
                        mapping_func=parse_year,
                        values_to_slice=academic_time_units.year.map_to_token)


def parse_datetimes(datetimes: Iterable[Union[str, pd.datetime]], as_datetime: bool=False) \
        -> Union[Tuple[str, str], Tuple[pd.datetime, pd.datetime], List[Union[str, pd.datetime]]]:
    """Parse given start/end dates.

    Parameters
    ----------
    datetimes : str
        FINISH here...
        Inclusive start date of form 'YY-MM-DD HH:MM:SS' where H:M:S is optional.
    as_datetime: bool=False
        Parse given dates to datetime objects if true, otherwise parsed to strings.

    Returns
    -------
    tuple of str
        Start/end dates of format '%Y-%m-%d %H:%M:%S' with H:M:S zero by default.
    tuple of datetime objects
        Start/end datetimes of format '%Y-%m-%d %H:%M:%S' with H:M:S zero by default.

    Raises
    ------
    ParsingError if incorrect dashed-string format or individual times
        cannot be parsed to a 24 hour 'HH:MM' format.
    ParsingError if value 'start_date' >= 'end_time', as no range can be generated.

    Notes
    -----
    Note that unlike the individual parser function, `parse_datetime`, time is allowed here.

    Examples
    --------
    >>> parse_datetimes(['2011/03/20'])
    ['2011-03-20 00:00:00']
    >>> parse_datetimes('2011-12-25 00:00  -  2012/12/25 00:00:01')
    ('2011-12-25 00:00:00', '2012-12-25 00:00:01')
    >>> parse_datetimes('1999/01/01 0:0:0 - 2000/11/11 23:00')
    ('1999-01-01 00:00:00', '2000-11-11 23:00:00')
    >>> parse_datetimes('2010/10/10 01:01:01 - 2010/10/11 10:10:10')
    ('2010-10-10 01:01:01', '2010-10-11 10:10:10')
    >>> parse_datetimes('2015/08/01 - 2015/08/02 3:00:42')
    ('2015-08-01 00:00:00', '2015-08-02 03:00:42')
    """
    def parse_datetime(dt: str or pd.datetime) -> str or pd.datetime:
        components = str(dt).partition(' ')  # converts datetime if necessary
        date, time = components[0].strip(' '), components[-1].strip(' ')
        if date:
            return cast_to_dt(date + ' ' + time) if time else cast_to_dt(date)
        raise ParsingError('\'{}\' is invalid - datetime must contain the following two items:'
                           '\n   - a valid date of the form \'YY-MM-DD\''
                           '\n   - an optional time of the format \'HH[:MM:SS am/pm]\''.format(dt))

    # after `_parse_input()`, map any values to datetime as necessary, and ensure dashed ordering
    parsed_values = _parse_input(user_input=datetimes, mapping_func=parse_datetime)
    if not isinstance(datetimes, str) or ' - ' not in datetimes or len(parsed_values) != 2:
        return parsed_values if as_datetime else list(map(str, parsed_values))
    elif parsed_values[0] < parsed_values[1]:  # two parsed values, return as a pair
        return tuple(parsed_values) if as_datetime else tuple(map(str, parsed_values))
    else:
        raise ParsingError('\'{}\' is invalid - dashed string must be of the form '
                           '\'LHS - RHS\' where LHS < RHS.'.format(datetimes))


def parse_time_of_day(time: Union[str, pd.datetime]) -> str:
    r"""Parse string representing time of day to a format 'HH:MM:SS'.

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
    def build_datetime():
        time_string = str(time).lower().replace(' ', '')
        is_twelve_hour_time = time_string.endswith('am') or time_string.endswith('pm')
        inferred_format = '%I' if is_twelve_hour_time else '%H'
        if ':' in time_string:
            inferred_format += ':%M' if time_string.count(':') == 1 else ':%M:%S'
        if is_twelve_hour_time:
            inferred_format += '%p'
        return datetime.datetime.strptime(time_string, inferred_format)

    try:
        dt = time if isinstance(time, pd.datetime) else build_datetime()
        return ':'.join([('0' if 0 <= t <= 9 else '') + str(t)
                         for t in (dt.hour, dt.minute, dt.second)])
    except ValueError:
        raise ParsingError('\'{}\' is invalid - time must be of format '
                           '\'HH[:MM:SS] [am/pm]\'.'.format(time.strip())) from None


def parse_full_course_name(course_name: str,
                           as_tuple: bool=False) -> Union[str, Tuple[str, str, str]]:
    """Partially parses entire course name (course, and optional subject/section).
    Notes:
        FOR EACH SUBJECT IN LIST OR COMMA SEPARATED INPUT (dashed input is not allowed!):
        - either (subject) OR (subject and number) OR (subject and number and section)
        - normalizes all spaces, splits input into subject, number (where first
          digit appears) and section (component trailing course number by a space).
        - then remove anomalies (present in unclean data)
             - period at end of course subject
             - F at beginning of course number, periods at the end of number
             - any number of 0's padding the front of non zero digits (eg: 01 -> 1),
               this applies to both course number and section
             - Capital O's in section name
        - maps course subject (eg: abbreviation) to a recognized subject
        - upper case the course's number and section
        - checks membership in a set of all possible course combinations,
          as taken from the (most recently updated) tutor log.
          If present in the records, return (3 element tuple if as tuple else string)
          If NOT present, check if it was due to incorrect number or section, and report
          to user.

    Examples
    --------
    >>> parse_full_course_name('computer science ', as_tuple=True)
    ('Computer Science', '', '')
    >>> parse_full_course_name('MATH 1A 01W', as_tuple=True)
    ('Mathematics', '1A', '1W')
    >>> parse_full_course_name('Mathematics   1d  02 ', as_tuple=True)
    ('Mathematics', '1D', '2')
    >>> parse_full_course_name('phys 4A 01', as_tuple=True)
    ('Physics', '4A', '1')
    >>> parse_full_course_name('MATH F022. 02', as_tuple=True)
    ('Mathematics', '22', '2')
    >>> parse_full_course_name('chem. 1A. 5', as_tuple=True)
    ('Chemistry', '1A', '5')
    """
    course_name_ = strings.normalize_spaces(course_name).lower()
    first_digit_position = next((k for k, char in enumerate(course_name_) if char.isdigit()), -1)
    if first_digit_position == -1:  # assume subject if input has no digits
        subject, number, section = course_name.strip(' '), '', ''
    else:  # otherwise, segment into three components (still works if section not present!)
        subject = course_name_[0:first_digit_position].strip(' ')
        number, _, section = course_name_[first_digit_position:].upper().partition(' ')

    # an 'f' padding number will be at the end of subject string since input sliced at 1st digit
    subject = re.sub('(\. f|\.| f)?$', '', subject)  # remove an end occurrence of '.', ' f', '. f'
    number = re.sub('^0{,2}|(\.)?$', '', number)     # remove up to 3 leading 0s and 1 trailing dot
    section = re.sub('^0|o', '', section)            # strip single starting occurrence of o or 0
    try:
        subject = course_subject_types.all.map_to_token(subject)
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
    raise ParsingError('\'{}\' cannot be recognized - course name requires a recognizable subject, '
                       'followed by either an existing course\'s number OR its number and section.'
                       .format(course_name_))


def parse_courses(course_names: Iterable[str],
                  as_tuple: bool=False) -> List[Union[str, Tuple[str, str, str]]]:
    """Parse given courses (dashed input not allowed!).Union[str, Tuple[str, str, str]]

    See Also:
        `parse_full_course_name` for details on how each individual course is parsed.

    Examples
    --------
    >>> parse_courses('chem. 1A. 5, math F0022. 2, math')
    ['Chemistry 1A 5', 'Mathematics 22 2', 'Mathematics']
    >>> parse_courses('cs 1a,phys 4c,mat 2a, Comp Sci 1B')
    ['Computer Science 1A', 'Physics 4C', 'Mathematics 2A', 'Computer Science 1B']
    """
    if ' - ' in course_names:
        raise ParsingError('\'{}\' is invalid - course input cannot be dashed.')
    course_parser = functools.partial(parse_full_course_name, as_tuple=as_tuple)
    return _parse_input(user_input=course_names, mapping_func=course_parser)


def parse_time_unit_label(time_unit_label: str) -> str:
    """Return given interval option as a normalized time unit name (eg: year, quarter, week_num).

    Examples
    --------
    >>> parse_time_unit_label('hours')
    'hour'
    >>> parse_time_unit_label('hrs')
    'hour'
    >>> parse_time_unit_label('yrs')
    'year'
    >>> parse_time_unit_label('yearly')
    'year'
    >>> parse_time_unit_label(' qtr ')
    'quarter'
    >>> parse_time_unit_label('DAYS')
    'day_in_week'
    >>> parse_time_unit_label('wk in qtr')
    'week_in_quarter'
    """
    time_unit_ = time_unit_label.strip(' ').lower()
    try:
        return label_types.time_unit.map_to_token(time_unit_)
    except:
        supported_label_types = strings.pretty_format_list(label_types.time_unit.keys(), conj='or')
        raise ParsingError('\'{}\' is invalid - time unit label must correspond to one of {}.'
                           .format(time_unit_, supported_label_types)) from None


def parse_metric_type(metric_label: str) -> str:
    """Return given interval option as a normalized time unit (demand, wait_time)."""
    metric_label_ = metric_label.strip(' ').lower()
    try:
        return label_types.metric.map_to_token(metric_label_)
    except:
        supported_label_types = strings.pretty_format_list(label_types.metric.keys(), conj='or')
        raise ParsingError('\'{}\' is invalid - metric type must correspond to one of {}.'
                           .format(metric_label_, supported_label_types)) from None


if __name__ == '__main__':
    from stem_analytics.warehouse import get_tutor_request_data
    df = get_tutor_request_data(as_clean=False)
    print(df['Description'])
    print(df['Description'].apply(parse_full_course_name))

