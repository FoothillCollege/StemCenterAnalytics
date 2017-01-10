"""Establish imports and generate a cleaned df from stem center database.

Notes
-----
* strong assumptions that the data used outside the scripts module -
  aka anywhere in this source directory - fulfills the assumptions
  detailed in the warehouse docs
* throughout the project, code standards exist, in addition to the ones
  in PEP-8, notably...
  * Naming:
      * use snake case and avoid overly descriptive names, as well as
        deliberately truncated names as much as possible (except for
        exceptionally common abbreviations like 'dir', or in the case
        of this project, df for Pandas DataFrame, or db for database).
        Also, hungarian notation should also be avoided.
        eg: favor 'is_valid_url' over 'is_valid_website_address' or
        'is_valid_
      * For methods in a class, do not overly describe, the context of the class
        name that contains it should be enough, eg: prefer 'canvas.draw' over
        'canvas.draw_on_canvas'. another example (taken from this project):
        'ParserDict.parse' over 'ParserDict.map_to_token' or
        'ParserDict.parse_to_dict'. In the case of module names, this holds to,
        so prefer 'os_lib.get_basename' over 'os_lib.get_basename_of_path'.
        Of course this means no import * as well, favor importing module names
        for this reason as much as possible.
      * all function and method names MUST start with a verb, with verb choices
        followed everywhere else a similar action is performed. Common idioms
        for the start of function names in this project include:
          * 'ensure': validity checks, eg: 'ensure_valid_sqlite_file',
            where nothing is returned if no errors are found, and errors
            thrown if invalid file. Specific validity is to be defined within
            the function's docstring
          * 'is': boolean checks, eg: 'is_existent_file'
          * 'get': retrieval functions.
            Remember, this isn't Java, a name starting with 'get' does NOT
            denote a field accessor in anyway. Rather a method or function
            name starting with 'get' signifies either retrieval of data
            (eg: 'get_tutor_request_data', returning dataframe containing
            tutor request data, as extracted from database), or modifying a
            copy of given parameter (eg: 'get_basename', returning given path's
            basename)
  * Documentation:
      * Present tense is be favored as much as possible, such as using
        'return' over 'returns' or 'returned'. We aim to dictate the action
        to be taken, not describing the past or describing an event
      * NumPYDoc conventions are to be followed at all times
      * ALL docstrings contain at a minimum a one line summary,
        including packages, modules, private functions and magic method
        implementations
      * Not all NumPYDoc sections are necessary, especially when parameters and
        return values are extremely obvious with no special cases -- other than
        perhaps the appropriate errors mentioned in `Raises` section.
        See `os_lib` for examples
      * Workhorse, client facing functions such as those in
        `stem_center_analytics.interface` should be documented as thoroughly as
        possible, with all NumPyDoc sections filled out as much as helpfully
        possible
  * ???

# todo: remove above examples with 'see also's' for referencing examples
# todo: breakup the contents of this module's docstring to appropriate read-only
  files such as a 'style-guidelines.pdf', with appropriate examples
"""
from sys import modules

import pandas

import stem_center_analytics.utils.os_lib

__author__ = 'Jeff'
__version__ = '1.3.0'
__all__ = ('utils', 'warehouse', 'core', 'interface')

SOURCE_DIR = stem_center_analytics.utils.os_lib.get_path_of_python_source(modules[__name__])
PROJECT_DIR = stem_center_analytics.utils.os_lib.get_parent_dir(SOURCE_DIR)

# let user know if any core dependencies or stem_center_analytics packages are missing
# this allows us to catch import errors early on, rather than upon use of specific modules
stem_center_analytics.utils.os_lib.ensure_successful_imports(
    names=('pandas', 'flask', 'numpy', 'cython'))

# fixme: figure out why below fails on heroku deploy but not here
# stem_center_analytics.utils.paths.ensure_successful_imports(names=__all__)  # catch import errors early on

# import public class APIs
from stem_center_analytics.interface import TutorLog, LoginData


def config_pandas_display_size(max_rows: int = 50, max_cols: int = 20,
                               max_width: int = 750) -> None:
    """Configure Pandas display settings, to allow pretty-printing for console output."""
    pandas.set_option('display.max_rows', max_rows)
    pandas.set_option('display.max_columns', max_cols)
    pandas.set_option('display.width', max_width)


config_pandas_display_size()
