"""Establish imports and generate a cleaned df from stem center database.

Package Dependencies: Pandas, SqlAlchemy, MatPlotLib, sk-learn

--------------------------- Package Assumptions -------------------------------

Below assumptions are made for any stem center related DataFrame...
Such a DataFrame will be considered to be sc_data... as explained below...

All methods that operate on Pandas DataFrames assume that the df is a time
series with each row corresponding to a specific student entry/request.

Additionally, any given df (whether as an attribute or function argument) is
assumed to contain columns corresponding to each of the following:
   * logged time (datetime index): exact time of each student entry/request
   - day (str): day of week in which entry was made (Sunday, ..., Saturday)
   - course name (str): subject area and course number (i.e.: math 2A, cs 10)
   - course section (str): section-number/online-indicator (i.e.: 5W, 1Y)
   - wait time (int): time elapsed between tutor request and tutor visit

In the case of MUNGED (cleaned/formatted) DataFrames, the above assumptions are
expanded to include the following additional column contents:
   - week in quarter (int): week in the quarter the entry was made (1, ..., 12)
   - quarter of request (str): (i.e.: F 14, W 15, S 15, SU 15)

The above assumptions being stated, it's important to note that there may be
instances of DataFrames with reduced columns, (explicitly stated), for speed up
purposes.

*index

* ALL DATES FORMATTED IN THE FORM: '%Y-%m-%d %H:%M:%S'

NOTE: NEED TO ADD ALIASES AT SOME POINT...

Examples:
    from stem_analytics.interface import TutorLog
    df = TutorLog()
    df.filter_by_day('m w t')
    print(df) # displays filtered df
-------------------------------------------------------------------------------
Notes:
    strong assumptions that the (clean) data resembles the following format:
        time_of_request,quarter,week_in_quarter,day,course,wait_time,anon_stud_id
        2013-09-25 11:05:32,F 13,1,W,chem 30a 1,62,782484
    any discrepancies in format are and will be dealt with in the data cleaning/updating phase.
"""
from sys import modules

import pandas as pd

import stem_center_analytics.utils.os_lib

__author__ = 'Jeff'
__version__ = '1.3.0'
__all__ = ('utils', 'warehouse', 'core', 'interface')

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 500)


SOURCE_DIR = stem_center_analytics.utils.os_lib.get_path_of_python_source(modules[__name__])
PROJECT_DIR = stem_center_analytics.utils.os_lib.get_parent_dir(SOURCE_DIR)

INTERNAL_DATASETS_DIR = stem_center_analytics.utils.os_lib.join_path(SOURCE_DIR, 'warehouse')
EXTERNAL_DATASETS_DIR = stem_center_analytics.utils.os_lib.join_path(PROJECT_DIR, 'external_datasets')


# let user know if any CORE_SUBJECTS dependencies or stem_center_analytics packages are missing
# this allows us to catch import errors early on, rather than upon use of specific modules.
stem_center_analytics.utils.os_lib.ensure_successful_imports(names=('pandas', 'flask', 'numpy', 'cython'))

# fixme: figure out why below fails on heroku deploy but not here
'''
stem_center_analytics.utils.paths.ensure_successful_imports(names=__all__)  # catch import errors early on
'''


# import public Class APIs
from stem_center_analytics.interface import TutorLog, LoginData


# todo: build sphinx-api-documentation
# todo: add automated backups to stem_center_db
# todo: ensure all documentation is up to date and cleaned up
# todo: finish conversion of all documentation to numpy format
# todo: migrate doctests to a testing package where appropriate - particularly regression tests
def config_pandas_display_size(max_rows: int=50, max_cols: int=20, max_width: int=500) -> None:
    """Configure Pandas display settings, to allow pretty-printing for console output."""
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.max_columns', max_cols)
    pd.set_option('display.width', max_width)
