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
SCRIPT_DIR = stem_center_analytics.utils.os_lib.join_path(PROJECT_DIR, 'scripts')
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


def config_pandas_display_size(max_rows: int=50, max_cols: int=20, max_width: int=500) -> None:
    """Configure Pandas display settings, to allow pretty-printing for console output."""
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.max_columns', max_cols)
    pd.set_option('display.width', max_width)


# group 1: build the deployment pipeline
# todo: separate private settings (email passwords, etc), into public repo excluded files
# todo: automate deployment process via shell script, including forced add for dev_settings.py
# todo: test out running automated scripts via cron job on heroku
# todo: start working on the email data extraction script
# todo: rewrite clean data script to clean the raw ALL_DATA file Eric sent, instead of semi-clean
# todo: once above is complete, integrate the scripts in an automated pipeline, on a single email
# todo: add login data support to pipeline (and rest of the project for that matter)
# todo: column detection heuristics (making it source as source independent as possible)
# todo: configure hourly updates, and officially release the pipeline in full to heroku
# todo: add fail-safe mechanisms such as rollbacks and increased atomicity of db/file transactions
# todo: add backup dbs as necessary, have an uncleaned db (rather than file)
# todo: figure out steps to take and notification process in case of massive errors/data-loss/etc
# todo: add roll back and log function (possibly via context manager?) for use in the above process
# todo: ensure separation of student id's...(possibly have on the heroku repo only?)
# see bottom of file for more details on this process


# group 2: essential core tasks
# todo: generalize the averaging scripts to a semi-generic computational library in core.stats
# todo: make the data averaging dynamic per request, supporting multiple subjects, etc.
# todo: add support for heatmap requests (figure out how to send - possibly as 2D array?)
# todo: integrate the newly generic core.stats functions to the interface
# todo: finish-up and polish the interface, giving our first API 'release'
# todo: determine if math 235, econ, etc (which have separate center) should be removed/separated
# todo: once dynamic averaging support/requests are added, remove the pre-generated data!


# group 3: testing, documentation, reporting, tweaking
# todo: build sphinx-api-documentation
# todo: ensure all documentation is up to date and cleaned up (like init docstrings, etc)
# todo: finish conversion of all documentation to numpy format
# todo: migrate doctests to a testing package where appropriate - particularly regression tests
# todo: add logging to the areas in which heroku doesn't automatically support
# todo: add examples to the highest/most-critical levels of the codebase
# todo: add sphinx docs to gh-pages
# todo: test high volume loads, updates, simultaneous requests, etc
# todo: test via duplicating existing data (will it work fast at 500k+ tutor requests in the db?)
# todo: optimize response times

# group 4: extras
# todo: setup the various tools/backend-app on eric's computer
# todo: setup the dashboard on the stem center TV in front room
# todo: improve the error handling of get/put requests to be more specific and include default data
# todo: add gui that wraps the backend api
# todo: add CLI support to scripts, where necessary
# todo: create environment that highcharts.js can easily be embedded, to test/explore visualizations
# todo: add GUI that wraps api doc
# todo: enable download via pypy or similar
# todo: make various technical/non-technical tutorials/read-me's for various use cases
# todo: user testing, code readability/use/local download testing
# todo: increase security/stability of flask web service, possibly add authentication?
# todo: have a hacker (like samuel) try and mess with things/investigate as feedback...


# group 5: moving on from the project
# todo: gather a small community of people to continue development/use (+delegate group 4 todos?)
# todo: play around with the app, with different visualizations, learning algos, stats, etc!!!
# todo: encourage use of project for data analysis/discovery/research (eg: by ken, bita, etc)
# todo: PARTY!! (SERIOUSLY -- YOU'LL BE EXHAUSTED BY NOW JEFF!!!)


# NOTE 1 -- the above aren't necessarily done in pure sequence, it's just a general guide.
# Also, the above serve as a general list, see specific files for more specific, up to date todos.
# Naturally, there will be things above not needed after all, and todos that were forgotten.


# NOTE 2 -- the overall automated pipeline process will be as follows:
# 1) the chron-job (or similar) runs the appropriate script(s) for the hour (if new email detected)
# 2) extract data from email as a csv
# 3) recognize the appropriate columns based on data types, and set column mappings accordingly
# 4) parse each column according to the type and location identified in the column map
# 5) if errors occurred, log time/traceback, and rollback to before the update
# 6) otherwise, write the cleaned csv to the database, rolling-back and logging if db error
# 7) once in, log the changes to the databases (as done in io_lib.write_df_to_database)
# 8) write the subject list to a file, rolling back and logging if error


# --------------------------------------------------------------------------------------------------
# TODO: ASK BITA for help on why git doesn't work when I version the current project/move it/etc
# ...aka, figure out why the code works
# I tried resetting git, invalidating caches/restarting, reinitializing, different locations, etc.,
# but nothing works (gives head error, etc).

# This needs to be figured out so I don't have the extra task of copying/pasting/etc
# the existing local copy EVERY time!
