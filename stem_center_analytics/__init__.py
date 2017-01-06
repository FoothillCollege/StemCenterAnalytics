"""Establish imports and generate a cleaned df from stem center database.

Notes
-----
* strong assumptions that the data used outside the scripts module -
  aka anywhere in this source directory - fulfills the assumptions
  detailed in the warehouse docs
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
stem_center_analytics.utils.os_lib.ensure_successful_imports(names=('pandas', 'flask', 'numpy', 'cython'))

# fixme: figure out why below fails on heroku deploy but not here
#stem_center_analytics.utils.paths.ensure_successful_imports(names=__all__)  # catch import errors early on

# import public class APIs
from stem_center_analytics.interface import TutorLog, LoginData


def config_pandas_display_size(max_rows: int=50, max_cols: int=20, max_width: int=750) -> None:
    """Configure Pandas display settings, to allow pretty-printing for console output."""
    pandas.set_option('display.max_rows', max_rows)
    pandas.set_option('display.max_columns', max_cols)
    pandas.set_option('display.width', max_width)

config_pandas_display_size()
