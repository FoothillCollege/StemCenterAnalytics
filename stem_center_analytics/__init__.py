"""Establish imports and generate a cleaned df from stem center database.

Let user know if any core dependencies or stem_center_analytics packages are missing,
the import checking also allows us to catch import errors early on, rather
than upon use of specific modules

Notes
-----
* strong assumptions that the data used outside the scripts module -
  aka anywhere in this source directory - fulfills the assumptions
  detailed in the warehouse docs
"""
__author__ = 'Jeff'
__version__ = '1.3.0'
__all__ = ('stem_center_analytics.utils', 'stem_center_analytics.warehouse',
           'stem_center_analytics.core', 'stem_center_analytics.interface')

SOURCE_DIR = ''
PROJECT_DIR = ''
def _run_initial_setup(debug_mode: bool=False) -> None:
    """Ensure successful imports and validate data sources

    All imports are done inside this functions, as to avoid polluting package
    namespace

    Notes
    -----
    * Perform the following checks and setups:
        * Check that the core dependencies (pandas, flask, numpy, cython)
          are present
        * Set the constants `SOURCE_DIR`, `PROJECT_DIR`
        * Establish wide dataframe display settings
        * If `debug_mode`=False, ensure data sources are existent and valid;
          debug mode is present to allow replacing/restoring/etc given data sources
    """
    from sys import modules

    from stem_center_analytics.utils import os_lib
    # check hard dependencies
    os_lib.ensure_successful_imports(path=__file__, names=('pandas', 'flask', 'numpy', 'cython'))

    global SOURCE_DIR, PROJECT_DIR
    SOURCE_DIR = os_lib.get_path_of_python_source(modules[__name__])
    PROJECT_DIR = os_lib.get_parent_dir(SOURCE_DIR)

    if not debug_mode:
        # ensure files and database connections are good to go
        from stem_center_analytics.warehouse import DATA_FILE_PATHS, connect_to_stem_center_db
        from stem_center_analytics.utils.os_lib import ensure_file_exists
        ensure_file_exists(DATA_FILE_PATHS.QUARTER_DATES)
        ensure_file_exists(DATA_FILE_PATHS.COURSE_RECORDS)
        with connect_to_stem_center_db():
            pass

    os_lib.ensure_successful_imports(path=__file__, names=__all__)
    from pandas import set_option
    # establish wide dataframe display
    set_option('display.max_rows', 50)
    set_option('display.max_columns', 20)
    set_option('display.width', 750)


# run initial setup, and import public class APIs
_run_initial_setup(debug_mode=True)
from stem_center_analytics.interface import TutorLog, LoginData


# todo: separate the installs/packages needed for server/pipeline/web-service vs using as library
# after the above is done, then reflect the above changes in `_run_initial_setup`

# todo: try benchmarking with shorter, abbreviated names, both in writing and reading
# todo: switch to docker file bootup, so that there is no longer a limit on slug size
