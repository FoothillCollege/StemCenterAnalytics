"""Contains aggregate calculations functions."""
from typing import Iterable

import numpy as np
import pandas as pd

from stem_analytics.core import input_validation, df_tools

# todo: generalize statistical calculations present in scripts, and move here


def generate_skeleton(sc_data: pd.DataFrame) -> pd.DataFrame:
    pass


def avg_by_hour(sc_data: pd.DataFrame) -> pd.DataFrame:
    year_hour_means = sc_data.resample(rule='24H', base=8).mean()
    return year_hour_means


def avg_by_week(sc_data: pd.DataFrame) -> pd.DataFrame:
    pass


def avg_by_year(sc_data: pd.DataFrame) -> pd.DataFrame:
    pass


def _get_test_df(date_range: str='') -> pd.DataFrame:
    # use the below to allow configuration's in viewer
    # configure displays for console output
    if not date_range:
        return ''
    start, end = input_validation.parse_datetimes(date_range)
    return df_tools.filter_by_date(sc_data=pd.DataFrame, start_date=start, end_date=end)


def bin_df(sc_data: pd.DataFrame) -> pd.DataFrame:
    # half open bins - i.e.: bin all values in [start_range, end_range)
    print(sc_data.index.hour)
    '''
    pd.cut(x=sc_data.index.hour,
           bins=,
           right=False,
           labels=None,
           retbins=,
           precision=,
           include_lowest=False)
    '''


def _average_col_by_interval(sc_data: pd.DataFrame, col_name: str,
                             interval_option: str) -> pd.DataFrame:
    """Return given DF aggregated by given column according to interval option (year/day/etc.)."""
    return sc_data.groupby([interval_option]).aggregate({col_name: np.mean})


def _sort_index_by_list(df: pd.DataFrame, rank_order: Iterable[object]) -> pd.DataFrame:
    """Return given DF sorted by given columns according to `rank_order`."""
    df_ = df
    sort_order = list(rank_order)
    rank_mappings = dict(zip(sort_order, range(0, len(sort_order))))  # build list index/value dict
    rank_col = df_.index.map(lambda k: rank_mappings[k])              # lookup index values for col
    df_.insert(loc=len(df_.columns), column='rank', value=rank_col)   # append rank_col to end of df
    df_.sort_values(by=['rank'], axis=0, inplace=True)                # sort the index by rank
    df_.drop(labels=['rank'], axis=1, inplace=True)                   # now drop the rank column
    return df_

# todo: replace below with input validation
SORT_ORDER = {
    'hour': tuple(range(0, 24)),
    'day': tuple(range(0, 24)),
    'year': tuple(range(2000, 3000)),
    'quarter': tuple([q + ' ' + str(y) for y in range(2000, 3000) for q in ('W', 'S', 'U', 'F')])
}


def get_avg_wait_time(sc_data: pd.DataFrame, interval_option: str) -> pd.DataFrame:
    """Return given DF aggregated by given column according to interval option (year/day/etc.)."""
    aggregated_df = sc_data.groupby([sc_data.index.hour]).aggregate({'wait_time': np.mean})
    interval_option_ = input_validation.parse_time_unit_label(interval_option)  # unsorted for now...
    return _sort_index_by_list(df=aggregated_df, rank_order=SORT_ORDER[interval_option_])


# --------------------------------------------------------------------------------------------------
# todo: take action on below task list for module core/stats.py
# (1) determine data-structure of generated averages...as a DataFrame/np.array/etc?
# (2) determine interval amount...infer from given data or instead allow user to choose?
# (3) minimize number of functions for given data as to allow different types and wait-time/demand_data
# (4) make it pretty -- improve-algorithm/clean-up/document-it/benchmark-it/test-corner-cases/etc
# (5) optimize performance: reduce memory/time via Cython and/or similar tools (ie numba)
# (6) perform statistical analysis (incorporate resolution parameters such as avg-width/median%/etc)
# (7) any leftover tasks such as: implement moving average so new values can be accumulated, etc.
# --------------------------------------------------------------------------------------------------


'''
# previously:
def main():
    df = _get_test_df()
    print(get_avg_wait_time_df(df, 'day'))
    quarters = input_validation.parse_quarters('F 15')
    df = filtering.filter_by_quarter(df, quarters)
    print(df)

    df = get_avg_wait_time_df(df, 'day')
    print(df)
'''
