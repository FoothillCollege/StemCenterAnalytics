"""Contains aggregate calculations functions."""
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from stem_center_analytics.core import input_validation

# todo: generalize statistical calculations present in scripts, and move here
# todo: replace below `SORT_ORDER` with input validation keys

SORT_ORDER = {
    'hour': tuple(range(0, 24)),
    'day': tuple(range(0, 24)),
    'year': tuple(range(2000, 3000)),
    'quarter': tuple([q + ' ' + str(y) for y in range(2000, 3000) for q in ('W', 'S', 'U', 'F')])
}


def get_avg_wait_time(sc_data: pd.DataFrame, interval_option: str) -> pd.DataFrame:
    """Return given DF aggregated by given column according to interval option (year/day/etc.)."""
    aggregated_df = sc_data.groupby([sc_data.index.hour]).aggregate({'wait_time': np.mean})
    interval_option_ = input_validation.parse_time_unit_name(interval_option)  # unsorted for now...
    return _sort_index_by_list(df=aggregated_df, rank_order=SORT_ORDER[interval_option_])


def avg_by_hour(sc_data: pd.DataFrame) -> pd.DataFrame:
    year_hour_means = sc_data.resample(rule='24H', base=8).mean()
    return year_hour_means


def avg_by_week(sc_data: pd.DataFrame) -> pd.DataFrame:
    pass


def avg_by_year(sc_data: pd.DataFrame) -> pd.DataFrame:
    pass


def _average_col_by_interval(sc_data: pd.DataFrame, col_name: str,
                             interval_option: str) -> pd.DataFrame:
    """Return given DF aggregated by given column according to interval option (year/day/etc.)."""
    return sc_data.groupby([interval_option]).aggregate({col_name: np.mean})


def _sort_index_by_list(df: pd.DataFrame, rank_order: Iterable[object]) -> pd.DataFrame:
    """Return given DF sorted by given columns according to `rank_order`."""
    # todo: add ability to sort numpy array by list as well, if necessary.
    df_ = df
    sort_order = list(rank_order)
    rank_mappings = dict(zip(sort_order, range(0, len(sort_order))))  # build list index/value dict
    rank_col = df_.index.map(lambda k: rank_mappings[k])              # lookup index values for col
    df_.insert(loc=len(df_.columns), column='rank', value=rank_col)   # append rank_col to end of df
    df_.sort_values(by=['rank'], axis=0, inplace=True)    # sort the index by rank
    df_.drop(labels=['rank'], axis=1, inplace=True)       # now drop the rank column
    return df_


def _aggregate_sc_data(sc_data: pd.DataFrame,
                       aggregate_option: Mapping[str, callable],
                       interval_type: str) -> pd.DataFrame:
    """Aggregate given DF on given column according to given time interval.

    Parameters
    ----------
    if index in aggregate option, then groupby index instead...
    aggregate_option : Mapping[str, callable]
        numpy function to apply element-wise, such as {'count', 'median', 'mean'}
    interval_type : {hour, day_in_week, week_in_quarter, month, quarter, year}
        interval to compute on

    Notes
    -----
        Aggregation is all done on 'wait_time' column, except for counts, in
        which no specific column is needed.
        Parameters are not parsed.
    """
    interval_type_ = input_validation.parse_time_unit_name(interval_type)
    if interval_type_ in ('day_in_week', 'week_in_quarter', 'quarter'):
        col_data = [sc_data.__getattr__(interval_type)]
    elif interval_type in ('hour', 'month', 'year'):
        col_data = [sc_data.__getattribute__('index').__getattribute__(interval_type)]
    else:
        raise ValueError('Internal error: illegal interval type.')

    if len(aggregate_option) != 1:
        raise ValueError('Internal error: only one column can be aggregated on at a time.')
    aggregated_df = sc_data.groupby(by=col_data).aggregate(aggregate_option)

    # fixme: alter wait_times to ensure nonzero, so wait_times can be used instead for counting...
    if 'quarter' in aggregate_option:
        aggregated_df.rename(columns={'quarter': 'num_requests'}, inplace=True)

    if interval_type == 'quarter':  # ordering undefined for quarter strings, so sort output...
        quarters = [q + ' ' + str(y) for y in range(2000, 3000) for q in ('W', 'S', 'U', 'F')]
        return _sort_index_by_list(df=aggregated_df, rank_order=quarters)
    return aggregated_df


def compute_metric_on_intervals(sc_data: pd.DataFrame, interval_type: str,
                                metric_type: str) -> pd.DataFrame:
    """Calculate demand and wait-time metrics given historical tutor requests.

    Parameters
    ----------
    sc_data : pd.DataFrame
        dataframe containing data to compute on
    interval_type : {hour, day_in_week, week_in_quarter, month, quarter, year}
        interval to compute metric on
    metric_type: {'demand', 'wait_time'}
        *demand: total number of requests are counted over intervals for given range.
        *wait_time: wait_times are averaged over intervals for given range.
    Notes
    -----
    Unlike aggregate sc_data, parameters are parsed.
    """
    metric_type_ = input_validation.parse_metric_name(metric_type)
    interval_type_ = input_validation.parse_time_unit_name(interval_type)
    # print(interval_type, '===>', interval_type_)
    if metric_type_ == 'demand':
        aggregate_mappings = {'quarter': np.count_nonzero}  # arbitrary column name for counting
    elif metric_type_ == 'wait_time':
        aggregate_mappings = {'wait_time': np.mean}  # explicitly give column to average on
    else:
        raise ValueError('Internal Error')
    return _aggregate_sc_data(sc_data, aggregate_option=aggregate_mappings,
                              interval_type=interval_type_)

# --------------------------------------------------------------------------------------------------
# todo: take action on below task list for module CORE_SUBJECTS/stats.py
# (1) determine data-structure of generated averages...as a DataFrame/np.array/etc?
# (2) determine interval amount...infer from given data or instead allow user to choose?
# (3) minimize number of functions, generalizing for different criteria & wait-time/demand data
# (4) make it pretty -- improve-algorithm/clean-up/document-it/benchmark-it/test-corner-cases/etc
# (5) optimize performance: reduce memory/time via Cython and/or similar tools (ie numba)
# (6) perform statistical analysis (incorporate resolution parameters such as avg-width/median%/etc)
# (7) any leftover tasks such as: implement moving average so new values can be accumulated, etc.
# --------------------------------------------------------------------------------------------------
