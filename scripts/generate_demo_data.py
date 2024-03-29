"""Script to generate demand and wait-time statistics based on historical data.

Three general categories of generated json files:
* Single file generated containing daily averages over a quarter range
* One file generated per week in qtr containing daily averages over a week range
* One file generated per day in qtr containing hourly averages over a day (24hr) range

Notes
-----
Assumes file format: 2013-09-25 11:05:32,F 13,1,W,chem 30a 1,62,782484
Assumes date format: ([] denotes optional): 'YY-MM-DD [HH[:MM:SS am/pm]]'

File names are generated with the following components:
* quarter name: format 'xxDD' (eg, sp15)
* domain: 'T_I' where T is time range type (i.e., qtr) and I is time
  range instance (eg, sp15)
* interval: one of 'hour', 'day', or 'week'
* metric: one of 'waittime' or 'demand'
"""
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from stem_center_analytics import warehouse, PROJECT_DIR
from stem_center_analytics.core import input_validation
from stem_center_analytics.utils import os_lib, io_lib


def _sort_index_by_list(df: pd.DataFrame, rank_order: Iterable[object]) -> pd.DataFrame:
    """Return given DF sorted by given columns according to `rank_order`."""
    # todo: add ability to sort numpy array by list as well, if necessary.
    df_ = df
    sort_order = list(rank_order)
    rank_mappings = dict(zip(sort_order, range(0, len(sort_order))))  # build list index/value dict
    rank_col = df_.index.map(lambda k: rank_mappings[k])              # lookup_by_alias index values for col
    df_.insert(loc=len(df_.columns), column='rank', value=rank_col)   # append rank_col to end of df
    df_.sort_values(by=['rank'], axis=0, inplace=True)    # sort the index by rank
    df_.drop(labels=['rank'], axis=1, inplace=True)       # now drop the rank column
    return df_


def _aggregate_sc_data(df: pd.DataFrame,
                       aggregate_option: Mapping[str, callable],
                       interval_type: str) -> pd.DataFrame:
    """Aggregate given DF on given column according to given time interval.

    Parameters
    ----------
    if index in aggregate option, then groupby index instead...
    aggregate_option : Mapping[str, callable]
        numpy function to apply element-wise to given column name,
        such as {'count', 'median', 'mean'}
    interval_type : {hour, day_in_week, week_in_quarter, month, quarter, year}
        interval to compute on

    Notes
    -----
    * Aggregation is all done on 'wait_time' column, except for counts, in
      which no specific column is needed. Parameters are not parsed.
    """
    interval_type_ = input_validation.TIME_UNIT_NAMES.lookup_by_alias(interval_type)
    if interval_type_ in ('day_in_week', 'week_in_quarter', 'quarter'):
        col_data = [df.__getattr__(interval_type)]
    elif interval_type in ('hour', 'month', 'year'):
        col_data = [df.__getattribute__('index').__getattribute__(interval_type)]
    else:
        raise ValueError('Internal error: illegal interval type.')

    if len(aggregate_option) != 1:
        raise ValueError('Internal error: only one column can be aggregated on at a time.')
    aggregated_df = df.groupby(by=col_data).aggregate(aggregate_option)

    if interval_type == 'quarter':  # ordering undefined for quarter strings, so sort output...
        return _sort_index_by_list(
            df=aggregated_df,
            rank_order=input_validation.TIME_UNIT_VALUES.QUARTERS_WITH_YEARS.keys()
        )
    return aggregated_df


def compute_metric_on_intervals(df: pd.DataFrame, interval_type: str,
                                metric_type: str) -> pd.DataFrame:
    """Calculate demand and wait-time metrics given historical tutor requests.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe containing data to compute on
    interval_type : {hour, day_in_week, week_in_quarter, month, quarter, year}
        interval to compute metric on
    metric_type: {'demand', 'wait_time'}
        *demand: total number of requests are counted over intervals for given range.
        *wait_time: wait_times are averaged over intervals for given range.

    Notes
    -----
    * Unlike aggregate sc_data, parameters are parsed.
    """
    metric_type_ = input_validation.OTHER_UNIT_NAMES.lookup_by_alias(metric_type)
    interval_type_ = input_validation.TIME_UNIT_NAMES.lookup_by_alias(interval_type)
    print(df)
    if metric_type_ == 'demand':
        aggregate_mappings = {'quarter': np.count_nonzero}  # arbitrary column name for counting
    elif metric_type_ == 'wait_time':
        aggregate_mappings = {'wait_time': np.mean}  # explicitly give column to average on
    else:
        raise ValueError('Internal Error')
    return _aggregate_sc_data(df, aggregate_option=aggregate_mappings,
                              interval_type=interval_type_)


def generate_demo_quarter_data(requests_in_quarter: pd.DataFrame, output_dir: str) -> None:
    """Generate demand and wait-time metrics for various ranges in quarter."""
    def generate_json_demo_data(data_in_range: pd.DataFrame, range_: str, interval_: str) -> None:
        # write json file with demand and wait_time for given range and interval
        demand = compute_metric_on_intervals(data_in_range, interval_, 'demand').to_dict()
        wait_time = compute_metric_on_intervals(data_in_range, interval_, 'wait_time').to_dict()

        # convert all entries to strings, and round integers by two
        demand_key, wait_time_key = list(demand.keys())[0], list(wait_time.keys())[0]
        data = {
            list(demand.keys())[0]:
                {str(key): str(demand[demand_key][key])
                 for key in demand[demand_key]},
            list(wait_time.keys())[0]:
                {str(key): str(round(wait_time[wait_time_key][key], 2))
                 for key in wait_time[wait_time_key]}
        }
        interval_ = 'week' if interval_ == 'week_in_quarter' else interval_
        file_name = f'time_range={range_}&interval={interval_}.json'
        io_lib.create_json_file(file_path=os_lib.join_path(output_dir, file_name), contents=data)

    if len(set(requests_in_quarter['quarter'])) != 1:
        raise ValueError('Given data must contain only one quarter type (eg: \'Fall 2015\').')

    # single file (since single quarter) generated containing daily stats over a quarter
    quarter_term, quarter_year = requests_in_quarter['quarter'].iloc[0].split()
    generate_json_demo_data(data_in_range=requests_in_quarter,
                            range_=f'quarter+{quarter_term}_{quarter_year}', interval_='week_in_quarter')

    # for each week in quarter, generate a file containing daily stats over a week range
    all_weeks_in_qtr = requests_in_quarter['week_in_quarter'].unique()
    for week_num in all_weeks_in_qtr:
        single_week_data = requests_in_quarter[requests_in_quarter['week_in_quarter'] == week_num]
        generate_json_demo_data(data_in_range=single_week_data,
                                range_=f'week+{week_num}', interval_='day')

    # for each day in quarter, generate a file containing hourly stats over a day (24 hour) range
    all_recorded_datetimes = pd.Series(data=requests_in_quarter.index)
    dates_in_qtr = all_recorded_datetimes.apply(func=lambda dt: str(dt).split()[0]).unique()
    for date in dates_in_qtr:
        generate_json_demo_data(data_in_range=requests_in_quarter[date],
                                range_=f'day+{date}', interval_='hour')


def main():
    df = warehouse.get_tutor_request_data()
    root_output_dir = os_lib.normalize_path(
        path=os_lib.join_path(PROJECT_DIR, 'external_datasets', 'pre_generated_data')
    )
    os_lib.remove_directory(root_output_dir, ignore_errors=True)  # clear the dir if exists
    os_lib.create_directory(root_output_dir)
    for quarter_name in df['quarter'].unique():
        output_sub_dir = os_lib.join_path(root_output_dir, quarter_name.replace(' ', '_'))
        os_lib.create_directory(output_sub_dir)
        requests_in_quarter = df[df['quarter'] == quarter_name]
        generate_demo_quarter_data(requests_in_quarter, output_sub_dir)


if __name__ == '__main__':
    main()
