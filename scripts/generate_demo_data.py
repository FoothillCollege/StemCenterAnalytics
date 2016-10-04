"""Script to generate demand and wait-time statistics based on historical data.

Three general categories of generated json files:
- single file generated containing daily averages over a quarter range
- one file generated per week in qtr containing daily averages over a week range
- one file generated per day in qtr containing hourly averages over a day (24hr) range

Notes
-----
Assumes file format: 2013-09-25 11:05:32,F 13,1,W,chem 30a 1,62,782484
Assumes date format: ([] denotes optional): 'YY-MM-DD [HH[:MM:SS am/pm]]'

File names are generated with the following components:
- qtr_name: format 'xxDD' (eg, sp15)
- domain: 'T_I' where T is time range type (i.e., qtr) and I is time
  range instance (eg, sp15)
- interval: one of 'hour', 'day', or 'week'
- metric: one of 'waittime' or 'demand'
"""
import shutil
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from stem_analytics import EXTERNAL_DATASETS_DIR
from stem_analytics.utils import paths
from stem_analytics.core import input_validation
from stem_analytics.warehouse import get_tutor_request_data


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
    interval_type_ = input_validation.parse_time_unit_label(interval_type)
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
    metric_type_ = input_validation.parse_metric_type(metric_type)
    interval_type_ = input_validation.parse_time_unit_label(interval_type)
    # print(interval_type, '===>', interval_type_)
    if metric_type_ == 'demand':
        aggregate_mappings = {'quarter': np.count_nonzero}  # arbitrary column name for counting
    elif metric_type_ == 'wait_time':
        aggregate_mappings = {'wait_time': np.mean}  # explicitly give column to average on
    else:
        raise ValueError('Internal Error')
    return _aggregate_sc_data(sc_data, aggregate_option=aggregate_mappings,
                              interval_type=interval_type_)


def generate_demo_quarter_data(requests_in_quarter: pd.DataFrame, output_dir: str) -> None:
    """Generate demand and wait-time metrics for various ranges in quarter."""
    def generate_json_demo_data(data_within_range: pd.DataFrame,
                                range_: str, interval_: str) -> None:
        """Generates json file with demand and wait_time for given range and interval."""
        demand_data = compute_metric_on_intervals(data_within_range, interval_, 'demand')
        wait_time_data = compute_metric_on_intervals(data_within_range, interval_, 'wait_time')
        if interval_ == 'week_in_quarter':
            interval_ = 'week'
        output_path = paths.join_path(output_dir,
                                      'time_range={}&interval={}.json'.format(range_, interval_))
        with open(output_path, 'w') as json_file:
            data_dict = {}
            data_dict.update(demand_data.to_dict())
            data_dict.update(wait_time_data.to_dict())
            json_string = pd.json.dumps(data_dict)
            json_file.write(json_string)

    if len(set(requests_in_quarter['quarter'])) != 1:
        raise ValueError('Given data must contain only one quarter type (eg: \'Fall 2015\').')

    # single file (since single quarter) generated containing daily stats over a quarter
    quarter_term, quarter_year = requests_in_quarter['quarter'].iloc[0].split()
    generate_json_demo_data(data_within_range=requests_in_quarter,
                            range_='quarter+{}_{}'.format(quarter_term, quarter_year),
                            interval_='week_in_quarter')

    # for each week in quarter, generate a file containing daily stats over a week range
    all_weeks_in_qtr = requests_in_quarter['week_in_quarter'].unique()
    for week_num in all_weeks_in_qtr:
        single_week_data = requests_in_quarter[requests_in_quarter['week_in_quarter'] == week_num]
        generate_json_demo_data(data_within_range=single_week_data,
                                range_='week+{}'.format(week_num),
                                interval_='day')

    # for each day in quarter, generate a file containing hourly stats over a day (24 hour) range
    all_recorded_datetimes = pd.Series(data=requests_in_quarter.index)
    dates_in_qtr = all_recorded_datetimes.apply(func=lambda dt: str(dt).split()[0]).unique()
    for date in dates_in_qtr:
        generate_json_demo_data(data_within_range=requests_in_quarter[date],
                                range_='day+{}'.format(date),
                                interval_='hour')


def main():
    df = get_tutor_request_data()
    root_output_dir = paths.join_path(EXTERNAL_DATASETS_DIR, 'pre_generated_data')
    for quarter_name in df['quarter'].unique():
        output_dir = paths.join_path(root_output_dir, quarter_name.replace(' ', '_'))
        shutil.rmtree(output_dir, ignore_errors=True)  # clear the dir if exists
        paths.make_dir_if_not_present(output_dir)
        requests_in_quarter = df[df['quarter'] == quarter_name]
        generate_demo_quarter_data(requests_in_quarter, output_dir)


if __name__ == '__main__':
    main()
