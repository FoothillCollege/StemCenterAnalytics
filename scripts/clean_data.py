"""Temporary data cleaning script: takes the 'cleaned' csv and cleans further, and builds db.

------ quick and dirty script to go from below form:
time_of_request,quarter,week_in_quarter,day,course,wait_time
2013-09-25 11:05:32,F 13,1,W,chem 30a 1,62
2013-09-25 11:06:12,F 13,1,W,math 48b 1,29

------ to this form:
time_of_request,wait_time,quarter,week_in_quarter,day,course_subject,course_number,
    course_section,day_in_week
2013-09-25 11:05:32,62,Fall 2013,1,W,Chemistry,30A,1,3
2013-09-25 11:06:12,29,Fall 2013,1,W,Mathematics,48B,1,3
"""
import pandas as pd

from stem_center_analytics import EXTERNAL_DATASETS_DIR, INTERNAL_DATASETS_DIR
from stem_center_analytics.core import input_validation
from stem_center_analytics.utils import io_lib, os_lib
from stem_center_analytics.warehouse import _data_models



def add_quarters(df: pd.DataFrame, drop_outside_range=False) -> pd.DataFrame:
    """Add quarter column to df.

    Assumes that the fh quarter log is up-to-date. Get only the dates we have in df.
    """
    new_df = df
    quarter_df = _data_models.get_quarter_dates()
    new_df.insert(loc=0, column='quarter', value=None)  # create a quarter column
    # brute force set quarter get_values for all sc_data entries...OPTIMIZE LATER!!
    for qtr_term, qtr_start, qtr_end in quarter_df.itertuples():
        for entry_date in new_df.index:
            if pd.to_datetime(qtr_start) <= pd.to_datetime(entry_date) <= pd.to_datetime(qtr_end):
                new_df.set_value(index=entry_date, col='quarter', value=qtr_term)

    return new_df if not drop_outside_range else new_df.dropna(subset=['quarter'])


def add_week_in_quarters(df: pd.DataFrame) -> pd.DataFrame:
    """Adds weeks in the quarter to the given df."""
    quarters = _data_models.get_quarter_dates()
    week_of_qtr_starts = df['quarter'].apply(lambda qtr: quarters.loc[qtr, 'start_date'].weekofyear)
    list_of_weeks_in_qtr = [entry_week - week_of_qtr_start + 1
                            for entry_week, week_of_qtr_start
                            in zip(df.index.weekofyear, week_of_qtr_starts)]

    new_df = df
    new_df.insert(loc=1, column='week_in_quarter', value=list_of_weeks_in_qtr)
    return new_df


def override_index(df: pd.DataFrame, col_name_to_index: str='time_of_request') -> pd.DataFrame:
    """Sort the column to be made index, reset and drop old index column."""
    # for dropping duplicate dates...
    # df.groupby(new_sc_data.index).first()
    new_df = df
    new_df.sort_values(by=[col_name_to_index], ascending=True, inplace=True)
    new_df.set_index(keys=col_name_to_index, drop=True, inplace=True)
    new_df.drop(df.index.name, axis=1, inplace=True)  # now, drop col with old index name
    return new_df


# for replacing negative wait-times...
# df.replace(to_replace=-1, value=0, inplace=True)

def main():
    data_file_name = 'unclean_tutor_requests.csv'
    df = io_lib.read_flat_file_as_df(os_lib.join_path(EXTERNAL_DATASETS_DIR, data_file_name))
    print(df)

    # replace course column by its components (subject, number, section)
    df['course'] = input_validation.parse_courses(list((df['Course_name'] + ' ' + df['Course_section'])))

    df[['course_subject', 'course_number', 'course_section']] = df['course'].apply(pd.Series)
    df.drop(labels=['Course_name', 'Course_section', 'course'], axis=1, inplace=True)
    print(df)

    df = add_quarters(df, drop_outside_range=True)
    df = add_week_in_quarters(df)
    print(df)
    '''
    # rebuilds database with tutor request data
    db_path = os_lib.join_path(INTERNAL_DATASETS_DIR, 'stem_center_db.sql')
    io_lib.create_sql_file(db_path, replace_if_exists=True)
    with _data_models.connect_to_stem_center_db() as con:
        io_lib.write_df_to_database(con, df, new_table_name='tutor_requests')
    '''


# since input validation's only accepts a datetime RANGE (dashed input),
# use input validation's function parse_datetime and apply to col/index


if __name__ == '__main__':
    main()


# todo: ENGLISH?!? what am I suppose to with that?
# todo: add wait_time fix to avoid issues with zero wait_times; cause such a thing is impossible
