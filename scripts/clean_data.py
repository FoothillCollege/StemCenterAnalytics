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
import stem_center_analytics
from stem_center_analytics import EXTERNAL_DATASETS_DIR, INTERNAL_DATASETS_DIR
from stem_center_analytics.utils import io_lib, os_lib
from stem_center_analytics.warehouse import _data_models

# todo: add wait_time fix to avoid issues with zero wait_times; cause such a thing is impossible
# ???

QUARTER_NAME_MAP = {'F': 'Fall', 'U': 'Summer', 'S': 'Spring', 'W': 'Winter'}
SUBJECT_NAME_MAP = {
    'chem': 'Chemistry',
    'math': 'Mathematics',
    'engr': 'Engineering',
    'cs': 'Computer Science',
    'ncbs': 'Non Credit Basic Skills',
    'phys': 'Physics',
    'psyc': 'Psychology',
    'astr': 'Astronomy',
    'econ': 'Economics',
    'bio': 'Biology'
}


def main():
    # fixme: modify script to clean completely raw data (rather than partially)
    data_file_name = 'semi_clean_requests.csv'
    stem_center_analytics.config_pandas_display_size()
    df = io_lib.read_flat_file_as_df(
        os_lib.join_path(EXTERNAL_DATASETS_DIR, 'misc_data', data_file_name)
    )

    # replace course column by its components (subject, number, section)
    cols = df.columns.tolist()
    df = df[cols[-1:] + cols[:-1]]
    df[['course_subject', 'course_number', 'course_section']] = df['course'].str.split(expand=True)
    df.drop(labels=['course'], axis=1, inplace=True)

    # expand subjects to their full names, uppercase course numbers, remove anomalies in sections
    df['course_subject'].replace(SUBJECT_NAME_MAP, inplace=True)
    df['course_number'] = df['course_number'].str.upper()
    df['course_section'].replace({'O': '0', 'l': '1'}, inplace=True)

    # expand quarter names from abbreviated to full description (eg, 'F 13' -> 'Fall 2013')
    df['quarter'] = df['quarter'].apply(
        lambda qtr: '{} 20{}'.format(QUARTER_NAME_MAP[qtr.split()[0]], qtr.split()[1])
    )

    # replace `day` col with col `day_in_week` containing (day) values with ints 1-7
    df.drop('day', axis=1, inplace=True)
    position_next_to_week = df.columns.tolist().index('week_in_quarter') + 1
    df.insert(loc=position_next_to_week, column='day_in_week', value=df.index.weekday + 1)

    # rebuilds database with tutor request data
    db_path = os_lib.join_path(INTERNAL_DATASETS_DIR, 'stem_center_db.sql')
    io_lib.create_sql_file(db_path, replace_if_exists=True)
    with _data_models.connect_to_stem_center_db() as con:
        io_lib.write_df_to_database(con, df, new_table_name='tutor_requests')


# since input validation's only accepts a datetime RANGE (dashed input),
# use input validation's function parse_datetime and apply to col/index


if __name__ == '__main__':
    main()
