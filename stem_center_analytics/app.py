#!flask/bin/python
"""REST web service meant to provide easy to graph data to a web frontend.

Examples
--------
To test the web service locally, run the below commands from terminal:
    curl -u jeff:python -i "http://127.0.0.1:5000/?day=2013-09-25&courses=all"
    curl -u jeff:python -i "http://127.0.0.1:5000/?week=Fall+2013+-+Week+1&courses=all"
    curl -u jeff:python -i "http://127.0.0.1:5000/?quarter=Fall+2013&courses=all"
"""
import io
from typing import Iterable

import flask
import pandas as pd
from flask import Flask, jsonify, make_response, request
from flask_cors import CORS

from stem_center_analytics import warehouse, PROJECT_DIR
from stem_center_analytics.utils import os_lib, io_lib

# NOTE - this web service is a temporary setup, with the data to be replaced by dynamic API calls

# --------------------------------------------------------------------------------------------------
# todo: replace static file retrieval with dynamic API calls to stem_center_analytics.CORE_SUBJECTS
# todo: add more specific error handling + catch dispatching errors (figure out on google, later!)
# todo: figure out how to cache code/modules (NOT the data) - aka minimize loading times/session(s)
# --------------------------------------------------------------------------------------------------

app = Flask(__name__)
CORS(app)

# establish the inferred interval for a given time range
RANGE_TO_INTERVAL_MAP = {
    'day': 'hour',
    'week': 'day',
    'quarter': 'week'
}


@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad request'}), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


def _determine_quarter_by_date(date_string: str) -> str:
    """Return quarter in which given date resides (eg: '2013-09-25'-> 'Fall 2013')."""
    df = warehouse.get_quarter_dates()
    date = pd.to_datetime(date_string, format='%Y-%m-%d %H:%M:%S')
    for quarter_name in df.index:
        quarter_row = df.ix[quarter_name]
        if quarter_row['start_date'] <= date <= quarter_row['end_date']:
            return quarter_name
    raise ValueError(f'Date {date_string} does not fall between dates of any archived quarters.')


def _parse_request(request_args: flask.Request.args) -> dict:
    """Return parsed args from route as a json response.

    Note: for now, only course=all is permitted.
    Sample query arguments:
        - quarter=Summer+2015&course=math
        - week=Summer+2015+-+Week+1&course=math
        - day=2015-09-25&course=math
    """
    # ensure query string contains two parameter names: courses, and either day, week, or quarter
    arg_names = list(request_args.keys())
    if (len(arg_names) != 2 or
            'courses' not in arg_names or not
            any((name in ['day', 'week', 'quarter']) for name in arg_names)):
        raise ValueError('Request cannot be parsed.')

    # get the parameter name corresponding to a time range (either day, week, or quarter)
    arg_names.pop(arg_names.index('courses'))
    time_range_type = arg_names.pop()

    # -------------------------------- Additional Inferring Below ----------------------------------
    # examples: day=2013-09-25 -- week=Fall+2013+-+Week+1 -- quarter=Fall+2013
    courses = request_args.get('courses', type=str).split(',')
    raw_time_range = request_args.get(time_range_type, type=str).replace('+', ' ')
    
    if time_range_type == 'day':
        quarter_name, time_range_value = _determine_quarter_by_date(raw_time_range), raw_time_range
    elif time_range_type == 'week':
        quarter_name, time_range_value = raw_time_range.split(' - ')
        time_range_value = time_range_value.replace('Week', '').strip()
    elif time_range_type == 'quarter':
        time_range_value, quarter_name = raw_time_range, raw_time_range
    else:
        raise ValueError('Internal Error.')
    interval_type = RANGE_TO_INTERVAL_MAP[time_range_type]
    # eg: Fall 2013 day week 1 ['all']
    # print(quarter_name, interval_type, time_range_type, time_range_value, courses)
    return {'quarter': quarter_name,
            'interval': interval_type,
            'range': {time_range_type: time_range_value},
            'courses': courses}


def _get_file(quarter: str, time_range_type: str,
              time_range: str, interval: str,
              courses: Iterable[str]=('all',)) -> io.TextIOWrapper:
    """Return json file corresponding to given data.

    Examples
    --------
    _get_file(quarter='Fall+2013', time_range_type='quarter',
              time_range='Fall+2013', interval='week')
    _get_file(quarter='Fall+2013', time_range_type='week',
              time_range='1', interval='day'))
    _get_file(quarter='Fall+2013', time_range_type='day',
              time_range='2013-09-25', interval='hour'))
    """
    if courses != 'all' and courses != ('all',) and courses != ['all']:
        raise ValueError('No specific courses supported yet.')
    time_range_ = time_range.replace('+', ' ').replace(' ', '_')
    matched_file = os_lib.join_path(
        PROJECT_DIR, 'external_datasets', 'pre_generated_data',
        quarter.replace('+', ' ').replace(' ', '_'),
        f'time_range={time_range_type}+{time_range_}&interval={interval}.json'
    )
    return io_lib.read_json_file(matched_file)


# fixme: add dispatching error handling for invalid tokens in url routes
@app.route('/', methods=['GET'])
def main():
    """Core web service function.

    Take the below return values from parse_request, extract them,
    and pass to get_file as parameters.

    {'quarter': quarter_name,
     'interval': interval_type,
     'range': {time_range_type: time_range_value},
     'courses': courses}
    """
    try:
        arg_dict = _parse_request(request.args)
        time_range_type = list(arg_dict['range'].keys()).pop()
        time_range = arg_dict['range'][time_range_type]
        interval, quarter, courses = arg_dict['interval'], arg_dict['quarter'], arg_dict['courses']
        return jsonify(_get_file(quarter, time_range_type, time_range, interval, courses))
    except (ValueError, FileNotFoundError):
        flask.abort(400)


if __name__ == '__main__':
    app.run(host='0.0.0.0')  # rebind to env's port

# todo: instead of relying on file retrieval, do the whole thing dynamically
