"""Generates a json file containing all courses logged in historical data."""
import json
from collections import OrderedDict

from stem_analytics import INTERNAL_DATASETS_DIR, warehouse
from stem_analytics.core import input_validation
from stem_analytics.utils import paths, strings


def update_course_records():
    # ordering of json file: math, phys, bio, chem, engr, cs, other
    tutor_log = warehouse.get_tutor_request_data()
    core_subject_names = list(input_validation.course_subject_types.core.keys())
    other_subject_names = list(input_validation.course_subject_types.other.keys())

    subject_map = OrderedDict({'ordering': core_subject_names + ['other']})
    for subject_name in core_subject_names:
        df_sliced_by_subj_name = tutor_log[tutor_log['course_subject'] == subject_name]
        course_numbers_for_subject = df_sliced_by_subj_name['course_number'].unique()
        subject_map[subject_name] = strings.natural_sort(strings=course_numbers_for_subject)

    subject_map['Other'] = []
    for subject_name in other_subject_names:
        df = tutor_log[tutor_log['course_subject'] == subject_name]  # sliced by subject name
        subject_map['Other'] += strings.natural_sort(
            strings=(df['course_subject'] + ' ' + df['course_number']).unique()
        )

    output_file = paths.join_path(INTERNAL_DATASETS_DIR, 'course_records.json')
    with open(output_file, 'w') as json_file:
        json_string = json.dumps(subject_map)
        json_file.write(json_string)

    # to read back the json file:
    # with open(_INTERNAL_DATASETS_PATH_MAP['course_records'], 'r') as json_file:
    #     json_string = json.load(json_file)


if __name__ == '__main__':
    update_course_records()
