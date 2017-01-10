"""Generates a json file containing all courses logged in historical data."""
import re
from typing import Iterable, Tuple, List
from collections import OrderedDict

import pandas as pd

from stem_center_analytics import warehouse
from stem_center_analytics.core import input_validation
from stem_center_analytics.utils import io_lib


def _natural_sort(strings: Iterable[str]) -> List[str]:
    """Return given iterable sorted in natural order.

    Natural order is a stricter form of lexicographic, in which numerical
    entries are accounted for.
    """
    def natural_sort_key(string: str) -> int or str:
        return [int(token) if token.isdigit() else token.lower()
                for token in re.split('([0-9]+)', string)]
    return sorted(strings, key=natural_sort_key)


def _extract_subject_and_number(course: str) -> Tuple[str, str]:
    course_without_section = course[:course.rfind(' ')]
    number_start = course_without_section.rfind(' ') + 1
    return course_without_section[:number_start - 1], course_without_section[number_start:]


if __name__ == '__main__':
    # ordering of json file: math, phys, bio, chem, engr, cs, other
    all_courses = warehouse.get_tutor_request_data(columns_to_use=['course'], as_unique=True)
    subject_and_number_records = [_extract_subject_and_number(course) for course in all_courses]

    courses = pd.DataFrame.from_records(subject_and_number_records, columns=['subject', 'number'])
    core_subject_names = list(input_validation.CORE_SUBJECTS.keys())
    other_subject_names = list(input_validation.OTHER_SUBJECTS.keys())

    subject_map = OrderedDict({'ordering': core_subject_names + ['Other']})
    for subject_name in core_subject_names:
        df_sliced_by_subj_name = courses[courses['subject'] == subject_name]
        course_numbers_for_subject = df_sliced_by_subj_name['number'].unique()
        subject_map[subject_name] = _natural_sort(strings=course_numbers_for_subject)

    subject_map['Other'] = []
    for subject_name in other_subject_names:
        df = courses[courses['subject'] == subject_name]  # sliced by subject name
        subject_map['Other'] += _natural_sort(
            strings=(df['subject'] + ' ' + df['number']).unique()
        )
    io_lib.create_json_file(warehouse.DATA_FILE_PATHS.COURSE_RECORDS, subject_map)
