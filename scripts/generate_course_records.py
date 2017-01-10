"""Generates a json file containing all courses logged in historical data."""
import re
from typing import Iterable, List
from collections import OrderedDict

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


if __name__ == '__main__':
    # ordering of json file: math, phys, bio, chem, engr, cs, OTHER_SUBJECTS
    courses = warehouse.get_set_of_all_courses()
    core_subject_names = list(input_validation.CORE_SUBJECTS.keys())
    other_subject_names = list(input_validation.OTHER_SUBJECTS.keys())

    subject_map = OrderedDict({'ordering': core_subject_names + ['Other']})
    for subject_name in core_subject_names:
        df_sliced_by_subj_name = tutor_log[tutor_log['course_subject'] == subject_name]
        course_numbers_for_subject = df_sliced_by_subj_name['course_number'].unique()
        subject_map[subject_name] = _natural_sort(strings=course_numbers_for_subject)

    subject_map['Other'] = []
    for subject_name in other_subject_names:
        df = tutor_log[tutor_log['course_subject'] == subject_name]  # sliced by subject name
        subject_map['Other'] += _natural_sort(
            strings=(df['course_subject'] + ' ' + df['course_number']).unique()
        )
    io_lib.create_json_file(warehouse.DATA_FILE_PATHS.COURSE_RECORDS, subject_map)
