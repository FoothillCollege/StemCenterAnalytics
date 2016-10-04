"""Collection of various (DataFrame IO) input/output tools."""
import pandas as pd

from stem_analytics.utils import paths


def read_flat_file_as_df(file_path: str, datetime_format: str=None) -> pd.DataFrame:
    """Fetch flat file from given path as a pandas DF. Supported: .csv, and .json.

    Notes:
        - if not datetime_format then attempt to infer.
        - datetime_format example: '%Y-%m-%d %H:%M:%S'
    """
    dt_format = datetime_format.strip(' ') if datetime_format else None
    date_unit = 's' if dt_format and dt_format.endswith('%S') else 'ms'  # infer type from format
    file_to_df_mappings = {
        '.csv': lambda: pd.read_csv(file_path, index_col=0, parse_dates=True,
                                    date_parser=lambda d: pd.to_datetime(d, format=datetime_format),
                                    encoding='utf8', infer_datetime_format=True),
        '.json': lambda: pd.read_json(file_path, date_unit=date_unit)
    }
    paths.ensure_file_path_exists(file_path, valid_file_types=file_to_df_mappings.keys())
    return file_to_df_mappings[paths.get_extension(file_path)]()


def write_df_to_flat_file(df: pd.DataFrame, file_path: str,
                          replace_if_exists: bool=False, datetime_format: str=None) -> None:
    """Write given DF to flat file at given location. Supported: .csv, and .json.

    Notes:
        - if not datetime_format then attempt to infer.
        - datetime_format example: '%Y-%m-%d %H:%M:%S'
    """
    dt_format = datetime_format.strip(' ') if datetime_format else None
    date_unit = 's' if dt_format and dt_format.endswith('%S') else 'ms'  # infer type from format
    df_to_file_mappings = {
        '.csv': lambda: df.to_csv(file_path, date_format=dt_format),
        '.json': lambda: df.to_json(file_path, date_format=dt_format, date_unit=date_unit)
    }
    if replace_if_exists and paths.file_exists(file_path):
        paths.remove_file_if_present(file_path)
    paths.ensure_creatable_file_path(file_path, valid_file_types=df_to_file_mappings.keys())
    return df_to_file_mappings[paths.get_extension(file_path)]()  # possible assertion error
