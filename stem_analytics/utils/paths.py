"""Contains general file functionality, including directory manipulation/introspection.

Notes:
    - This module is meant is self containing in terms of tools needed for path
      name manipulation, as a layer over the built in os module. OS should not be
      needed outside this module.
    - This Module also contains built upon os functions with additional
      normalization, checks, etc.
    - Minimal usage of neighboring functions in this module to reduce call-stack
      complexity, thus os.path funcs are used repetitively.
    - Errors/exceptions are further standardized
    - ALL paths are normalized/made path independent for EVERY function in this module.

Raises:
    OSErrors are raised in many of these support functions (if file not found, etc.),
    as almost nothing in the project will work if data is not retrieved correctly.
"""
import os
import errno
import inspect
import importlib
import contextlib
from typing import Iterable

from stem_analytics.utils import strings


def ensure_successful_imports(names: Iterable[str]) -> None:
    """Raise error if any names (eg: foo.bar, baz) cannot be imported."""
    # note: imports are lost once the func goes out of scope
    unsuccessful_imports = []
    for name in names:
        try:
            importlib.import_module(name)
        except ImportError:
            unsuccessful_imports.append(name)
    if unsuccessful_imports:
        raise ImportError('Failed to import module(s) - {}.'
                          .format(strings.pretty_format_list(unsuccessful_imports)))


def get_path_of_python_source(obj: object) -> str:
    """Return path to the python source code of given object.

    If inferred path directs to an __init__.py module
    Notes:
        - If inferred path directs to an __init__.py module, then returns the
          package directory that lies directly above.
    """
    file_path = os.path.normpath(inspect.getfile(object=obj))
    name_without_ext, file_extension = os.path.splitext(os.path.split(file_path)[-1])
    if file_extension != '.py':
        raise ValueError('Only python source objects can be inspected for path.')

    # in the case of a package, the file name will be __init__.py, so we get the dir above
    source_path = os.path.dirname(file_path) if name_without_ext == '__init__' else file_path
    if os.path.exists(source_path):
        return source_path
    raise FileNotFoundError(errno.ENOENT, 'Python source cannot be found ', file_path)


def make_dir_if_not_present(dir_path: str) -> None:
    """Make immediate directory at given path if not present (interm dirs are NOT created!)."""
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    if not dir_exists(dir_path_):
        try:
            ensure_creatable_file_path(dir_path_)
            os.mkdir(path=dir_path_)
        except OSError as e:
            raise e from None


def remove_file_if_present(file_path: str) -> bool:
    """Silently (errors suppressed) remove file if found."""
    file_path_ = os.path.normpath(file_path.strip(' '))
    if file_exists(file_path):
        with contextlib.suppress(OSError):
            os.remove(file_path_)


def file_exists(file_path: str) -> bool:
    """Return True if given file exists, else False.

    See Also:
        `ensure_file_path_exists` for more comprehensive exception version.
    """
    try:
        ensure_file_path_exists(file_path)
        return True
    except FileNotFoundError:
        return False


def dir_exists(dir_path: str) -> bool:
    """Return True if given file exists, else False.

    See Also:
        `ensure_directory_exists` for more comprehensive exception version.
    """
    try:
        ensure_directory_exists(dir_path)
        return True
    except FileNotFoundError:
        return False


def ensure_file_type_is_supported(file_path: str, valid_file_types: Iterable(str)=()) -> None:
    """No error raise if given `file_path` is of a type present in `valid_file_types`."""
    file_name, file_type = get_basename(file_path), get_extension(file_path, with_dot=False)
    supported_types = [e.lower().strip(' ').replace('.', '') for e in valid_file_types]
    if supported_types != [] and (file_name.count('.') != 1 or file_type not in supported_types):
        raise ValueError('Unsupported file type of \'{}\': try a {} file instead.'
                         .format(file_name, strings.pretty_format_list(supported_types, conj='or')))


def ensure_file_path_exists(file_path: str, valid_file_types: Iterable[str]=()) -> None:
    """Validates as an existing file_path.

    Notes:
        - for valid existing: path must exist with legal extension
        - This is not (and not intended to) be fail safe for either file existence or creation.
          Rather, this function (and similar ones in `file_paths.py`) are meant to filter out
          obvious file errors before reading/writing/conversion of contents.
    Raises:
        OSError is raised if invalid existing/creatable file_path.
    See Also:
        `validate_dir_path()` for similar functionality for dirs.
    """
    file_path_ = normalize_path(file_path)
    if not os.path.isfile(file_path_):
        raise FileNotFoundError(errno.ENOENT, 'No such file', file_path_)
    ensure_file_type_is_supported(file_path_, valid_file_types)  # path is cleared: validate extension


def ensure_creatable_file_path(file_path: str, valid_file_types: Iterable[str]=()) -> None:
    """Validates as creatable file path.

    Notes:
        - for valid path creation: nonexistent path with existent parent dir and legal extension
        - This is not (and not intended to) be fail safe for either file existence or creation.
          Rather, this function (and similar ones in `file_paths.py`) are meant to filter out
          obvious file errors before reading/writing/conversion of contents.
    Raises:
        OSError is raised if invalid existing/creatable file_path.
    See Also:
        `validate_dir_path()` for similar functionality for dirs.
    """
    file_path_ = os.path.normpath(file_path.strip(' '))
    parent_dir_path_ = os.path.dirname(file_path_)
    if os.path.exists(file_path_):
        raise FileExistsError(errno.ENOENT,
                              'Cannot create file at non-vacant location', file_path_)
    if not os.path.isdir(parent_dir_path_):
        raise FileNotFoundError(errno.ENOENT, 'Cannot create file in non-existent parent directory',
                                parent_dir_path_)
    ensure_file_type_is_supported(file_path_, valid_file_types)


def ensure_directory_exists(dir_path: str) -> None:
    """No errors raised if given path is an existent directory.

    Notes:
        - for valid existing: path must exist as a valid directory
        - This is not (and not intended to) be fail safe for either dir existence or creation.
          Rather, this function (and similar ones in `file_paths.py`) are meant to filter out
          obvious file errors before reading/writing/conversion of contents.
    Raises:
        OSError is raised if invalid existing/creatable dir_path.
    See Also:
        `ensure_file_path_exists()` for similar functionality for files.
    """
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    if not os.path.isdir(dir_path_):
        raise FileNotFoundError(errno.ENOENT, 'No such dir', dir_path_)


def ensure_directory_is_creatable(dir_path: str) -> None:
    """No errors raised if given path is a creatable directory.

    Notes:
        - for valid path creation: nonexistent path with existent parent dir
        - This is not (and not intended to) be fail safe for either dir existence or creation.
          Rather, this function (and similar ones in `file_paths.py`) are meant to filter out
          obvious file errors before reading/writing/conversion of contents.
    Raises:
        OSError is raised if invalid existing/creatable dir_path.
    See Also:
        `ensure_file_path_exists()` for similar functionality for files.
    """
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    parent_dir_path_ = os.path.dirname(dir_path_)
    if os.path.exists(dir_path_):
        raise FileExistsError(errno.ENOENT, 'Cannot create dir at non-vacant location', dir_path_)
    if not os.path.isdir(parent_dir_path_):
        raise FileNotFoundError(errno.ENOENT, 'Cannot create dir in non-existent parent directory',
                                parent_dir_path_)


def join_path(base_path: str, *args: str) -> str:
    """Return normalized base path with items appended (does more than os.path.join())."""
    base_path_ = os.path.normpath(base_path.strip(' '))
    path_components = [arg.strip(' ') for arg in args]
    return os.path.normpath(os.path.join(base_path_, *path_components))


def normalize_path(path: str) -> str:
    """Return platform independent path with any leading/trailing whitespace removed."""
    return os.path.normpath(path.strip(' '))


def get_extension(path: str, with_dot: bool=True) -> str:
    """Return extension of given path, includes '.' if `with_dot`=True."""
    path_ = os.path.normpath(path.strip(' '))
    file_extension = os.path.splitext(path_)[-1].lower()
    return file_extension if with_dot else file_extension.replace('.', '')


def get_parent_dir(path: str) -> str:
    r"""Return immediate parent directory of given path.

    Examples:
    >>> path = r'C:\Users\jperm\Dropbox\Stem_Analytics\stem_analytics\utils\paths.py'
    >>> print(get_parent_dir(path))
    C:\Users\jperm\Dropbox\Stem_Analytics\stem_analytics\utils
    """
    return os.path.dirname(os.path.normpath(path.strip(' ')))


def get_basename(path: str, with_ext: bool=True) -> str:
    """Return unix-style basename (i.e.: '/foo/bar/' => 'bar')."""
    name = os.path.split(os.path.normpath(path.strip(' ')))[-1]  # normalize + grab last component
    return name if with_ext else os.path.splitext(name)[0]
