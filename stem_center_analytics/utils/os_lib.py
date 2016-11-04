"""Collection of path, file, and directory utilities.

Intended to add a self containing, consistent, and robust layer over the
standard os module. OS should not be needed outside this module.
There are four main areas of file/directory functionality covered:
introspection, generic removal/creation, validation, and path parsing.

Notes
-----
All path parameters for every function are normalized (made path independent).

Minimal usage of neighboring functions in this module reduce call-stack
complexity, thus os.path functions are used repetitively.

Functions with names that start with 'ensure' are meant to be used prior to
file input/output, to ensure failures are caught ahead of time. This is very
important since virtually nothing in the API will work correctly if data is not
retrieved correctly.
"""
import os
import errno
import shutil
import inspect
import importlib
import functools
import contextlib
from typing import Iterable


def is_existent_file(file_path: str) -> bool:
    """Return True if given file exists, else False.

    See Also
    --------
    Function `ensure_file_exists` for the exception raising counterpart
    """
    try:
        ensure_file_exists(file_path)
        return True
    except FileNotFoundError:
        return False


def is_existent_directory(dir_path: str) -> bool:
    """Return True if given path is dir exists, else False.

    See Also
    --------
    Function `ensure_directory_exists` for the exception raising counterpart
    """
    try:
        ensure_directory_exists(dir_path)
        return True
    except FileNotFoundError:
        return False


def is_empty_file(file_path: str) -> bool:
    """Return True if file is empty, else False. Raises if file not present."""
    ensure_file_exists(file_path)
    return os.stat(os.path.normpath(file_path.strip(' '))).st_size == 0


def get_path_of_python_source(obj: object) -> str:
    """Return path to the python source code of given object.

    Raises
    ------
    ValueError
        * If inspected path is not a python file
    FileNotFoundError
        * If inspected path does not exist (extremely unlikely)

    Notes
    -----
    Given object is a package if its name is same as its parent directory's,
    and its source file is an __init__.py. In that case, we return the parent
    directory of the source file (exact package path).
    """
    file_path = os.path.normpath(inspect.getfile(object=obj))
    if os.path.splitext(file_path)[-1].lower() != '.py':
        raise ValueError('Only python source objects can be inspected for path.')
    if not os.path.isfile(file_path):
        raise FileNotFoundError(errno.ENOENT, 'Python source cannot be found ', file_path)

    if file_path.endswith('__init__.py') and os.path.dirname(file_path).endswith(obj.__name__):
        return os.path.normpath(os.path.dirname(file_path))
    return file_path


def create_directory(dir_path: str) -> None:
    """Make single directory (no intermediates) at given path if creatable.

    Notes
    -----
    * Unlike the similar functions, `remove_directory` and `remove_file`,
      no boolean is returned

    See Also
    --------
    For requirements of a successful directory creation, and the corresponding
    exceptions raised, visit function `ensure_directory_is_creatable`.
    """
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    ensure_directory_is_creatable(dir_path_)
    if not is_existent_directory(dir_path_):
        try:
            os.mkdir(path=dir_path_)
        except OSError as e:
            raise e from None


def remove_directory(dir_path: str) -> None:
    """Remove directory"""
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    ensure_directory_exists(dir_path_)
    shutil.rmtree(os.path.normpath(dir_path.strip(' ')), ignore_errors=True)


def remove_file(file_path: str) -> None:
    """Attempt to remove file, silently (suppress errors)."""
    with contextlib.suppress(OSError):
        file_path_ = os.path.normpath(file_path.strip(' '))
        if not os.path.isfile(file_path_):
            raise FileNotFoundError(errno.ENOENT, 'No such file', file_path_)
        os.remove(file_path_)


def ensure_successful_imports(names: Iterable[str]) -> None:
    """Valid if all objects of given names are successfully imported.

    Raises
    ------
    ImportError
        * If any module in names failed to be imported correctly.

    Notes
    -----
    Imports are lost (goes out of scope) once the function returns
    """
    # note: imports are lost once the func goes out of scope
    unsuccessful_imports = []
    for name in names:
        try:
            importlib.import_module(name)
        except ImportError:
            unsuccessful_imports.append(name)
    if unsuccessful_imports:
        raise ImportError('Failed to import modules - {}.'.format(tuple(unsuccessful_imports)))


def ensure_file_type_is_supported(file_path: str, valid_file_types: Iterable(str)=()) -> None:
    """Valid if file is one of the given file types.

    ValueError
        * If file does not end with one of `valid_file_types` extensions
          or has more than one extension.
    """
    file_name, file_type = get_basename(file_path), get_extension(file_path, with_dot=False)
    supported_types = [e.lower().strip(' ').replace('.', '') for e in valid_file_types]
    if supported_types != [] and (file_name.count('.') != 1 or file_type not in supported_types):
        raise ValueError('Unsupported file type of \'{}\': try one of {} files instead.'
                         .format(file_name, tuple(valid_file_types)))


def ensure_file_is_creatable(file_path: str, valid_file_types: Iterable[str]=()) -> None:
    """Creatable if vacant path in existing directory of a given file type.

    Raises
    ------
    FileExistsError
        * If file path already exists
    FileNotFoundError
        * If file path is a child of a non-existent parent directory
    ValueError
        * If file does not end with one of `valid_file_types` extensions
    """
    file_path_ = os.path.normpath(file_path.strip(' '))
    parent_dir_path_ = os.path.dirname(file_path_)
    if os.path.exists(file_path_):
        raise FileExistsError(errno.ENOENT, 'Cannot create file at non-vacant location', file_path_)
    if not os.path.isdir(parent_dir_path_):
        raise FileNotFoundError(errno.ENOENT, 'Cannot create file in non-existent parent directory',
                                parent_dir_path_)
    ensure_file_type_is_supported(file_path_, valid_file_types)


def ensure_directory_is_creatable(dir_path: str) -> None:
    """Directory is creatable if path is vacant in existing directory.

    Raises
    ------
    FileExistsError
        * If directory path already exists
    FileNotFoundError
        * If path is a child of a non-existent parent directory
    """
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    parent_dir_path_ = os.path.dirname(dir_path_)
    if os.path.exists(dir_path_):
        raise FileExistsError(errno.ENOENT, 'Cannot create directory at non-vacant location',
                              dir_path_)
    if not os.path.isdir(parent_dir_path_):
        raise FileNotFoundError(errno.ENOENT,
                                'Cannot create a sub directory in a non-existent parent directory',
                                parent_dir_path_)


def ensure_file_exists(file_path: str, valid_file_types: Iterable[str]=()) -> None:
    """Valid if path exists as a file ending with one of the given file types.

    Raises
    ------
    FileNotFoundError
        * If path is not an existing file
    ValueError
        * If file does not end with one of `valid_file_types` extensions
    """
    file_path_ = normalize_path(file_path)
    if not os.path.isfile(file_path_):
        raise FileNotFoundError(errno.ENOENT, 'No such file', file_path_)
    ensure_file_type_is_supported(file_path_, valid_file_types)  # path valid: check extension


def ensure_directory_exists(dir_path: str) -> None:
    """Valid if path exists as a directory.

    Raises
    ------
    FileNotFoundError
        * If path is a child of a non-existent parent directory
    """
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    if not os.path.isdir(dir_path_):
        raise FileNotFoundError(errno.ENOENT, 'No such directory', dir_path_)


def join_path(base_path: str, *args: str) -> str:
    """Return normalized base path joint with given items."""
    base_path_ = os.path.normpath(base_path.strip(' '))
    path_components = [arg.strip(' ') for arg in args]
    return os.path.normpath(os.path.join(base_path_, *path_components))


def normalize_path(path: str) -> str:
    """Return platform independent path with leading/trailing whitespace removed."""
    return os.path.normpath(path.strip(' '))


def get_extension(path: str, with_dot: bool=True) -> str:
    """Return path's extension with or without leading '.' separator."""
    path_ = os.path.normpath(path.strip(' '))
    file_extension = os.path.splitext(path_)[-1].lower()
    return file_extension if with_dot else file_extension.replace('.', '')


def get_parent_dir(path: str) -> str:
    """Return immediate parent directory of given path."""
    return os.path.normpath(os.path.dirname(os.path.normpath(path.strip(' '))))


def get_basename(path: str, with_ext: bool=True) -> str:
    """Return unix-style basename (eg: '/foo/bar/' => 'bar')."""
    name = os.path.split(os.path.normpath(path.strip(' ')))[-1]  # normalize + grab last component
    return name if with_ext else os.path.splitext(name)[0]
