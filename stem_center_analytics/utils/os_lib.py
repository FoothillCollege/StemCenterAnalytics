"""Collection of path, file, and directory utilities.

Intended to add a self containing, consistent, and robust layer over the
standard os module, and thus `os` should not be needed outside this module.
There are four main areas of file/directory functionality covered:
introspection, generic removal/creation, validation, and path parsing.

Notes
-----
* All path parameters for every function are normalized (made path independent).
* Functions with names that start with 'ensure' are meant to be used prior to
  file input/output, to ensure failures are caught ahead of time. This is very
  important since virtually nothing in the API will work correctly if data is not
  retrieved correctly.
* Minimal usage of neighboring functions in this module to reduce call-stack
  complexity, thus os.path functions are used repetitively.
"""
import os
import errno
import codecs
import shutil
import inspect
import importlib
import contextlib
from typing import Sequence


# --------------------------------- file and directory inspection ----------------------------------
def is_existent_file(file_path: str) -> bool:
    """Return True if given file exists, else False.

    See Also
    --------
    * Function `ensure_file_exists` for the exception raising counterpart
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
    * Function `ensure_directory_exists` for the exception raising counterpart
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
    * Given object is a package if and only if its name is same as its parent
      directory's, and its source file is an __init__.py. In that case, we
      return the parent directory of the source file (exact package path)
    """
    file_path = os.path.normpath(inspect.getfile(object=obj))
    if os.path.splitext(file_path)[-1].lower() != '.py':
        raise ValueError('Only python source objects can be inspected for path.')
    if not os.path.isfile(file_path):
        raise FileNotFoundError(errno.ENOENT, 'Python source cannot be found ', file_path)

    if file_path.endswith('__init__.py') and os.path.dirname(file_path).endswith(obj.__name__):
        return os.path.normpath(os.path.dirname(file_path))
    return file_path


# -------------------------------- file and directory manipulation ---------------------------------
def create_directory(dir_path: str) -> None:
    """Make single directory (no intermediates) at given path if creatable.

    See Also
    --------
    * For requirements of a successful directory creation, and the corresponding
      exceptions raised, visit function `ensure_directory_is_creatable`.
    """
    dir_path_ = os.path.normpath(dir_path)
    ensure_directory_is_creatable(dir_path_)
    os.mkdir(path=dir_path_)


def create_temp_file():
    """Create temp file."""
    pass


def remove_directory(dir_path: str, ignore_errors=False) -> None:
    """Remove directory and all children at given path, with an option of ignoring any Errors."""
    dir_path_ = os.path.normpath(dir_path.strip(' '))
    if ignore_errors:
        with contextlib.suppress(OSError):
            shutil.rmtree(path=dir_path_, ignore_errors=True)
    else:
        try:
            ensure_directory_exists(dir_path_)
            shutil.rmtree(path=dir_path_, ignore_errors=False)
        except Exception:
            raise OSError(f'Directory \'{dir_path_}\' failed to be removed.')


def remove_file(file_path: str, ignore_errors=False) -> None:
    """Remove file at given path, with an option of ignoring any Errors."""
    file_path_ = os.path.normpath(file_path.strip(' '))
    if ignore_errors:
        with contextlib.suppress(OSError):
            os.remove(file_path_)
    else:
        try:
            ensure_file_exists(file_path_)
            os.remove(file_path_)
        except Exception:
            raise OSError(f'File \'{file_path_}\' failed to be removed.')


@contextlib.contextmanager
def change_directory(dir_path: str) -> str:
    """Temporarily change directory to given path, similar to 'cd' in terminal.

    Examples
    --------
    >>> import os
    >>> from stem_center_analytics import PROJECT_DIR

    >>> previous_dir = os.getcwd()
    >>> with change_directory(PROJECT_DIR): print(os.getcwd() == PROJECT_DIR)
    True
    >>> previous_dir == os.getcwd()
    True
    """
    new_cwd = os.path.normpath(dir_path)
    old_cwd = os.path.normpath(os.getcwd())
    ensure_directory_exists(new_cwd)
    try:
        os.chdir(new_cwd)
        yield new_cwd
    finally:
        os.chdir(old_cwd)


# ---------------------------------------- error checking ------------------------------------------
def ensure_successful_imports(path: str, names: Sequence[str]) -> None:
    """Valid if all object names are successfully imported at given path location.

    Parameters
    ----------
    path : str
        Path at which to import the given names. If file path given,
        names are imported from its parent directory
    names : array-like of str
        Names of objects to import in the order given. This can be the name of
        any importable object, but to ensure successful imports favor
        full, absolute import names such as 'package.module.func', over '.func'

    Raises
    ------
    ImportError
        * If any module in names failed to be imported correctly

    Notes
    -----
    * Imports are lost (goes out of scope) once the function returns
    """
    path_ = os.path.normpath(path.strip(' '))
    dir_to_import_at = path_ if os.path.isdir(path_) else os.path.normpath(os.path.dirname(path_))

    # import given names from the inferred directory location
    unsuccessful_imports = []
    with change_directory(dir_to_import_at):
        for name in names:
            try:
                importlib.import_module(name)
            except ImportError:
                unsuccessful_imports.append(name)
    if unsuccessful_imports:
        raise ImportError(f'Failed to import modules - {tuple(unsuccessful_imports)}.')


def ensure_file_exists(file_path: str) -> None:
    """Valid if path exists as a file.

    Raises
    ------
    FileNotFoundError
        * If path is not an existing file
    ValueError
        * If file does not end with one of `file_type` extensions
    """
    file_path_ = os.path.normpath(file_path.strip(' '))
    if not os.path.isfile(file_path_):
        raise FileNotFoundError(errno.ENOENT, 'No such file', file_path_)


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


def ensure_file_is_creatable(file_path: str) -> None:
    """Creatable if vacant path in existing directory..

    Raises
    ------
    FileExistsError
        * If file path already exists
    FileNotFoundError
        * If file path is a child of a non-existent parent directory
    ValueError
        * If file does not end with one of `file_type` extensions
    """
    file_path_ = os.path.normpath(file_path.strip(' '))
    parent_dir_path_ = os.path.normpath(os.path.dirname(file_path_))
    if os.path.exists(file_path_):
        raise FileExistsError(errno.ENOENT, 'Cannot create file at non-vacant location', file_path_)
    if not os.path.isdir(parent_dir_path_):
        raise FileNotFoundError(errno.ENOENT, 'Cannot create file in non-existent parent directory',
                                parent_dir_path_)


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
    parent_dir_path_ = os.path.normpath(os.path.dirname(dir_path_))
    if os.path.exists(dir_path_):
        raise FileExistsError(errno.ENOENT, 'Cannot create directory at non-vacant location',
                              dir_path_)
    if not os.path.isdir(parent_dir_path_):
        raise FileNotFoundError(errno.ENOENT,
                                'Cannot create a sub directory in a non-existent parent directory',
                                parent_dir_path_)


def ensure_path_is_absolute(file_path: str) -> None:
    """Valid if file path is absolute.

    Raises
    ------
    ValueError
        * If file path is not absolute

    Notes
    -----
    * In contrast to most other functions in `os_lib` starting with 'ensure',
      given path is NOT checked for existence
    """
    if not os.path.isabs(os.path.normpath(file_path.strip(' '))):
        raise ValueError('File path \'{}\' is not absolute.')


def ensure_valid_file_type(file_path: str, file_type: str) -> None:
    """Valid if file is one of the given file types.

    Raises
    ------
    ValueError
        * If file name has more than one extension
        * If file name doesn't have extension of `file_type`

    Notes
    -----
    * In contrast to most other functions in `os_lib` starting with 'ensure',
      given path is NOT checked for existence
    """
    file_type_ = file_type.lstrip('.')
    file_name, file_extension = get_basename(file_path), get_extension(file_path).lstrip('.')
    if file_name.count('.') != 1:
        raise ValueError(f'File \'{file_name}\' is invalid - file name must have a single extension.')
    if file_extension != file_type_:
        raise ValueError(f'File \'{file_name}\' is invalid - file must be of type \'{file_type_}\'.')


def ensure_valid_file_encoding(file_path: str, encoding: str) -> None:
    """Valid if file at given path can be successfully decoded with given encoding."""
    file_path_ = os.path.normpath(file_path.strip(' '))
    encoding_ = encoding.strip().lower()

    try:
        with codecs.open(file_path_, mode='r', encoding=encoding_, errors='strict'): pass
    except LookupError:
        raise LookupError(f'Unknown encoding - \'{encoding_}\'.') from None
    except UnicodeError:
        raise UnicodeDecodeError(f'File \'{file_path_}\' cannot be decoded as \'{encoding_}\'.')


# ------------------------------------- path name manipulation -------------------------------------
def join_path(path: str, *args: str) -> str:
    """Return path joined with given items.

    Notes
    -----
    * Will break if items contain separators
    """
    base_path_ = os.path.normpath(path.strip(' '))
    path_components = [arg.strip(' ') for arg in args]
    return os.path.normpath(os.path.join(base_path_, *path_components))


def normalize_path(path: str) -> str:
    """Return platform independent path with leading/trailing whitespace removed."""
    return os.path.normpath(path.strip(' '))


def get_extension(path: str) -> str:
    """Return path's (last) extension, including leading '.' separator."""
    return os.path.splitext(os.path.normpath(path.strip(' ')))[-1].lower()


def get_parent_dir(path: str) -> str:
    """Return immediate parent directory of given path."""
    return os.path.normpath(os.path.dirname(os.path.normpath(path.strip(' '))))


def get_basename(path: str, include_extension: bool=True) -> str:
    """Return unix-style basename (eg: '/foo/bar/' => 'bar')."""
    # normalize path, and return the last component
    name = os.path.split(os.path.normpath(path.strip(' ')))[-1]
    return name if include_extension else os.path.splitext(name)[0]
