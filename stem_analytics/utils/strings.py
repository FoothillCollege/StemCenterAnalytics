"""Collection of various string utilities, such as formatting, parsing, and string manipulation."""
import re
from collections import OrderedDict
from typing import Sequence, Iterable, Tuple, List, Set


class ParsingError(ValueError):
    """Base exception raised for value-related errors encountered while parsing a string."""


class ParserDict(OrderedDict):

    """ValidTokenMappings class.

    Examples:
        will raise error: TokenMappings(('monday', ('mon', 'm')))
        won't raise error: TokenMappings(('monday', {'mon', 'm'}))
    """

    def __init__(self, *args: Tuple[str, Set[str]]):
        """Creates mappings (1st item in pair str, 2nd item must support membership testing)."""
        for pair in args:
            message = 'Cannot construct {} object - '.format(self.__class__.__name__)
            if len(pair) != 2 or not isinstance(pair[1], set):
                raise ValueError(message + 'all arguments must be of form Tuple[str, set].')
            if not isinstance(pair[0], str):
                raise ValueError(message + 'keys can contain only contain '
                                           'letters, digits, spaces, and underscores.')
            if (not isinstance(pair[0], str) or not
                    pair[0].replace('_', '').replace(' ', '').isalnum()):
                raise ValueError(message + 'keys can contain only contain '
                                           'letters, digits, spaces, and underscores.')
            if any((not isinstance(s, str) or not s.replace('_', '').replace(' ', '').isalnum())
                   for s in pair[1]):
                raise ValueError(message + 'all strings in each set can only contain '
                                           'letters, digits, spaces, and underscores.')
        super().__init__(args)

    def map_to_token(self, string: str, raise_if_not_found: bool=True) -> str:
        """Return mapped item.

        Notes:
            - `raise_if_not_found`: if true raise if not found, else return '' instead.
            - For each valid token in mappings, parse it
            - NOTHING is done to the string -- it is taken as is (not stripped, etc.)...
        """
        tokens = self.keys()
        for token in tokens:  # check each parser set for membership
            if string in self.__getitem__(token):
                return token

        if not raise_if_not_found:
            return ''
        tokens_ = tuple(tokens)
        tokens_ = (tokens_[:2] + ('...',) + tokens_[-2:]) if len(tokens) > 4 else tokens_
        raise ParsingError('\'{}\' cannot be recognized as one of the following - {}'
                           .format(string, tokens_))


def parse_collection_of_strings(user_input: str or Sequence[str],
                                mapping_func: callable(str)) -> List[str]:
    """Parse collection (iterable) of strings."""
    # distinguishes between a string being an iterable of strings and a list/tuple/etc of strings
    if (isinstance(user_input, str) or not
            isinstance(user_input, Iterable) or not
            all(isinstance(s, str) for s in user_input)):
        raise ParsingError('{} is invalid: only a collection (str, tuple, etc) '
                           'of strings can be parsed.'.format(user_input))
    return [mapping_func(normalize_spaces(s)) for s in user_input]


def parse_dashed_string(user_input: str, mapping_func: callable(str),
                        mapped_values_to_slice: Sequence[str or object]=None) \
        -> List[str or object] or Tuple[str, str] or Tuple[object, object]:
    """Parse string partitioned about ' - ', returning sliced value (if given) or endpoints (None).

    Notes:
        mapped_values_to_slice: isn't necessarily strs! could be objects!
        Parse a dashed string (partitioned about ' - ').
        IF mapping values is none: then return end points only.
        SPACES ARE STRIPPED!
        Allows three legal forms of input: sequence, dashed/delimited-string.
        delimiters are taken to be ',' and ' ' ONLY.
    Examples:
        With each below input corresponding to the elements 'M', 'T', 'W',
        the following function calls return the same mapped values:
            parse_all_input_types('M', 'T', W', day_parser, day_names)
            parse_all_input_types('M - W', day_parser, day_names)
            parse_all_input_types('M, T, W', day_parser, day_names)
    Args:
        user_input (str or Sequence[str]): string(s) to be parsed.
        mapping_func (callable(str)): function used to map string elements to

        mapped_values_to_slice: values within the given list of values to map to + slice.
        delimiters str: the delimiters to split on (default='\|, ')
    Returns:
        Values mapped from input according to input 'type':
            if given as list -- parse the values and return
            if given as non-dashed str -- tokenize the string and parse it
            if given as dashed-str -- slice given list by the values on either side.
    Raises:
        PartialParsingWarning (raised in corresponding lower-level parser) if:
            *Input is a string or sequence with only some components
                successfully mapped to a corresponding value in given list.
        ParsingErrors if:
            *Input contains dash but cannot be parsed as a dashed string.
            *Input cannot be parsed as either a sequence or delimited string,
                which means that no components could be successfully mapped.
    See Also*:
        *What is considered parse-able for each form of input is described in
        detail in each corresponding parser (helper) function.

    FIXME: add definitions of dashed/simple/etc. string.
    """
    if not isinstance(user_input, str):
        raise ParsingError('{} is invalid: only a string can be '
                           'parsed as dashed input.'.format(user_input))
    if ' - ' not in user_input:
        raise ParsingError('\'{}\' is invalid: dashed string must '
                           'contain a \' - \'.'.format(user_input))

    left_token, _, right_token = normalize_spaces(user_input).partition(' - ')
    left_value, right_value = mapping_func(left_token), mapping_func(right_token)
    if not mapped_values_to_slice:  # endpoints only (no order checking here)
        return left_value, right_value

    # other wise we return sliced list
    values = list(mapped_values_to_slice)
    left_index, right_index = values.index(left_value), values.index(right_value)
    if left_index < right_index:
        return values[left_index: right_index + 1]
    raise ParsingError('\'{}\' is invalid: dashed string must be of the form '
                       '\'LHS - RHS\' where LHS < RHS.'.format(user_input))


def parse_delimited_string(user_input: str or Iterable[str],
                           mapping_func: callable(str)) -> List[str]:
    """Parse delimited string (if no delimiters, returns single item list)."""
    if not isinstance(user_input, str):
        raise ParsingError('{} is invalid: only a string can parsed as '
                           'delimited input.'.format(user_input))
    string_segments = normalize_spaces(user_input).split(',')
    return [mapping_func(string.strip(' ')) for string in string_segments]


def normalize_spaces(string: str) -> str:
    """Return given string with all spaces convert to one, with external spaces striped."""
    return ' '.join(string.split())


def natural_sort(strings: Iterable[str]) -> List[str]:
    """Return given iterable sorted in natural order."""
    # todo: add reference section to docstring + example
    def natural_sort_key(string: str) -> int or str:
        return [int(token) if token.isdigit() else token.lower()
                for token in re.split('([0-9]+)', string)]
    return sorted(strings, key=natural_sort_key)


def pretty_format_list(items: Iterable(str), conj: str='and') -> str:
    """Return string containing items separated by comma with conjunction (such as 'and' or 'or').

    Args:
        - args: list elements used to form the sentence.
        - conjunction, str, default='and': str (such as and/or/but/...)
          used to join the last two elements of a (>= 2 element) list.
    """
    token = ' {} '.format(conj.strip(' '))
    strings_ = [str(s).strip(' ') for s in items]
    if len(strings_) == 1:
        return strings_[0]
    elif len(strings_) == 2:
        return strings_[0] + token + strings_[1]
    else:
        return ', '.join(strings_[:-1]) + ',' + token + strings_[-1]
