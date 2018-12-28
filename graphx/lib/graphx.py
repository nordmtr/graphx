import json
import sys
import typing
import logging
from functools import reduce
from copy import deepcopy
from pprint import pprint
from itertools import groupby
from collections.abc import Iterator
from abc import ABC, abstractmethod
from operator import itemgetter


class Chain:
    """
    Basic class for graph computations. Supports operations *map*, *reduce*, *sort*, *fold* and *join*.
    """

    def __init__(self, source: typing.Union[typing.TypeVar('Chain'), str]):
        """
        Construct a Chain object

        :param source: Chain object or str, identifies the source of data for the current chain
        """
        self._source = source
        self._operations = []
        self._table = []
        self._launches = 0
        self._max_launches = 0
        self._kwargs = {}
        if isinstance(self._source, Chain):
            self._source._max_launches += 1

    def add_map(self, mapper_function: typing.Generator):
        """
        Adds map operation to the graph

        :param mapper_function: generator, takes one row, yields some rows

        Example:
            def mapper_double(row):
                yield row
                yield row
        """
        self._operations.append(MapOperation(mapper_function=mapper_function))
        return deepcopy(self)

    def add_sort(self, keys: typing.Union[list, tuple], reverse: bool = False):
        """
        Adds sort operation to the graph

        :param keys: tuple of column names, by which the table will be sorted
        :param reverse: if True, sort is done in reversed order
        """
        self._operations.append(SortOperation(keys=keys, reverse=reverse))
        return deepcopy(self)

    def add_fold(self, folder_function: typing.Callable, initial_state: dict = None):
        """
        Adds fold operation to the graph

        :param initial_state (optional): if provided, folder function takes this as an initial value
        :param folder_function: function, takes one row, yields one row

        Example:
            def folder_sum_columnwise(state, record):
                for column in state:
                    state[column] += record[column]
                return state
        """
        self._operations.append(
            FoldOperation(folder_function=folder_function, initial_state=initial_state)
        )
        return deepcopy(self)

    def add_reduce(self, reducer_function: typing.Generator, keys: typing.Union[list, tuple]):
        """
        Adds reduce operation to the graph. Requires graph sorted by *keys*. Before running checks
        if it is sorted and if not sorts it.

        :param keys: keys to be used in sort, grouping and reducing
        :param reducer_function: generator, takes rows, grouped by the same value in *keys*, yields rows

        Example:
            def term_frequency_reducer(records):
                word_count = Counter()

                for r in records:
                    word_count[r['word']] += 1

                total  = sum(word_count.values())
                for w, count in word_count.items():
                    yield {
                        'doc_id' : r['doc_id'],
                        'word' : w,
                        'tf' : count / total
                    }

        """
        self._operations.append(ReduceOperation(reducer_function=reducer_function, keys=keys))
        return deepcopy(self)

    def add_join(
            self,
            on: typing.Union[typing.TypeVar('Chain'), list],
            keys: typing.Union[list, tuple] = (),
            strategy: str = 'inner'
    ):
        """
        Joins the current graph table with the table *on* using *strategy*

        :param on: prebuilt Chain object or iterable
        :param keys: tuple of columns to merge by
        :param strategy: can be one of 4 strings
            'inner' -- joins only strings with coinciding values in the *keys* columns
            'left'  -- executes inner strategy and then adds remaining rows in the left table,
                       filling missing values from the right table with Nones
            'right' -- executes strategy, symmetrical to the left
            'outer' -- executes both left and right strategies
        """
        self._operations.append(JoinOperation(on=on, keys=keys, strategy=strategy))
        if isinstance(on, Chain):
            on._max_launches += 1
        return deepcopy(self)

    def run(self, output_stream: typing.TextIO = None, verbose: bool = False, debug: bool = False, **kwargs):
        """
        Runs the predefined graph

        :param output_stream (optional): IO object. If provided writes the computed table into it
        :param verbose (optional): boolean. If True logs all operations
        :param kwargs: *kwargs[source]* is IO object or list
        """
        self._table = []
        self._launches = 0
        if debug:
            logging.basicConfig(
                format='%(asctime)s - %(levelname)s - %(message)s',
                level=logging.DEBUG
            )
        elif verbose:
            logging.basicConfig(
                format='%(asctime)s - %(levelname)s - %(message)s',
                level=logging.INFO
            )
        else:
            logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
        run_result = self._run(output_stream, verbose, **kwargs)
        del self._table
        return run_result

    def _run(self, output_stream=None, verbose=False, **kwargs):
        logging.info('Executing run')
        logging.debug('Current launches: %d', self._launches)
        logging.debug('Max launches: %d', self._max_launches)
        self._launches += 1
        if self._launches == 1:
            logging.debug('Kwargs:')
            logging.debug(str(kwargs))
            if self._source in kwargs.keys():
                input_stream = kwargs[self._source]
            elif isinstance(self._source, Chain):
                input_stream = self._source._run(verbose=verbose, **kwargs)
            self._kwargs = kwargs
            self._load_table(input_stream)
            for operation in self._operations:
                logging.info('Executing operation %s', repr(operation))
                self._table = operation.run(self._table, verbose=verbose, **kwargs)
                logging.info('Operation %s successfully executed', repr(operation))
        else:
            logging.info('Table has already been computed')
        if output_stream:
            pprint(self._table, stream=output_stream)
        self._table = list(self._table)
        return self._table

    def _load_table(self, input_stream):
        if isinstance(input_stream, list):
            self._table = iter(input_stream)
        elif hasattr(input_stream, 'read'):
            for line in input_stream:
                self._table.append(json.loads(line))
        elif isinstance(input_stream, Iterator):
            self._table = input_stream
        logging.info('Table loaded')


class Operation(ABC):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        super().__init__()

    @abstractmethod
    def run(self, _table, verbose=False, **kwargs):
        pass


class MapOperation(Operation):
    def run(self, _table, verbose=False, **kwargs):
        mapper_function = self.kwargs['mapper_function']
        for row in _table:
            yield from mapper_function(row)


class ReduceOperation(Operation):
    def run(self, _table, verbose=False, **kwargs):
        reducer_function = self.kwargs['reducer_function']
        keys = self.kwargs['keys']

        group_keys = []

        for key, group in groupby(_table, key=lambda k: [k[column] for column in keys]):
            if key not in group_keys:
                group_keys.append(key)
            else:
                logging.error('Table is not sorted, result of this operation is unexpectable.')
            yield from reducer_function(group)


class FoldOperation(Operation):
    def run(self, _table, verbose=False, **kwargs):
        folder_function = self.kwargs['folder_function']
        initial_state = self.kwargs['initial_state']
        if initial_state:
            yield reduce(folder_function, _table, initial_state)
        else:
            yield reduce(folder_function, _table)


class SortOperation(Operation):
    def run(self, _table, verbose=False, **kwargs):
        keys = self.kwargs['keys']
        reverse = self.kwargs['reverse']
        _table = list(_table)
        new_keys = [key for key in keys if key in _table[0].keys()]
        if new_keys != keys:
            logging.warning('Not all keys exist in the table')
            print('Missing keys:', file=sys.stderr)
            pprint([item for item in keys if item not in new_keys])
        return sorted(_table, key=itemgetter(*new_keys), reverse=reverse)


class JoinOperation(Operation):
    def _merge_dicts(self, left_dict, right_dict, keys):
        left_keys = left_dict.keys()
        right_keys = right_dict.keys()
        new_dict = left_dict.copy()
        for key in right_keys - keys:
            if key not in left_keys:
                new_dict[key] = right_dict[key]
            else:
                new_dict[key + '1'] = left_dict[key]
                new_dict[key + '2'] = right_dict[key]
                del new_dict[key]
        yield new_dict

    def _merge_groups_with_different_keys(self, smaller_group, greater_group, keys):
        greater_keys = greater_group[0].keys()
        for item in smaller_group:
            yield from self._merge_dicts(
                item,
                {key: None for key in greater_keys - keys},
                keys
            )

    def run(self, _table, verbose=False, **kwargs):
        on = self.kwargs['on']
        keys = set(self.kwargs['keys'])
        strategy = self.kwargs['strategy']

        if isinstance(on, Chain):
            new_table = on._run(verbose=verbose, **kwargs)
            if on._launches >= on._max_launches:
                del on._table
        else:
            new_table = on
        if strategy == 'left':
            left_table = _table
            right_table = new_table
        else:
            left_table = new_table
            right_table = _table

        left_groups = groupby(left_table, key=lambda k: [k[key] for key in keys])
        right_groups = groupby(right_table, key=lambda k: [k[key] for key in keys])

        try:
            left_value, left_group = next(left_groups)
            right_value, right_group = next(right_groups)
            previous_left_value = left_value
            previous_right_value = right_value
            left_group = list(left_group)
            right_group = list(right_group)
        except StopIteration:
            pass

        left_groups_empty = False
        right_groups_empty = False

        while not left_groups_empty or not right_groups_empty:
            if left_value < previous_left_value or right_value < previous_right_value:
                logging.error('Tables are not sorted, result of this operation is unexpectable.')
            if (left_value < right_value or right_groups_empty) and not left_groups_empty:
                if strategy in ('left', 'right', 'outer'):
                    yield from self._merge_groups_with_different_keys(left_group, right_group, keys)
                try:
                    previous_left_value = left_value
                    left_value, left_group = next(left_groups)
                    left_group = list(left_group)
                except StopIteration:
                    left_groups_empty = True
            elif (left_value > right_value or left_groups_empty) and not right_groups_empty:
                if strategy == 'outer':
                    yield from self._merge_groups_with_different_keys(right_group, left_group, keys)
                try:
                    previous_right_value = right_value
                    right_value, right_group = next(right_groups)
                    right_group = list(right_group)
                except StopIteration:
                    right_groups_empty = True
            else:
                for left_item in left_group:
                    for right_item in right_group:
                        yield from self._merge_dicts(left_item, right_item, keys)
                try:
                    previous_left_value = left_value
                    left_value, left_group = next(left_groups)
                    left_group = list(left_group)
                except StopIteration:
                    left_groups_empty = True
                try:
                    previous_right_value = right_value
                    right_value, right_group = next(right_groups)
                    right_group = list(right_group)
                except StopIteration:
                    right_groups_empty = True
