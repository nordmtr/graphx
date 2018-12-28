'''Tests for graph functions'''

import graphx.lib.graphx as gx


def mapper_double(row):
    yield row
    yield row


def test_map():
    table = [
        {'text': 'Double me, plz', 'index': 1},
        {'text': 'And me too', 'index': 2},
    ]

    etalon = [
        {'text': 'Double me, plz', 'index': 1},
        {'text': 'Double me, plz', 'index': 1},
        {'text': 'And me too', 'index': 2},
        {'text': 'And me too', 'index': 2},
    ]

    chain = gx.Chain(source='table')
    chain.add_map(mapper_double)

    result = chain.run(table=table)

    assert result == etalon


def test_sort():
    table = [
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 3, 'time': 8, 'index': 1},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 2, 'index': 6},
    ]

    etalon = [
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 2, 'index': 6},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 3, 'time': 8, 'index': 1},
    ]
    keys = ['distance', 'time']

    chain = gx.Chain(source='table')
    chain.add_sort(keys=keys)

    result = chain.run(table=table)

    assert result == etalon


def folder_sum_columnwise(state, record):
    for column in state:
        state[column] += record[column]
    return state


def test_fold():
    table = [
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 3, 'time': 8, 'index': 1},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 2, 'index': 6},
    ]

    etalon = [
        {'distance': 16, 'time': 33, 'index': 21}
    ]

    chain = gx.Chain(source='table')
    chain.add_fold(folder_sum_columnwise, {'distance': 0, 'time': 0, 'index': 0})

    result = chain.run(table=table)

    assert result == etalon


def reducer_unique(group):
    yield next(group)


def test_reduce():
    table = [
        {'index': 0, 'text': 'I am the first'},
        {'index': 0, 'text': 'I am the second, delete me'},
        {'index': 0, 'text': 'I am the third, delete me too'},
        {'index': 1, 'text': 'I am the first in this group'},
        {'index': 1, 'text': 'Delete me plz'},
    ]

    etalon = [
        {'index': 0, 'text': 'I am the first'},
        {'index': 1, 'text': 'I am the first in this group'},
    ]

    chain = gx.Chain(source='table')
    chain.add_reduce(reducer_unique, ['index'])

    result = chain.run(table=table)

    assert result == etalon


def test_join_table_inner():
    table = [
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 3, 'time': 8, 'index': 1},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 2, 'index': 6},
    ]

    speed = [
        {'index': 1, 'speed': 30},
        {'index': 3, 'speed': 50},
        {'index': 6, 'speed': 70},
        {'index': 8, 'speed': 130},
    ]

    etalon = [
        {'distance': 3, 'time': 8, 'index': 1, 'speed': 30},
        {'distance': 3, 'time': 6, 'index': 3, 'speed': 50},
        {'distance': 3, 'time': 2, 'index': 6, 'speed': 70},
    ]

    chain = gx.Chain(source='table')
    chain.add_join(speed, ['index'], 'inner')
    chain.add_sort(['index'])

    result = chain.run(table=table)

    assert result == etalon


def test_join_table_outer():
    table = [
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 3, 'time': 8, 'index': 1},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 2, 'index': 6},
    ]

    speed = [
        {'index': 1, 'speed': 30},
        {'index': 3, 'speed': 50},
        {'index': 6, 'speed': 70},
        {'index': 8, 'speed': 130},
    ]

    etalon = [
        {'distance': 1, 'time': 7, 'index': 0, 'speed': None},
        {'distance': 3, 'time': 8, 'index': 1, 'speed': 30},
        {'distance': 2, 'time': 4, 'index': 2, 'speed': None},
        {'distance': 3, 'time': 6, 'index': 3, 'speed': 50},
        {'distance': 1, 'time': 3, 'index': 4, 'speed': None},
        {'distance': 3, 'time': 3, 'index': 5, 'speed': None},
        {'distance': 3, 'time': 2, 'index': 6, 'speed': 70},
        {'distance': None, 'time': None, 'index': 8, 'speed': 130},
    ]

    chain = gx.Chain(source='table')
    chain.add_join(speed, ['index'], 'outer')
    chain.add_sort(['index'])

    result = chain.run(table=table)

    assert result == etalon


def test_join_table_left():
    table = [
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 3, 'time': 8, 'index': 1},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 2, 'index': 6},
    ]

    speed = [
        {'index': 1, 'speed': 30},
        {'index': 3, 'speed': 50},
        {'index': 6, 'speed': 70},
        {'index': 8, 'speed': 130},
    ]

    etalon = [
        {'distance': 1, 'time': 7, 'index': 0, 'speed': None},
        {'distance': 3, 'time': 8, 'index': 1, 'speed': 30},
        {'distance': 2, 'time': 4, 'index': 2, 'speed': None},
        {'distance': 3, 'time': 6, 'index': 3, 'speed': 50},
        {'distance': 1, 'time': 3, 'index': 4, 'speed': None},
        {'distance': 3, 'time': 3, 'index': 5, 'speed': None},
        {'distance': 3, 'time': 2, 'index': 6, 'speed': 70},
    ]

    chain = gx.Chain(source='table')
    chain.add_join(speed, ['index'], 'left')
    chain.add_sort(['index'])

    result = chain.run(table=table)

    assert result == etalon


def test_join_table_right():
    table = [
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 3, 'time': 8, 'index': 1},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 2, 'index': 6},
    ]

    speed = [
        {'index': 1, 'speed': 30},
        {'index': 3, 'speed': 50},
        {'index': 6, 'speed': 70},
        {'index': 8, 'speed': 130},
    ]

    etalon = [
        {'distance': 3, 'time': 8, 'index': 1, 'speed': 30},
        {'distance': 3, 'time': 6, 'index': 3, 'speed': 50},
        {'distance': 3, 'time': 2, 'index': 6, 'speed': 70},
        {'distance': None, 'time': None, 'index': 8, 'speed': 130},
    ]

    chain = gx.Chain(source='table')
    chain.add_join(speed, ['index'], 'right')
    chain.add_sort(['index'])

    result = chain.run(table=table)

    assert result == etalon


def test_join_chain_inner():
    table = [
        {'distance': 1, 'time': 7, 'index': 0},
        {'distance': 3, 'time': 8, 'index': 1},
        {'distance': 2, 'time': 4, 'index': 2},
        {'distance': 3, 'time': 6, 'index': 3},
        {'distance': 1, 'time': 3, 'index': 4},
        {'distance': 3, 'time': 3, 'index': 5},
        {'distance': 3, 'time': 2, 'index': 6},
    ]

    speed = [
        {'index': 1, 'speed': 30},
        {'index': 3, 'speed': 50},
        {'index': 6, 'speed': 70},
        {'index': 8, 'speed': 130},
    ]

    etalon = [
        {'distance': 3, 'time': 8, 'index': 1, 'speed': 30},
        {'distance': 3, 'time': 6, 'index': 3, 'speed': 50},
        {'distance': 3, 'time': 2, 'index': 6, 'speed': 70},
    ]

    chain_speed = gx.Chain(source='speed')
    chain_speed.add_sort(['index'])

    chain_table = gx.Chain(source='table')
    chain_table.add_join(chain_speed, ['index'], 'inner')
    chain_table.add_sort(['index'])

    result = chain_table.run(table=table, speed=speed)

    assert result == etalon
