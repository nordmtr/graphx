import re

from collections import Counter
from math import log, asin, sin, cos, pi
from pprint import pprint
from datetime import datetime
from itertools import cycle, islice

import graphx.lib.graphx as gx


def build_word_count_graph(input_stream, text_column='text', count_column='count'):
    delimiters = [
        ' ', '.', '?', '!', ':', ',', '"',
        ';', '$', '%', '^', '&', '*', '(', ')',
        '@', '#', '~', '<', '>', '/', '-'
    ]

    def mapper_split_text(row):
        splitted_text = re.split('[' + ''.join(delimiters) + ']', row[text_column])
        for word in splitted_text:
            if word:
                yield {'text': word.lower(), count_column: 1}

    def reducer_count_words(word_dictionary):
        word_count = Counter()

        for row in word_dictionary:
            word_count[row['text']] += 1

        for word, count in word_count.items():
            yield {
                'text': word,
                'count': count
            }

    chain = gx.Chain(source=input_stream)
    chain.add_map(mapper_split_text)
    chain.add_sort(keys=['text'])
    chain.add_reduce(reducer_count_words, keys=['text'])
    chain.add_sort(keys=['count'])

    return chain


def build_inverted_index_graph(input_stream, doc_column='doc_id', text_column='text'):
    delimiters = [
        ' ', '.', '?', '!', ':', ',', '"',
        ';', '$', '%', '^', '&', '*', '(', ')',
        '@', '#', '~', '<', '>', '/', '-'
    ]

    def mapper_tokenizer(row):
        """
         splits rows with 'text' field into set of rows with 'token' field
        (one for every occurence of every word in text)
        """
        tokens = re.split('[' + ''.join(delimiters) + ']', row[text_column])

        for token in tokens:
            if token:
                yield {
                    'doc_id': row['doc_id'],
                    'word': token.lower(),
                }

    def folder_count_docs(state, record=None):
        state['docs_count'] += 1
        return state

    def reducer_unique(group):
        yield next(group)

    def reducer_calc_idf(group):
        counter = 0
        for row in group:
            counter += 1
            current_row = row
        yield {
            'word': current_row['word'],
            'idf': log(current_row['docs_count'] / counter)
        }

    def term_frequency_reducer(records):
        '''
            calculates term frequency for every word in doc_id
        '''

        word_count = Counter()

        for row in records:
            word_count[row['word']] += 1
            current_row = row

        total = sum(word_count.values())
        for word, count in word_count.items():
            yield {
                'doc_id': current_row['doc_id'],
                'word': word,
                'tf': count / total
            }

    def tf_idf_mapper(row):
        tf = row['tf']
        idf = row['idf']
        yield {
            'word': row['word'],
            'doc_id': row['doc_id'],
            'tf_idf': tf * idf
        }

    def invert_index_reducer(group):
        for row in list(group)[-3:][::-1]:
            yield row

    split_word = gx.Chain(source=input_stream)
    split_word.add_map(mapper_tokenizer)

    count_docs = gx.Chain(source=input_stream)
    count_docs.add_fold(folder_count_docs, {'docs_count': 0})

    count_idf = gx.Chain(source=split_word)
    count_idf.add_sort(keys=['doc_id', 'word'])
    count_idf.add_reduce(reducer_unique, keys=['doc_id', 'word'])
    count_idf.add_join(count_docs, strategy='outer')
    count_idf.add_sort(keys=['word'])
    count_idf.add_reduce(reducer_calc_idf, keys=['word'])

    calc_index = gx.Chain(source=split_word)
    calc_index.add_sort(keys=['doc_id'])
    calc_index.add_reduce(term_frequency_reducer, keys=['doc_id'])
    calc_index.add_sort(keys=['word'])
    calc_index.add_join(count_idf, keys=['word'], strategy='inner')
    calc_index.add_map(tf_idf_mapper)
    calc_index.add_sort(keys=['word', 'tf_idf'])
    calc_index.add_reduce(invert_index_reducer, keys=['word'])

    return calc_index


def build_pmi_graph(input_stream, doc_column='doc_id', text_column='text'):
    delimiters = [
        ' ', '.', '?', '!', ':', ',', '"',
        ';', '$', '%', '^', '&', '*', '(', ')',
        '@', '#', '~', '<', '>', '/', '-'
    ]

    def mapper_tokenizer(row):
        """
         splits rows with 'text' field into set of rows with 'token' field
        (one for every occurence of every word in text)
        """
        tokens = re.split('[' + ''.join(delimiters) + ']', row[text_column])

        for token in tokens:
            if len(token) > 4:
                yield {
                    'doc_id': row['doc_id'],
                    'word': token.lower(),
                }

    def folder_count_words(state, record=None):
        state['total_words_count'] += 1
        return state

    def frequency_in_all_docs_reducer(group):
        counter = 0

        for row in group:
            counter += 1
            current_row = row
        yield {
            'word': current_row['word'],
            'otf': counter / current_row['total_words_count']
        }

    def term_frequency_reducer(records):
        word_count = Counter()

        for row in records:
            word_count[row['word']] += 1
            current_row = row

        total = sum(word_count.values())
        for word, count in word_count.items():
            yield {
                'doc_id': current_row['doc_id'],
                'word': word,
                'tf_doc': count / total
            }

    def find_doc_ids_folder(state, new_row):
        state['doc_ids'].add(new_row['doc_id'])
        return state

    def pmi_mapper(row):
        tf_doc = row['tf_doc']
        otf = row['otf']
        yield {
            'pmi': log(tf_doc / otf),
            'word': row['word'],
            'doc_id': row['doc_id']
        }

    # def double_words_reducer(group):
    #     doc_count = Counter()
    #     new_group = list(group)
    #     for row in new_group:
    #         doc_count[row['doc_id']] += 1
    #
    #     for key in new_group[0]['doc_ids']:
    #         if key not in doc_count.keys():
    #             return
    #         if doc_count[key] < 2:
    #             return
    #
    #     for row in new_group:
    #         yield row

    def double_words_reducer(group):
        new_group = list(group)
        if len(new_group) >= 2:
            for row in new_group:
                yield row

    def pmi_reducer(group):
        for row in list(group)[-10:][::-1]:
            yield row

    doc_ids = gx.Chain(source=input_stream)
    doc_ids.add_fold(find_doc_ids_folder, {'doc_ids': set([])})

    split_word = gx.Chain(source=input_stream)
    split_word.add_map(mapper_tokenizer)
    split_word.add_join(doc_ids, strategy='outer')
    split_word.add_sort(keys=['word'])
    split_word.add_reduce(double_words_reducer, keys=['word'])

    count_words_in_all_docs = gx.Chain(source=split_word)
    count_words_in_all_docs.add_fold(folder_count_words, {'total_words_count': 0})

    count_all_docs = gx.Chain(source=split_word)
    count_all_docs.add_join(count_words_in_all_docs, strategy='outer')
    count_all_docs.add_sort(keys=['word'])
    count_all_docs.add_reduce(frequency_in_all_docs_reducer, keys=['word'])

    calc_index = gx.Chain(source=split_word)
    calc_index.add_sort(keys=['doc_id'])
    calc_index.add_reduce(term_frequency_reducer, keys=['doc_id'])
    calc_index.add_sort(keys=['word'])
    calc_index.add_join(count_all_docs, keys=['word'], strategy='inner')
    calc_index.add_map(pmi_mapper)
    calc_index.add_sort(keys=['doc_id', 'pmi'])
    calc_index.add_reduce(pmi_reducer, keys=['doc_id'])

    return calc_index


def build_yandex_maps_graph(input_stream, input_stream_length):

    weekdays = {
        0: 'Mon',
        1: 'Tue',
        2: 'Wed',
        3: 'Thu',
        4: 'Fri',
        5: 'Sat',
        6: 'Sun'
    }

    def compute_length(lon1, lat1, lon2, lat2):
        radius = 6365
        lat1 = lat1 * pi / 180
        lat2 = lat2 * pi / 180
        lon1 = lon1 * pi / 180
        lon2 = lon2 * pi / 180
        haversine = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
        return 2 * radius * asin(haversine ** 0.5)

    def mapper_time_and_distance(row):
        enter_time = datetime.strptime(row['enter_time'], '%Y%m%dT%H%M%S.%f')
        leave_time = datetime.strptime(row['leave_time'], '%Y%m%dT%H%M%S.%f')

        weekday = weekdays[enter_time.weekday()]
        hour = enter_time.hour
        time_lapse = (leave_time - enter_time).total_seconds() / 3600
        distance = compute_length(*row['end'], *row['start'])

        yield {
            'weekday': weekday,
            'hour': hour,
            'time_lapse': time_lapse,
            'distance': distance
        }

    def speed_reducer(group):
        total_distance = 0
        total_time = 0

        for row in group:
            total_distance += row['distance']
            total_time += row['time_lapse']
            weekday = row['weekday']
            hour = row['hour']

        yield {
            'weekday': weekday,
            'hour': hour,
            'speed': total_distance / total_time
        }

    times = gx.Chain(source=input_stream)

    routes = gx.Chain(source=input_stream_length)
    routes.add_join(times, keys=['edge_id'], strategy='inner')
    routes.add_map(mapper_time_and_distance)
    routes.add_sort(keys=['weekday', 'hour'])
    routes.add_reduce(speed_reducer, keys=['weekday', 'hour'])

    return routes
