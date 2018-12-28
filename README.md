# GraphX

This framework provides a useful tool for computations on large data frames using MapReduce model.
```python
import graphx as gx

chain = gx.Chain(source='docs')
chain.add_map(mapper_split_text)
chain.add_sort(keys=['text'])
chain.add_reduce(reducer_count_words, keys=['text'])
chain.add_sort(keys=['count'])

chain.run(docs=docs)
```
All computations are divided into 2 steps:
1) Build a graph structure
2) Run this graph on a preferred input, consecutively executing all operations according to the predefined structure

## Installation

To install the package you need to clone this repository to your computer and install it using pip.
```bash
git clone https://gitlab.manytask.org/python-autumn-2018/aesthete/compgraph.git
cd ./compgraph
pip install .
```

## Operations

Graph is built of 5 different operations:
### Map
Applies mapper function on each row of the table and yields a new table with consecutive results of this function.
Interface of `add_map`:
```python
chain.add_map(mapper_function)
```
  **Example**:
  Mapper function, that duplicates all rows in the table:
  ```python
  def mapper_double(row):
      yield row
      yield row
  ```
### Sort
Interface of `add_sort`:
```python
chain.add_sort(keys=[key1, key2, key3], reverse=True)
```
Sorts the table by `keys`. If `reverse` argument is True, sort is done in the reversed order.
### Fold
Folds the table into one row using `folder_function`. Fold consecutively calls `folder_function` on a pair of current state and new row.
Interface of `add_fold`:
```python
chain.add_fold(folder_function, initial_state)
```
Parameter `initial_state` defines the initial value of `current_state`, that is transferred to `folder_function`.
**Example**:
Folder function, that counts the number of row in the table
```python
def count_rows_folder(current_state, new_row):
	current_state['count'] += 1
	return current_state
```
### Reduce
Calls reduce function on the group of table rows with the common value in `keys` column
Interface of `add_reduce`:
```python
chain.add_reduce(reducer_function, keys=[key1, key2, key3])
```
The table must be sorted by `keys` before reducing. If not, graph warns you about this and performs sort by itself.
**Example**:
Reducer function, that retains only one row for each set of values in `keys` columns:
```python
def reducer_unique(group):
	yield next(group)
```
### Join
Merges two tables by `keys`, using preferred strategy of joining. Rows of the new table are created from rows of these two tables.
Interface of `add_join`:
```python
chain.add_join(
	on=other_chain,
	keys=[key1, key2, key3],
	strategy='inner'
)
```
This operation finds rows in the right table, which values in `keys` columns coincide with corresponding values in rows in the left table, and adds the cartesian product of these rows to the new table.
Further actions depend on the strategy of joining.
- Inner
Return the new table "as is"
- Left
For each row in the left table, that was not added to the new table, fill missing values in columns of the right table with Nones and add them to the new table
- Right
Execute the symmetrical strategy to the left one.
- Outer
Executes left and right strategy simultaneously.

You can find more specific information about the strategies on [Wikipedia](https://en.wikipedia.org/wiki/Join_(SQL))

## Running graph

To run a prebuilt graph you need to execute run method:
```python
chain.run(source=source, verbose=True)
```
Parameter `source` denotes the source of the data, and can be either opened file, IO object, iterable or list
If `verbose` is True, info logging is on.
If `debug` is True, debug logging is on.

## Examples
### Word count
**Task**:

Count the number of word occurrences in all texts for each word.

**Input**:
```python
docs = [
	{'doc_id': 1, 'text': 'hello, my little WORLD'},
	{'doc_id': 2, 'text': 'Hello, my little little hell'}
]
```
**Solution**:
```python
delimiters = [
    ' ', '.', '?', '!', ':', ',', '"',
    ';', '$', '%', '^', '&', '*', '(', ')',
    '@', '#', '~', '<', '>', '/', '-'
]

def mapper_split_text(row):
	"""
	splits text into words
	"""
    splitted_text = re.split('[' + ''.join(delimiters) + ']', row[text_column])
    for word in splitted_text:
        if word:
            yield {'text': word.lower(), count_column: 1}

def reducer_count_words(word_dictionary):
	"""
	counts word occurrences in the dictionary
	"""
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

chain.run(docs=docs)
```
**Output**:
```python
etalon = [
    {'count': 1, 'text': 'hell'},
    {'count': 1, 'text': 'world'},
    {'count': 2, 'text': 'hello'},
    {'count': 2, 'text': 'my'},
    {'count': 3, 'text': 'little'}
]
```
