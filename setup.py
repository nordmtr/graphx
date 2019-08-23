import setuptools


setuptools.setup(
    name='graphx',
    version='1.0',
    description='Framework for graph computations. Supports map, reduce, fold, sort and join operations',
    author='Dmitry Norkin',
    author_email='nordmtr@gmail.com',
    requires=[
        'json',
        'sys',
        'functools',
        'itertools',
        'pprint',
        'copy',
        'logging',
        'collections',
        'abc',
        'operator',
        'typing'
    ],
    packages=setuptools.find_packages(),
)
