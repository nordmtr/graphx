#!/usr/bin/env python

from setuptools import setup

# your setup code here ...
packages = {
    'graphx': 'graphx/',
    'graphx.lib': 'graphx/lib/',
    'algorithms': 'test/'
}


setup(
    name='graphx',
    version='1.0',
    description='Framework for graph computations. Supports map, reduce, fold, sort and join operations',
    author='Dmitry Norkin',
    author_email='esthete282@yandex.ru',
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
    packages=packages,
    package_dir=packages
)
