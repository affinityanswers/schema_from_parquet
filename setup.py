#!/usr/bin/env python3

import setuptools

setuptools.setup(
    name="parquet_athena",
    version='0.1',
    description='Package for generating athena table schema for parquet ',
    url='https://bitbucket.org/aa-scm-admin/misc', 
    packages=['parquet_to_athena'],
    scripts = ['scripts/parquet_to_athena'],
    install_requires=['pyarrow', 'awscli', 's3fs', 'query-athena @ git+https://github.com/affinityanswers/query-athena.git@64d2d46270fac135e0b4dbcc533f1f440c0f614c']
)
