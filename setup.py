#!/usr/bin/env python3

import setuptools

setuptools.setup(
    name="parquet_athena",
    version="0.2",
    description="Package for generating athena table schema for parquet ",
    url="https://github.com/affinityanswers/schema_from_parquet",
    packages=["parquet_to_athena"],
    scripts=["scripts/parquet_to_athena"],
    install_requires=[
        "pyarrow<15,>=14",
        "awscli<1.31>=1.0",
        "s3fs<0.5,>=0.4",
        "query-athena @ git+https://github.com/affinityanswers/query-athena.git@64d2d46270fac135e0b4dbcc533f1f440c0f614c",
    ],
)
