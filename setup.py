#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-indeed",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_indeed"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.12.2",
        "pandas==1.3.5",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-indeed=tap_indeed:main
    """,
    packages=["tap_indeed"],
    package_data={
        "schemas": ["tap_indeed/schemas/*.json"]
    },
    include_package_data=True,
)
