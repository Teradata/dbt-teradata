#!/usr/bin/env python
import os
import sys

from setuptools import setup


if sys.version_info < (3, 7) or sys.version_info >= (3, 10):
    print('Error: dbt-teradata does not support this version of Python.')
    print('Please install Python 3.7 or higher but less than 3.10.')
    sys.exit(1)


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt-teradata"
package_version = "1.0.0.0"
description = """The Teradata adapter plugin for dbt (data build tool)"""


setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author="Teradata Corporation",
    author_email="developers@teradata.com",
    url="https://github.com/Teradata/dbt-teradata",
    packages=[
        'dbt.adapters.teradata',
        'dbt.include.teradata',
    ],
    package_data={
        'dbt.include.teradata': [
            'macros/*.sql',
            'macros/materializations/**/*.sql',
            'dbt_project.yml',
            'sample_profiles.yml',
        ],
    },
    install_requires=[
        "dbt-core==1.0.0",
        "teradatasql>=16.20.0.0",
    ],
    classifiers=[
        'Development Status :: 4 - Beta',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.7,<3.10",
)
