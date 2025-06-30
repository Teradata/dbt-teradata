#!/usr/bin/env python
import os
import sys

from setuptools import setup


if sys.version_info < (3, 9) or sys.version_info >= (3, 13):
    print('Error: dbt-teradata does not support this version of Python.')
    print('Please install Python 3.9 or higher but less than 3.13.')
    sys.exit(1)


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


package_name = "dbt-teradata"
package_version = "1.9.3"
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
            'macros/utils/*.sql',
            'macros/dbt_external_tables/helpers/*.sql',
            'macros/dbt_external_tables/*.sql',
            'dbt_project.yml',
            'sample_profiles.yml',
        ],
    },
    install_requires=[
        "dbt-adapters>=1.7.2",
        "dbt-common>=1.3.0",
        "teradatasql>=20.00.00.10",
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    python_requires=">=3.9,<3.13",
)
