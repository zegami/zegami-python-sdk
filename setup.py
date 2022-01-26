# -*- coding: utf-8 -*-
# Copyright 2021 Zegami Ltd

"""Setup code."""

import os
import pathlib

from setuptools import setup

# Update version here when you want to increment the version in PyPi
sdk_version = '0.4.5'

# If no ZEGAMI_SDK_VERSION set use the version
try:
    version = os.environ['ZEGAMI_SDK_VERSION']
    if version == '':
        raise KeyError
except KeyError:
    version = sdk_version
    if not os.environ.get('SDK_PRODUCTION_BUILD'):
        version += '+dev'


HERE = pathlib.Path(__file__).parent
with open('README.md', 'r', encoding='utf-8') as f:
    README = f.read()

setup(
    name='zegami-sdk',
    version=version,
    description='A suite of tools for interacting with Zegami through Python.',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/zegami/zegami-python-sdk',
    author='Zegami Ltd',
    author_email='help@zegami.com',
    license='Apache 2.0',
    packages=[
        'zegami_sdk'
    ],
    include_package_data=True,
    install_requires=[
        'azure-storage-blob>=12.8.1',
        'python-magic>=0.4.24; sys_platform=="linux"',
        'python-magic-bin; platform_system=="Windows" or sys_platform=="darwin"',
        'pathlib>=1.0',
        'numpy>=1.17.0',
        'pandas>=1.0.0',
        'Pillow>=7.0.0',
        'requests>=2.22.0',
        'tqdm>=4.29.0',
        'urllib3>=1.25.2'
    ],
    python_requires='>=3.6'
)
