import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent
with open('README.md', 'r', encoding='utf-8') as f:
    README = f.read()

setup(
    name='zegami-sdk',
    version='0.3.0',
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
        'azure-storage-blob',
        'python-magic; sys_platform=="linux"',
        'python-magic-bin; platform_system=="Windows" or sys_platform=="darwin"',
        'colorama',
        'importlib-metadata',
        'keyring',
        'twine',
        'pathlib',
        'numpy',
        'pandas',
        'Pillow',
        'requests',
        'xlrd',
    ],
    python_requires='>=3.6'
)
