import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name='zegami-sdk',
    version='0.1.0',
    description='A suite of tools for interacting with Zegami through Python.',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/zegami/zegami-python-sdk',
    author='Zegami Ltd',
    author_email='help@zegami.com',
    license='Apache 2.0',
    packages= [
        'zegami_sdk'
    ],
    include_package_data=True,
    install_requires=[
        'pathlib',
        'numpy',
        'pandas',
        'Pillow',
        'requests',
        'xlrd',
    ]
)