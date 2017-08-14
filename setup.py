import os
from setuptools import setup

this_dir = os.path.dirname(__file__)

PACKAGES = [
    'pypresto'
]

PACKAGE_CLASSIFIERS = [
    'Programming Language :: Python :: 2.7',
    'Development Status :: 4 - Beta'
]

PACKAGE_KEYWORDS = [
    'presto',
    'wrapper'
]

readme_filename = os.path.join(this_dir,'README.md')
with open(readme_filename) as f:
    PACKAGE_LONG_DESCRIPTION = f.read()

requirements_filename = os.path.join(this_dir,'requirements.txt')
with open(requirements_filename) as f:
    PACKAGE_INSTALL_REQUIRES = [line[:-1] for line in f]

setup(
    name='pypresto',
    version='0.1.5',
    author='NonWhite',
    author_email='wperezurcia@gmail.com',
    url='https://github.com/NonWhite/pyprestowrapper',
    license='LICENSE',
    description='Python wrapper for running queries on Presto server',
    packages=PACKAGES,
    keyswords=PACKAGE_KEYWORDS,
    long_description=PACKAGE_LONG_DESCRIPTION,
    install_requires=PACKAGE_INSTALL_REQUIRES,
    classifiers=PACKAGE_CLASSIFIERS,
)
