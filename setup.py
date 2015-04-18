"""package setup"""

import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    """Our test runner."""

    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ["tests"]

    def finalize_options(self):
        # pylint: disable=W0201
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name="arangodb",
    version="0.0.4",
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    ],
    license='ApacheV2',

    keyword="ArangoDB REST API Requests graph nosql database AQL",

    package_dir={'': 'src'},
    namespace_packages=['arangodb'],
    packages=find_packages(
        'src',
        exclude=["tests*"]
    ),
    entry_points="",

    install_requires=[
        'requests',
    ],

    cmdclass={'test': PyTest},
    tests_require=[
        # tests
        'pytest',
        'pytest-pep8',
        'pytest-cache',
        'pytest-random',
        'mock',
    ]
)
