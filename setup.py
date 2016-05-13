from setuptools import setup

setup(
    name = 'flowz',
    version = '0.1.0',
    description = 'Async I/O - oriented dependency programming framework',
    url = 'https://github.com/ethanrowe/flowz',
    author = 'Ethan Rowe',
    author_email = 'ethan@the-rowes.com',
    test_suite = 'nose.collector',
    packages = [
        'flowz',
        'flowz.examples',
        'flowz.test',
        'flowz.test.channels',
        'flowz.test.util',
        ],
    tests_require = [
        'mock',
        'nose',
        ],
    install_requires = [
        'tornado >= 4.2',
        ])

