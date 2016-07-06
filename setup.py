from setuptools import setup

setup(
    name = 'flowz',
    version = '0.4.0',
    description = 'Async I/O - oriented dependency programming framework',
    url = 'https://github.com/ethanrowe/flowz',
    author = 'Ethan Rowe',
    author_email = 'ethan@the-rowes.com',
    license = 'MIT',
    test_suite = 'nose.collector',
    packages = [
        'flowz',
        'flowz.channels',
        'flowz.examples',
        'flowz.test',
        'flowz.test.artifacts',
        'flowz.test.channels',
        'flowz.test.util',
        ],
    tests_require = [
        'mock',
        'nose',
        'six >= 1.9.0',
        ],
    install_requires = [
        'tornado >= 4.2',
        'futures >= 3.0.5'
        ])
