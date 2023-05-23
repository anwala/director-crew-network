#!/usr/bin/env python

from setuptools import setup, find_packages

desc = """Mapping Collaboration Networks in Film and TV Production (building a film/TV director-crew network)"""

__appversion__ = None

#__appversion__, defined here
exec(open('dcnet/version.py').read())


setup(
    name='dcnet',
    version=__appversion__,
    description=desc,
    long_description='See: https://github.com/anwala/director-crew-network',
    author='Alexander C. Nwala',
    author_email='acnwala@wm.edu',
    url='https://github.com/anwala/director-crew-network',
    packages=find_packages(),
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    package_data={
        'dcnet': []
    },
    install_requires=[
        'beautifulsoup4',
        'isoduration',
        'NwalaTextUtils',
        'pandas',
        'PyMovieDb'
    ],
    scripts=[
        'bin/dcnet'
    ]
)
