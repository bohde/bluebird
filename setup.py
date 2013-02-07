import sys
from setuptools import setup, find_packages

setup(
    name='bluebird',
    version='0.1.0',
    author='Josh Bohde',
    author_email='josh@joshbohde.com',
    description=('bluebird is a client for Kestrel queues',),
    license='BSD',
    packages=['bluebird', 'bluebird.thrift_kestrel'],
    install_requires=[
      'thrift >= 0.9.0',
    ],
    classifiers=[
      "License :: OSI Approved :: BSD License",
    ],
)