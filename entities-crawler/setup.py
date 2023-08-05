# -*- coding: utf-8 -*-
from distutils.core import setup
from setuptools import find_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='entitycrawler',
    version='0.1e',
    author=u'Crawler Inc.',
    author_email='yaroslav.furmuzal17@gmail.com',
    url='',
    license='LICENSE',
    description='Web crawling and entities extractor.',
    long_description=open('README.md').read(),
    packages=find_packages(),
    install_requires=required,
    zip_safe=False,
    include_package_data=True,
)