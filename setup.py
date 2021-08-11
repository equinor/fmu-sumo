#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="fmu-sumo",
    description="Python package for interacting with Sumo in an FMU setting",
    url="https://github.com/equinor/fmu-sumo",
    version="0.1",
    author="Per Olav Svendsen",
    license="GPLv3",
    keywords="fmu, sumo",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    author_email="peesv@equinor.com",
    install_requires=[
        "PyYAML",
        "pandas",
        "sumo-wrapper-python",
        "setuptools",
    ],
    python_requires=">=3.4",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=False,
)
