"""Setup script for StemCenterAnalytics."""
from setuptools import setup, find_packages

setup(
    # ------------------------- Project Implementation Details/Requirements ------------------------
    name="STEM Center Analytics: Backend",
    version="0.1.0",

    packages=find_packages(),  # - or find_packages(where='stem_center_analytics')
    scripts=['\scripts'],
    install_requires=["python>=3.5.2",
                      "cython>=0.25.2",
                      "numpy>=1.11.3"
                      "flask>=0.12.0",
                      "pandas>=0.19.2"],
    package_data={
        'docs': ['*.txt', '*.pdf', '*.rst'],       # documentation in text/restructured text/pdf (s)
        'external_datasets': ['*.json', '*.sql'],
        # external_datasets are stored as sql, csv, and json files
    },

    # -------------------------------- Metadata for upload to PyPI ---------------------------------
    # Application author details:
    author="Jeffrey Persons",
    author_email="jperman8@gmail.com",

    # Application author details:
    # https://github.com/FoothillCollege/StemAnalytics
    description="This is the backend for Foothill STEM Center's STEM Analytics Project.",
    license="TBD",
    keywords="TBD",
    url="http://foothillcollege.github.io/StemAnalytics/",
    long_description="TBD",
    download_url="TBD",
)

# todo: verify setup/install works correctly in multiple envs (+ test scripts & packages)
# attain MIT Open Source License, or similar...
