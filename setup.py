from setuptools import setup, find_packages
import codecs
import os

VERSION = '0.0.3'
DESCRIPTION = 'PySpark utility created to quickly provide details regarding which attributes differ between 2 dataframes with same schema and primary key.'
requires = [
    'pyspark>=3.0.2', 'tabulate>=0.8.9'
]
# Setting up
setup(
    name="pyspark-datacol-diff",
    version=VERSION,
    author="Jasjyot Singh Jaswal",
    author_email="<jasjyot_singh_jaswal@yahoo.com>",
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=requires,
    url="https://github.com/jasjyotsinghjaswal/pyspark-datacol-diff",
    license="MIT",
    keywords=['data diff', 'data analysis', 'record comparison', 'data difference', 'row comparison',
              'compare records'],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
