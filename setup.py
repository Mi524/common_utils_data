import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="common_utils_data",
    version="0.0.7",
    author="Tracy Tang",
    author_email="tracytang58@icloud.com",
    description="common functions for data processing and database connections",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Mi524/common_utils_pkg",
    project_urls={
        "Bug Tracker": "https://github.com/Mi524/common_utils_pkg/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
      'pandas>=1.2.0',
      'numpy',
      'openpyxl',
      'xlsxwriter',
      'xlwings',
      'html5lib',
      'lxml',
      'sqlalchemy>=1.3.22',
      'pymysql>=1.0.2',
      'mysql-client',
      'flashtext',
      'swifter>=1.0.7',
      'cx_Oracle'
  ],
)