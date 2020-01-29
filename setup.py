import setuptools

with open('README.md','r') as fp:
    md = fp.read()


setuptools.setup(
    name="fast_sql",
    version="1.0",
    author="wuwukai",
    author_email="1286345540@qq.com",
    description="Multithreaded packaging based on python3.6, for fast read read SQL loaded as DataFrame, fast read read SQL written to CSV, Quick read migration table.",
    long_description=md,
    long_description_content_type="text/markdown",
    install_requires=[
        "pandas>=0.23.0",
        "DBUtils>=1.3",
        'tqdm>=4.29',
        'sqlalchemy==1.2.15',
        'cx_Oracle>=6.0',
        'pymysql>=0.9.2',
    ],
    url="https://github.com/tosmart01/fastsql",
    packages=setuptools.find_packages(),

    classifiers=[
       "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
