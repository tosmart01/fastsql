import setuptools



setuptools.setup(
    name="fast_sql",
    version="1.3.0",
    author="wuwukai",
    author_email="1286345540@qq.com",
    description="Multithreaded packaging based on python3.6, for fast read read SQL loaded as DataFrame, fast read read SQL written to CSV, Quick read migration table.",
    long_description=open('./README.md','r',encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        "pandas>=0.23.0",
        "DBUtils",
        'tqdm',
        'sqlalchemy',
        'cx_Oracle>=6.0',
        'pymysql',
    ],
    url="https://github.com/tosmart01/fastsql",
    packages=setuptools.find_packages(),

    classifiers=[
       "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    data_files=['README.md']
)
