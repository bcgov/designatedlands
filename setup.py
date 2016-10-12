from setuptools import setup

setup(
    name='conservationlands',
    version='0.1',
    py_modules=['conservationlands'],
    install_requires=[
        'Click',
        'Requests',
        'Fiona',
        'git+git://github.com/smnorris/pgdb.git',
        'git+git://github.com/smnorris/bcdata.git',
    ],
    entry_points='''
        [console_scripts]
        conservationlands=conservationlands:cli
    ''',
)
