from setuptools import setup

setup(
    name='conservationlands',
    version='0.1',
    py_modules=['conservationlands'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        conservationlands=conservationlands:cli
    ''',
)
