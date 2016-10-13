from setuptools import setup

setup(
    name='conservationlands',
    version='0.1',
    py_modules=['conservationlands'],
    install_requires=[
        'Click',
        'Requests',
        'Fiona',
        'pgdb',
        'bcdata'
    ],
    entry_points='''
        [console_scripts]
        conservationlands=conservationlands:cli
    '''
)
