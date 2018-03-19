import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


# Find version number in module's __init__.py
with open('designatedlands/__init__.py', 'r') as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            break

setup(name='designatedlands',
      version=version,
      description=u"BC land designations that contribute to conservation",
      long_description=read('README.md'),
      classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3.6'
      ],
      keywords='"British Columbia" conservation designated lands',
      author=u"Simon Norris",
      author_email='snorris@hillcrestgeo.ca',
      url='https://github.com/bcgov/designatedlands',
      license='Apache',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=read('requirements.txt').splitlines(),
      extras_require={
        'test': ['pytest', 'coverage']},
      entry_points="""
      [console_scripts]
      designatedlands=designatedlands.main:cli
      """
      )
