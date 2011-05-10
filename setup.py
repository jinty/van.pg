import os
from setuptools import setup, find_packages

_here = os.path.dirname(__file__)
README = open(os.path.join(_here, 'van', 'pg', 'README.txt'), 'r').read()

setup(name="van.pg",
      version="1.3",
      description="Tools to programmatically manage PostgreSQL clusters as Python test fixtures.",
      packages=find_packages(),
      long_description=README,
      license='BSD',
      author="Brian Sutherland",
      author_email='brian@vanguardistas.net',
      namespace_packages=["van"],
      install_requires=["setuptools",
                        'testresources',
                        ],
      classifiers=[
          "Development Status :: 4 - Beta",
          "Intended Audience :: Developers",
          "Operating System :: OS Independent",
          "License :: OSI Approved :: BSD License",
          "Topic :: Database",
          "Topic :: Software Development :: Testing",
          "Programming Language :: Python :: 2.5",
          "Programming Language :: Python :: 2.6",
          "Programming Language :: Python :: 2.7",
          ],
      tests_require=['psycopg2',
                     'transaction'],
      test_suite = 'van.pg.tests',
      include_package_data = True,
      zip_safe = False,
      )
