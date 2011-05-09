from setuptools import setup, find_packages

setup(name="van.pg",
      version="1.0dev",
      packages=find_packages(),
      namespace_packages=["van"],
      install_requires=["setuptools",
                        'testresources',
                        ],
      include_package_data = True,
      zip_safe = False,
      )
