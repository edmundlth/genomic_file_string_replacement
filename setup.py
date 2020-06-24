#!/usr/bin/env python

from distutils.core import setup

setup(name="replace_string",
      version="0.0.1",
      author="",
      author_email="",
      packages=["."],
      package_data={},
      entry_points={
          "console_scripts": [
              "replace_string = file_replace_string:main",
          ]
      },
      install_requires=[
          "biopython>=1.73",
          "numpy"
      ],
      )
