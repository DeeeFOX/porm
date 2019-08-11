import re
from collections import OrderedDict
import io

from setuptools import setup, find_packages

from os import path

this_directory = path.abspath(path.dirname(__file__))
with io.open(path.join(this_directory, "README.md"), "rt", encoding="utf-8") as fd:
    readme = fd.read()

with io.open("src/porm/__init__.py", "rt", encoding="utf-8") as f:
    version = re.search(r"__version__ = \"(.*?)\"", f.read()).group(1)

reqs = [
    'pymysql==0.9.3',
    'python-dateutil==2.8.0'
]

setup(
    name="Porm",
    version=version,
    url="https://github.com/DeeeFOX/porm",
    project_urls=OrderedDict(
        (
            ("Documentation", "https://github.com/DeeeFOX/porm"),
            ("Code", "https://github.com/DeeeFOX/porm"),
            ("Issue tracker", "https://github.com/DeeeFOX/porm/issues"),
        )
    ),
    license="BSD",
    maintainer="porm team",
    maintainer_email="dennias.chiu@gmail.com",
    author="deefox",
    author_email="602716933@qq.com",
    description=(
        "A flexible forms validation and rendering library for Python"
        " web development"
        " that inherited and learned from Peewee."
    ),
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=reqs
)
