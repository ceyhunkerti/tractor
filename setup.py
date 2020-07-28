import pathlib
from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="tractor",
    version="0.0.21",
    description="Cross platform data transfer utility",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/bluecolor/tractor",
    author="ceyhun kerti",
    author_email="ceyhun.kerti@bluecolor.io",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    install_requires=[
        "click",
        "funcy",
        "PyYAML",
        "tqdm",
        "yaspin",
        "pathlib"
    ],
    setup_requires=['wheel'],
    entry_points={"console_scripts": ["tractor=tractor.__main__:tractor",]},
)
