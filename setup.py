"""Setup configuration for epanet-utils package."""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="epanet-utils",
    version="0.2.0",
    author="NEER",
    author_email="info@neer.ai",
    description="Utilities for interpreting EPA EPANET input (.inp) and report (.rpt) files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/neeraip/epanet-utils",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Hydrology",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.0.0",
        "pyarrow>=10.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ],
        # Producer-side dependencies for emit_results_zarr (used by NEER
        # Console / WRM API). Heavy; not pulled in for plain decode/encode.
        "console": [
            "xarray>=2024.1.0",
            "zarr>=3.0.0",
            "numcodecs>=0.13.0",
            "numpy>=1.24",
        ],
    },
)
