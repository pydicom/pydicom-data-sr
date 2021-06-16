
from pathlib import Path
from setuptools import setup, find_packages


PROJECT_DIR = Path(__file__).parent.resolve(strict=True)


def get_version() -> str:
    """Return the package version as a str"""
    VERSION_FILE = PROJECT_DIR / "sr" / "_version.py"
    _globals = {}
    with open(VERSION_FILE) as f:
        exec(f.read(), _globals)

    return _globals["__version__"]


setup(
    name="pydicom-data-sr",
    packages=find_packages(),
    include_package_data=True,
    version=get_version(),
    zip_safe=False,
    description="",
    long_description="",
    long_description_content_type="text/markdown",
    author="scaramallion",
    author_email="scaramallion@users.noreply.github.com",
    url="https://github.com/pydicom/pydicom-sr-data",
    license="MIT",
    keywords="dicom python pydicom sr structuredreports",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "Development Status :: 1 - Planning",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
        "Topic :: Software Development :: Libraries",
    ],
    python_requires=">=3.6",
    setup_requires=["setuptools>=18.0"],
    install_requires=[],
    extras_require={
        "dev": [
            "beautifulsoup4==4.9.3",
            "requests==2.25.1",
            "black==21.6b0",
            "mypy==0.902",
        ]
    },
    entry_points={
        "pydicom.data.external_sources": "pydicom-data-sr = srdata:DataStore",
    },
)
