from setuptools import setup, find_packages

setup(
    name="esource_pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.2.0",
    ],
)
