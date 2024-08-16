from setuptools import find_packages, setup

setup(
    name="mflix_pipelines",
    packages=find_packages(exclude=["mflix_pipelines_tests"]),
    install_requires=[
        "dagster==1.7.7",
        "dagster-cloud==1.7.7",
        "dagster-snowflake==0.23.7",
        "pymongo>=4.3.3",
        "dlt[snowflake]>=0.3.5",
        "scikit-learn==1.5.0",
        "dagster-embedded-elt==0.23.7",
        "",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
