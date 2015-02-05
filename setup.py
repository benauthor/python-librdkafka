from setuptools import setup, find_packages

setup(
    name="rd_kafka",
    version="0.0.0.dev1",
    description="Python wrapper for the Apache Kafka client-library librdkafka",
    url="https://bitbucket.org/yungchin/python-librdkafka",
    author="Yung-Chin Oei",
    author_email="yungchin@yungchin.nl",
    license="BSD",

    packages=find_packages(),
    install_requires=["cffi>=0.8.6"],
    zip_safe=False, # for cffi extension
    )
