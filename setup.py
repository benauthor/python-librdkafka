from setuptools import setup, find_packages

import rd_kafka.headers


setup(
    name="rd_kafka",
    version="0.0.0.dev0",
    description="Python wrapper for the Apache Kafka client-library librdkafka",
    author="Yung-Chin Oei",
    author_email="yungchin@yungchin.nl",
    license="BSD",

    packages=find_packages(exclude=["tests"]),
    install_requires=["cffi>=0.8.6"],
    ext_modules=[rd_kafka.headers.ffi.verifier.get_extension()],
    zip_safe=False, # for cffi extension
    # TODO include librdkafka itself for bdist?
    )

