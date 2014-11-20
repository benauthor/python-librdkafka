from setuptools import setup, find_packages

try:
    import rd_kafka.headers
    ext_modules = [rd_kafka.headers.ffi.verifier.get_extension()]
    # TODO include librdkafka itself for bdist?
except ImportError:
    ext_modules = []


setup(
    name="rd_kafka",
    version="0.0.0.dev0",
    description="Python wrapper for the Apache Kafka client-library librdkafka",
    author="Yung-Chin Oei",
    author_email="yungchin@yungchin.nl",
    license="BSD",

    packages=find_packages(exclude=["tests"]),
    install_requires=["cffi>=0.8.6"],
    ext_modules=ext_modules,
    zip_safe=False, # for cffi extension
    )

