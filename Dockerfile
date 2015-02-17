FROM debian:jessie
MAINTAINER Yung-Chin Oei <yungchin@yungchin.nl>

RUN apt-get -y update

# librdkafka build deps:
RUN apt-get -y install build-essential zlib1g-dev unzip
ADD https://github.com/edenhill/librdkafka/archive/9afc74fb51eb99a3d7dac1613e3cdf21c0a5c972.zip /tmp/source.zip
RUN cd /tmp && \
    unzip source.zip && mv librdkafka-* librdkafka && \
    cd /tmp/librdkafka && \
    ./configure && \
    make all && make install && \
    make clean && ./configure --clean

RUN apt-get -y install python-cffi python-dev
# ^^ python-dev is used by cffi although declared as 'suggests' only
