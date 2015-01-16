FROM debian:jessie
MAINTAINER Yung-Chin Oei <yungchin@yungchin.nl>

RUN apt-get -y update && \
    apt-get -y install librdkafka-dev python-cffi gcc python-dev \
                       python-pip python-setuptools ipython

RUN ln -s /srv/python-librdkafka/.ipython /root/.ipython
