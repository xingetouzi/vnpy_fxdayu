FROM continuumio/anaconda3:4.3.0

ADD ./sources.list /etc/apt/sources.list
RUN apt-get update 
RUN apt-get install -y tar gcc g++ make python3-dev

ADD . /vnpy_fxdayu
WORKDIR /vnpy_fxdayu
ENV PYTHONPATH=/vnpy_fxdayu:$PYTHONPATH
RUN echo ${INSTALL_CTP:-N} | bash install-docker.sh
