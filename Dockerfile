FROM continuumio/anaconda3:5.0.0

ADD ./sources.list /etc/apt/sources.list
RUN apt-get update 
RUN apt-get install -y tar

ADD . /vnpy_fxdayu
WORKDIR /vnpy_fxdayu
RUN conda install -y python-snappy
RUN echo ${INSTALL_CTP:-N} | bash install.sh
