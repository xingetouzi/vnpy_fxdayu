FROM continuumio/anaconda3:5.0.0

RUN apt-get update && apt-get install -y tar

ADD . /vnpy_fxdayu
RUN echo ${INSTALL_CTP:-N} | bash install.sh
