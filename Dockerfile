FROM continuumio/anaconda3:5.3.0

WORKDIR /opt/vnpy_fxdayu

RUN apt-get update
RUN apt-get install -y wget git bash build-essential libgl1-mesa-glx

COPY install.sh ./

# ./install.sh --quiet --deps-only
RUN chmod +x install.sh && bash ./install.sh -q -n -d 

COPY requirements.txt ./

RUN conda install -y python-snappy pyqt=5

RUN pip install --no-cache -r requirements.txt

COPY . .

RUN bash ./install.sh -q -n

ENV PYTHONUNBUFFERED=1
# RUN apt-get clean

WORKDIR /root/working
