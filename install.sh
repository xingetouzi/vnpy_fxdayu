#!/bin/bash

#Build ctp/lts/ib api
pushd vnpy/api/ctp
bash build.sh
popd

#pushd vnpy/api/lts
#bash build.sh
#popd

#pushd vnpy/api/xtp
#bash build.sh
#popd

#pushd vnpy/api/ib
#bash build.sh
#popd

#Install Ta-Lib
conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
conda config --set show_channel_urls yes
# conda install -c quantopian ta-lib=0.4.9
python -c "import talib"
if [ $? -nq 0 ]; then
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
    tar -xzf ta-lib-0.4.0-src.tar.gz
    pushd ta-lib/
    ./configure --prefix=/usr
    make
    sudo make install
    pip install ta-lib
    popd
    rm ta-lib-0.4.0-src.tar.gz
    rm -rf ta-lib
fi

#Install Python Modules
pip install -r requirements.txt

#Install vn.py
python setup.py install