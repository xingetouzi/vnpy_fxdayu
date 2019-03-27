#!/bin/bash

function check_result() {
    if [ $? -ne 0 ]; then
        echo " "
        echo "do command failed for $1 !!!"
        echo " "
        exit 1
    fi
}

#Build ctp/lts/ib api
echo "是否要安装'CTP'接口? (Do you need 'CTP' interface?)"
read -p "Enter [y]n: " var1
var1=${var1:-y}
if [ "$var1" = "y" ]; then
	pushd vnpy/api/ctp
	bash build.sh
	popd
fi

#Install Ta-Lib
python -c "import talib"
if [ $? -nq 0 ]; then
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
    tar -xzf ta-lib-0.4.0-src.tar.gz
    pushd ta-lib/
    ./configure --prefix=/usr
    make
    make install
    pip install ta-lib
    popd
    rm ta-lib-0.4.0-src.tar.gz
    rm -rf ta-lib
fi

conda install -y python-snappy

#Install Python Modules
pip install -r requirements.txt

#Install vn.py
# python setup.py install