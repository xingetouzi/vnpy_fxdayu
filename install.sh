#!/bin/bash
set -ex

function check_result() {
    if [ $? -ne 0 ]; then
        echo " "
        echo "do command failed for $1 !!!"
        echo " "
        exit 1
    fi
}

RUN_QUIETLY=0
BUILD_CTP=0
INSTALL_TALIB=0
DEPS_ONLY=0
NO_CACHE=0

while getopts "qctdn" opt; do
  case $opt in
    q)
      RUN_QUIETLY=1
      ;;
    t)
      INSTALL_TALIB=1
      ;;
    c)
      BUILD_CTP=1
      ;;
    d)
      DEPS_ONLY=1
      ;;
    n)
      NO_CACHE=1
      ;;
    \?)
      echo "Invalid option: -$OPTARG" 
      ;;
  esac
done

function build_ctp() {
    pushd vnpy/api/ctp
	bash build.sh
	popd
}

function install_talib() {
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
    tar -xzf ta-lib-0.4.0-src.tar.gz
    pushd ta-lib/
    ./configure --prefix=/usr
    make
    make install
    if [ $NO_CACHE -eq 0 ]; then
        pip install ta-lib
    else
        pip install --no-cache ta-lib
    fi
    popd
    rm ta-lib-0.4.0-src.tar.gz
    rm -rf ta-lib
}

function query_build_ctp() {
    echo "是否要安装'CTP'接口? (Do you need 'CTP' interface?)"
    read -p "Enter [y]n: " var1
    var1=${var1:-y}
    if [ "$var1" = "y" ]; then
        BUILD_CTP=1
    fi
}

function query_install_talib() {
    set +ex
    python -c "import talib"
    if [ $? -ne 0 ]; then
        INSTALL_TALIB=1
    fi
    set -ex
}

function main() {
    if [ $RUN_QUIETLY -eq 0 ]; then    
        query_build_ctp;
    fi
    query_install_talib;
    if [ $BUILD_CTP -ne 0 ]; then
        build_ctp;
    fi

    if [ $INSTALL_TALIB -ne 0 ]; then
        install_talib;
    fi

    if [ $DEPS_ONLY -eq 0 ]; then
        #Install Python Modules
        if [ $NO_CACHE -eq 0 ]; then
            pip install -r requirements.txt
        else
            pip install --no-cache -r requirements.txt
        fi
        #Install vn.py
        python setup.py install
    fi
}

main;