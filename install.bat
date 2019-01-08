::Install Python Modules

call pip install msgpack-0.5.6-cp36-cp36m-win_amd64.whl
call pip install TA_Lib-0.4.17-cp36-cp36m-win_amd64.whl
call pip install python_snappy-0.5.2-cp36-cp36m-win_amd64.whl
pip install -r requirements.txt -i https://pypi.douban.com/simple/
python setup.py install
pause
::Install Ta-Lib
conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
conda config --set show_channel_urls yes
:: conda install -c quantopian ta-lib=0.4.9
