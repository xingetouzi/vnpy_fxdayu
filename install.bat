::Install Python Modules

call pip install msgpack-0.6.1-cp37-cp37m-win_amd64.whl
call pip install TA_Lib-0.4.17-cp37-cp37m-win_amd64.whl
pip install -r requirements.txt -i https://pypi.douban.com/simple/
python setup.py install
pause
::Install Ta-Lib
conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
conda config --set show_channel_urls yes
:: conda install -c quantopian ta-lib=0.4.9
