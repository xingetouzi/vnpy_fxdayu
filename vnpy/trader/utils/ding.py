import json
import requests
from vnpy.trader.vtGlobal import globalSetting
from datetime import datetime
from .email import mail

def sendDingDing(msg, strategy):

    url=f'https://oapi.dingtalk.com/robot/send?access_token={globalSetting["dingding"]}'
    HEADERS={"Content-Type":"application/json;charset=utf-8"}

    msg += f'<br><br> from strategy:{strategy.name}<br><br> Good Luck<br> datetime.now().strftime("%Y%m%d %H:%M:%S")'

    String_textMsg={"msgtype":"text","text":{"content":msg}}
    String_textMsg=json.dumps(String_textMsg)
    res=requests.post(url,data=String_textMsg,headers=HEADERS)

    """{"errmsg":"ok","errcode":0}"""
    errcode = res.text.get("errcode","error")
    if errcode:
        content = f'dingding error: {res.text}, source_msg:{msg}'
        mail(content,strategy)