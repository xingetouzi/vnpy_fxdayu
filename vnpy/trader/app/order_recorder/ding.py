import json
import requests
from vnpy.trader.vtGlobal import globalSetting
def notify(title, msg):

    token = globalSetting.get("dingding", None)
    if not token:
        return
    url=f'https://oapi.dingtalk.com/robot/send'
    HEADERS={"Content-Type" : "application/json;charset=utf-8"}
    params = {"access_token" : token}
    String_textMsg={
        "msgtype" : "markdown", 
        "markdown": {
                "title": title,
                "text": msg
        }
    }
    String_textMsg=json.dumps(String_textMsg)
    res=requests.post(url, data = String_textMsg, headers = HEADERS, params = params)
    print(res)
    print(res.text)
    """
    {"errmsg":"ok","errcode":0}
    {"errmsg":"send too fast","errcode":130101}
    {"errmsg":"缺少参数 access_token","errcode":40035}
    """