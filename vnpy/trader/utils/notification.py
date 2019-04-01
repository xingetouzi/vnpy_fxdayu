from vnpy.trader.vtGlobal import globalSetting

def notify(msg,strategy):
    ding_key = globalSetting.get("dingding",None)
    mail_acc = globalSetting.get("mailAccount",None)
    if ding_key:
        from vnpy.trader.utils.ding import sendDingDing
        ret = sendDingDing(msg,strategy)
    elif mail_acc:
        from vnpy.trader.utils.email import mail
        ret = mail(msg,strategy)

    return ret