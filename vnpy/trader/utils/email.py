import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from datetime import datetime
from vnpy.trader.vtGlobal import globalSetting

def mail(my_context,strategy):
    mailaccount, mailpass = globalSetting['mailAccount'], globalSetting['mailPass']
    mailserver, mailport = globalSetting['mailServer'], globalSetting['mailPort']
    if "" in [mailaccount,mailpass,mailserver,mailport]:
        return 'Please fill sender\'s mail info in vtSetting.json'

    if strategy.mailAdd:
        if len(strategy.mailAdd)>1:
            to_receiver = strategy.mailAdd[0]
            cc_receiver = strategy.mailAdd[1:len(strategy.mailAdd)]
            cc_receiver = ",".join(cc_receiver)
            my_receiver = ",".join([to_receiver,cc_receiver])
        elif len(strategy.mailAdd)==1:
            to_receiver = my_receiver = strategy.mailAdd[0]
            cc_receiver = ""
    else:
        return "Please fill email address in ctaSetting.json"
    
    if not my_context:
        return "Please write email context"

    ret=True
    try:
        my_context = my_context +"<br><br> from strategy: "+ strategy.name+"<br><br>Good Luck<br>"+ datetime.now().strftime("%Y%m%d %H:%M:%S")
        msg=MIMEText(my_context,'html','utf-8')
        msg['From']=formataddr(['VNPY_CryptoCurrency',mailaccount])
        msg['To']=to_receiver#formataddr(["收件人昵称",to_receiver])
        if cc_receiver:
            msg['Cc']=cc_receiver#formataddr(["CC收件人昵称",cc_receiver])
        msg['Subject'] = '策略信息播报'

        server=smtplib.SMTP_SSL(mailserver, mailport, timeout = 2)
        server.login(mailaccount, mailpass)
        if cc_receiver:
            server.sendmail(mailaccount,[to_receiver,cc_receiver],msg.as_string())
        else:
            server.sendmail(mailaccount,[to_receiver],msg.as_string())
        server.quit()
        msg = "Send email successfully ..."
    except Exception:
        ret=False
        msg = "Send email failed ..."
    return msg