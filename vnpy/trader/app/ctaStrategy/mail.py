import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

my_sender='trade-msg@yandex.com'
my_server_smtp, port = "smtp.yandex.com",465
my_pass = 'zaq!1234'
# my_receiver='xxxx@xxxx.com'    # 收件人邮箱
# my_context=''     # 邮件正文（可由参数传入）
my_title='策略信息播报'   # 邮件标题
my_name='VNPY_CryptoCurrency'  # 发件人显示名称

# 仅需修改以上配置

def mail(my_receiver,my_context):
    if not my_context:
        print("Please write email context")
        return
    elif not my_receiver:
        print('Please provide receiver\'s email')
        return
    ret=True
    try:
        msg=MIMEText(my_context,'html','utf-8')
        msg['From']=formataddr([my_name,my_sender])
        msg['To']=formataddr(["收件人昵称",my_receiver])
        msg['Subject']=my_title
  
        server=smtplib.SMTP_SSL(my_server_smtp, port)
        server.login(my_sender, my_pass)
        server.sendmail(my_sender,[my_receiver,],msg.as_string())
        server.quit()
        print("Send successfully ...")
    except Exception:
        ret=False
        print("Send email failed ...")
    return ret
