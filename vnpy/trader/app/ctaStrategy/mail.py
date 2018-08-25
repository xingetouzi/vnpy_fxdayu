import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

my_sender='xxxx@xxxx.com'  # 发件人邮箱
my_pass = 'xxxxxxx'  # 发件人邮箱密码
my_receiver='xxxx@xxxx.com'    # 收件人邮箱
# my_context=''     # 邮件正文（可由参数传入）
my_title='策略信息播报'   # 邮件标题
my_name='VNPY_CryptoCurrency'  # 发件人显示名称

# 仅需修改以上配置

def mail(my_context = None):
    ret=True
    try:
        msg=MIMEText(my_context,'html','utf-8')
        msg['From']=formataddr([my_name,my_sender])
        msg['To']=formataddr(["收件人昵称",my_receiver])
        msg['Subject']=my_title
  
        server=smtplib.SMTP_SSL("smtp.qq.com", 465)
        server.login(my_sender, my_pass)
        server.sendmail(my_sender,[my_receiver,],msg.as_string())
        server.quit()
    except Exception:
        ret=False
    return ret
