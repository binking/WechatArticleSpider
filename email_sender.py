#coding=utf-8
# Exceptions info format:
# 	When raise
#	Relative topic
#	Relative wechat url
#	err_msg: defined by you, used to locate
# 	exc_detail: tarceback.format_exc()

import sys, time
from datetime import datetime as dt
import smtplib
from email.mime.text import MIMEText
reload(sys)
sys.setdefaultencoding('utf-8')


FROM = "jiangzhibin2014.xujie@gmail.com"
TOs = ['jiangzb@heptax.com']
SUBJECT = "Wechat Spider Exceptions Report"

server = smtplib.SMTP("smtp.gmail.com", 587)
server.set_debuglevel(1)
# Erro1: smtplib.SMTPException: SMTP AUTH extension not supported by server.
# Solution:
server.ehlo()
server.starttls()
server.login("jiangzhibin2014.xujie@gmail.com", "jzbwymxjno1_gmail")

# Erro2: smtplib.SMTPAuthenticationError: 
# Solution: start gmail private access
msg = MIMEText('hello, send by Python...', 'plain', 'utf-8')
msg = 'Subject: %s\n\n%s' % (SUBJECT, msg)
server.sendmail(FROM, TOs, msg)

server.quit()
