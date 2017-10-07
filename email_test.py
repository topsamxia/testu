#coding=utf-8
# ##Python语言: Hotmail 发信

from email.mime.text import MIMEText
import smtplib


class Hotmail(object):
    def __init__(self, account, password):

        self.account = "%s@Hotmail.com" % account
        self.password = password


    def send(self, to, title, content):
        print self.account, self.password

        server = smtplib.SMTP('smtp.live.com')
        ## server.set_debuglevel(1)
        server.docmd("EHLO server")
        server.starttls()
        server.login(self.account, self.password)

        msg = MIMEText(content)
        msg['Content-Type'] = 'text/plain; charset="utf-8"'
        msg['Subject'] = title
        msg['From'] = self.account
        msg['To'] = to
        server.sendmail(self.account, to, msg.as_string())
        server.close()


class QQMail(object):
    def __init__(self, account, password):

        self.account = "%s@qq.com" % account
        self.password = password


    def send(self, to, title, content):
        """
        send('zsp007@Hotmail.com,zsp747@Hotmail.com")
        """

        print self.account, self.password
        server = smtplib.SMTP('smtp.qq.com')
        server.set_debuglevel(1)
        ## server.docmd("EHLO server" )
        ## server.starttls()
        server.login(self.account, self.password)

        msg = MIMEText(content)
        msg['Content-Type'] = 'text/plain; charset="utf-8"'
        msg['Subject'] = title
        msg['From'] = self.account
        msg['To'] = to
        server.sendmail(self.account, to, msg.as_string())
        server.close()

if __name__ == "__main__":
    Hotmail = QQMail("xzhenxin@hotmail.com", "justsoso757314")
    Hotmail.send("codemo@126.com", "测试ok", "content")






















# from email.mime.multipart import multipart
#
# from email.mime.text import
# from email.MIMEImage import MIMEImage
# msg = multipart()
# msg.attach(MIMEText(file("C:\\Users\\samx\\PycharmProjects\\testu\\hist_sample\\SZ300191.csv").read()))
#
#
# import smtplib
# fromaddr = 'myemail@gmail.com'
# toaddrs  = 'youremail@gmail.com'
# msg = 'Hello'
#
# username = 'myemail'
# password = 'yyyyyy'
#
# mailer = smtplib.SMTP('smtp.gmail.com:587')
# mailer.connect()
# mailer.sendmail(fromaddr, toaddrs, msg.as_string())
# mailer.close()
#
# import smtplib
# #SERVER = "localhost"
#
# FROM = 'xzhenxin@hotmail.com'
#
# TO = ["xzhenxin@hotmail.com"] # must be a list
#
# SUBJECT = "Hello!"
#
# TEXT = "This message was sent with Python's smtplib."
#
# # Prepare actual message
#
# message = """\
# From: %s
# To: %s
# Subject: %s
#
# %s
# """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
#
# # Send the mail
#
# server = smtplib.SMTP('myserver')
# server.sendmail(FROM, TO, message)
# server.quit()

