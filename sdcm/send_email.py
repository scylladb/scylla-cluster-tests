import smtplib
import os.path
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Email(object):
    """
    Responsible for sending emails
    """

    def __init__(self, server='localhost', sender='scylla@scylladb.com', recipients=[]):
        """
        :param server: mail server ip/name
        :param sender: email address to send from
        :param recipients: list of email addresses to send to
        """
        self.server = server
        self.sender = sender
        self.recipients = recipients

    def send(self, subject, content, html=True, files=()):
        """
        :param subject: text
        :param content: text/html
        :param html: True/False
        :param files: paths of the files that will be attached to the email
        :return:
        """
        msg = MIMEMultipart()
        msg['subject'] = subject
        msg['from'] = self.sender
        msg['to'] = ','.join(self.recipients)
        if html:
            text_part = MIMEText(content, "html")
        else:
            text_part = MIMEText(content, "plain")
        msg.attach(text_part)
        for path in files:
            with open(path, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=os.path.basename(path)
                )
            part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(path)
            msg.attach(part)
        ms = smtplib.SMTP(self.server)
        ms.sendmail(self.sender, self.recipients, msg.as_string())
        ms.quit()
