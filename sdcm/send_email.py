import smtplib
import email.message as email_message


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

    def send(self, subject, content, html=True):
        """
        :param subject: text
        :param content: text/html
        :param html: True/False
        :return:
        """
        msg = email_message.Message()
        msg['subject'] = subject
        msg['from'] = self.sender
        msg['to'] = ','.join(self.recipients)
        if html:
            msg.add_header('Content-Type', 'text/html')
        msg.set_payload(content)

        ms = smtplib.SMTP(self.server)
        ms.sendmail(self.sender, self.recipients, msg.as_string())
        ms.quit()
