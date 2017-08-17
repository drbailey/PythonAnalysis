__author__  = 'drew bailey'
__version__ = 2.0


"""
Email distribution package. Uses SMTP sends with HTML embedded messages.
"""


from ...loggers import rLog
from ...util import ambiguous_extension
from email.mime.text import MIMEText
from email.MIMEBase import MIMEBase  # not in email __init__ but works fine.
from email.MIMEImage import MIMEImage  # not in email __init__ but works fine.
from email import Encoders
import smtplib
import os


class Eml(object):
    """
    Eml class uses email and smtplib packages to format email messages to users using smtp server.

    Required Fields:
        sender:     an email address as a string.
        subject:    email subject as a string.
        message:    email body as a string, usually multi-line. HTML allowed.

    Semi-Optional Fields*:
        to:         a list of email addresses as strings to be added to the message's 'To' field.
        cc:         a list of email addresses as strings to be added to the message's 'CC' field.
        bcc:        a list of email addresses as strings to be added to the message's 'BCC' field.
        recipients: a list of email addresses as strings that the message will be delivered to. if no recipients are
                        provided it will be sent to all of to, cc, and bcc targets.**
        *   please note: one of 'to', 'cc', 'bcc', or 'recipients' fields must have at least one entry.
        **  only use the recipient field when it is desired to have a message delivered to a different group than is
                displayed!

    Optional Fields:
        directory:  an iterable object containing a list of file locations to attach as strings.
        smtp_server:smtp server address as a string.
        smtp_port:  smtp port as an integer.
        password:   smtp authentication password as string.
        path:       a path, as a string, to the database where log files are held if they are not in the cwd.
                        if log files are in cwd no path is required.
        embed:      if html image embedding is required include image PATH here.
        embedh:     header text as html for image.
    """
    def __init__(self,
                 sender,
                 subject,
                 message,

                 to=None,
                 cc=None,
                 bcc=None,
                 recipients=None,

                 files=None,
                 smtp_server=None,
                 smtp_port=None,
                 path=None,
                 embed=None,
                 embedh=None,
                 password=None,

                 parse_sender=True,
                 ):
        self.sender = sender
        if not embedh and embed:
            self.embedh = '<b>An <i>HTML</i> header</b> and image.'
        else:
            self.embedh = embedh
        self.embed = embed
        self.subject = subject
        self.message = message
        self.files = files
        self.SMTP_SERVER = smtp_server
        self.SMTP_PORT = smtp_port
        self.password = password
        self.to = []
        if to:
            self.to = to
        self.cc = []
        if cc:
            self.cc = cc
        self.bcc = []
        if bcc:
            self.bcc = bcc
        self.recipients = recipients
        if not self.recipients:
            self.recipients = self.to + self.cc + self.bcc
        if path:
            self.path = path
        else:
            self.path = os.getcwd()
        self.embed = embed

        self.msg = None

        self.parse_sender = parse_sender
        # print sender,recipient,subject,message,directory,smtp_server,smtp_port,cc,password
    # creates a msg object

    def construct(self):
        """ Constructs email message from parts using email.mime.MIMEMultipart."""
        from email.mime.multipart import MIMEMultipart
        self.msg = MIMEMultipart()

        self.msg['To'] = ', '.join(self.to)
        self.msg['CC'] = ', '.join(self.cc)
        self.msg['BCC'] = ', '.join(self.bcc)

        if self.parse_sender:
            self.msg['From'] = self.parse_from(sender=self.sender)
        else:
            self.msg['From'] = self.sender

        self.msg['Subject'] = self.subject

    @staticmethod
    def parse_from(sender):
        name = sender.split('@')[0]
        name = ''.join([n for n in name if not n.isdigit()])
        # first = name.split('.')[0]
        # last = name.split('.')[-1]
        return ' '.join(name.split('.'))

    def attach(self):
        """ Attaches file(s) from cwd. Can embed html image with header. Runs on default email package. """
        # ## EMBED HTML IMAGE
        if self.embed:
            body_text = '%s<br><img src="cid:%s"><br>%s' % (self.embedh, self.embed, self.message)
            self.part = MIMEText(body_text, 'html')
            self.msg.attach(self.part)
            fp = open(self.embed, 'rb')
            img = MIMEImage(fp.read())
            fp.close()
            img.add_header('Content-ID', self.embed)
            self.msg.attach(img)
        # ## OR DON'T ...
        else: 
            self.part = MIMEText(self.message, "html")
            self.msg.attach(self.part)

        if self.files:
            for f in self.files:
                if f.strip().endswith('*'):
                    f = ambiguous_extension(f)
                    # print d_
                self.part = MIMEBase('application', "octet-stream")
                self.part.set_payload(open(f, "rb").read())
                Encoders.encode_base64(self.part)
                self.part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
                self.msg.attach(self.part)

    def send(self):
        """
        Uses smtplib package to connect to provided smtp address and port, then send the message.
        Supports smtp password authentication.
        Logs sender, recipient, and attachment.
        """
        session = smtplib.SMTP(self.SMTP_SERVER, self.SMTP_PORT)
        # the smtplib error is not very descriptive: "SMTPServerDisconnected: please run connect() first"
        if not self.SMTP_SERVER:
            raise smtplib.SMTPConnectError(code='', msg="No SMTP Server provided.")
        # allows for password on smtp
        if self.password:
            session.ehlo()
            session.starttls()
            session.ehlo()
            session.login(self.sender, self.password)
        session.sendmail(from_addr=self.sender, to_addrs=self.recipients, msg=self.msg.as_string())
        logstr = """Email Sent: to -%s, from -%s, attached -%s.""" % (self.recipients, self.sender, self.files)
        logstr = logstr.replace('\\', '').replace("'", '')
        rLog.info(logstr)
        session.quit()

    def auto(self):
        """
        Constructs, attaches, and sends an email message from initialization parameters.
        :return:
        """
        self.construct()
        self.attach()
        self.send()
