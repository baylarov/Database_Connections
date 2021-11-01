import smtplib,ssl,base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(filename='loglar.log', filemode='a', format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

class Mail:
    def __init__(self,konu,metin):
        self.konu=konu
        self.metin=metin

    @staticmethod
    def __mailContext():
        smtp_server = 'smtp.gmail.com'
        port = 465
        context = ssl.create_default_context()
        cntxt=smtplib.SMTP_SSL(smtp_server, port, context=context)
        return cntxt

    def sendMail(self):
        srvr=self.__mailContext()
        sender = 'senderMail@mail.com'
        receivers = ['receiver_first@mail.com']
        cryptedPsw = 'UmFuZCMA=='.encode('ascii')
        password = base64.b64decode(cryptedPsw).decode('ascii')
        subject = self.konu
        template="Merhaba,\n\nÇalışma esnasında hata alındı. Hata detayı şu şekildedir:\n\n"
        body = template+self.metin

        message = MIMEMultipart()
        message["From"] = sender
        message["To"] = ", ".join(receivers)
        message["Subject"] = subject

        message.attach(MIMEText(body, "plain"))
        text = message.as_string()

        with srvr as server:
            try:
                server.login(sender, password)
                server.sendmail(sender, receivers, text)
            except Exception as exc:
                logging.error(exc)
            finally:
                server.quit()