#Mailing List

import smtplib as smtp
import ssl
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from getpass import getpass

port = 465 #would be 465 to actually send e-mail, 1025 is for debugging server
report_file = '/home/ea/output.xlsx'
text = "Here is a weekly report"

#Prepare report file for e-mailing
report = MIMEBase('application', 'octet-stream')

xls_file = open(report_file, 'rb')
report.set_payload(xls_file.read())
xls_file.close()

encoders.encode_base64(report)
report.add_header(
    'Content-Disposition',
    f"attachment;filename={'output.xlsx'}"
)

# Assemble message object using MIMEMultipart
energize_email = input("Email: ")
report_emails = ["example@gmail.com"]
subject = "Weekly Report for Energize Andover"

message = MIMEMultipart()
message['From'] = energize_email
message['Subject'] = subject

final_text = MIMEText(text, 'plain')
message.attach(final_text)
message.attach(report)
final_msg = message.as_string()

print(type(final_msg))
print(type(energize_email))
password = getpass()
print(type(password))

for report_email in report_emails:
    message['To'] = report_email
    security_context = ssl.create_default_context()
    with smtp.SMTP_SSL('smtp.gmail.com', port, context=security_context) as server:
        print(type(port))
        print(type(security_context))
        print(type(server))
        server.login(energize_email, password)
        server.sendmail(energize_email, report_email, final_msg)
        server.quit()
