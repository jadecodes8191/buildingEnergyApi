#Mailing List

import smtplib as smtp
import ssl
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

port = 1025 #would be 465 to actually send e-mail, 1025 is for debugging server
report_file = 'output.xlsx'
text = "Here is a weekly report"

#Prepare report file for e-mailing
report = MIMEApplication('application', 'vnd.ms-excel')

xls_file = open(report_file, 'rb')
report.set_payload(xls_file.read())
xls_file.close()

encoders.encode_base64(report)
report.add_header(
    'Content-Disposition',
    f"attachment;filename={report_file}"
)

# Assemble message object using MIMEMultipart
energize_email = ''
report_emails = ''
subject = "Weekly Report for Energize Andover"

message = MIMEMultipart()
message['To'] = report_emails
message['From'] = energize_email
message['Subject'] = subject

final_text = MIMEText(text, 'plain')
message.attach(final_text)
message.attach(report)
final_msg = message.as_string()

password = input('Enter password: ')
security_context = ssl.create_default_context()
with smtp.SMTP_SSL('smtp.gmail.com', port, context=security_context) as server:
    server.login(energize_email, password)
    server.sendmail(energize_email, report_emails, final_msg)
    server.quit()
