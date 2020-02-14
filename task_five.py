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
report_1_file = '/home/ea/high_co2.xlsx'
report_2_file = '/home/ea/low_co2.xlsx'
report_3_file = '/home/ea/warm.xlsx'
report_4_file = '/home/ea/cold.xlsx'

text = "Here is a weekly report"

file_stuff = [report_1_file, report_2_file, report_3_file, report_4_file]
formal_file_names = ['high_co2.xlsx', 'low_co2.xlsx', 'warm.xlsx', 'cold.xlsx']

#Prepare report file for e-mailing
report1 = MIMEBase('application', 'octet-stream')
report2 = MIMEBase('application', 'octet-stream')
report3 = MIMEBase('application', 'octet-stream')
report4 = MIMEBase('application', 'octet-stream')

reports = [report1, report2, report3, report4]

for i in range (0, 4):
    report_file = file_stuff[i]
    report = reports[i]
    xls_file = open(report_file, 'rb')
    report.set_payload(xls_file.read())
    xls_file.close()

for i in range(0, 4):
    encoders.encode_base64(report)
    file_name = formal_file_names[i]
    report.add_header(
        'Content-Disposition',
        f"attachment;filename={file_name}"
    )

# Assemble message object using MIMEMultipart
energize_email = input("Email: ")
report_emails = ["jnair2022@k12.andoverma.us"]
subject = "Weekly Report for Energize Andover"

message = MIMEMultipart()
message['From'] = energize_email
message['Subject'] = subject

final_text = MIMEText(text, 'plain')
message.attach(final_text)
for report in reports:
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
