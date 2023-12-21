import concurrent.futures
from datetime import datetime
import email
import imaplib
from email.header import decode_header
from email.utils import parsedate_to_datetime
import html2text


def get_unread_emails(imapUserEmail, imapPassword):
    imap_server = "imap.gmail.com"
    imap_port = 993

    imap_conn = imaplib.IMAP4_SSL(imap_server, imap_port)

    imap_conn.login(imapUserEmail, imapPassword)

    imap_conn.select('inbox')

    _, message_ids = imap_conn.search(None, "UNSEEN")

    unread_emails = []

    for message_id in message_ids[0].split():
        try:
            _, data = imap_conn.fetch(message_id, "(RFC822)")
            raw_email = data[0][1]
            email_message = email.message_from_bytes(raw_email)

            subject = email_message["Subject"]
            sender = email.utils.parseaddr(email_message["From"])[1]
            body1 = ""

            # Extract the "Date" header to get the receiving time
            date_received = parsedate_to_datetime(email_message["Date"])

            if email_message.is_multipart():
                for part in email_message.walk():
                    if part.get_content_type() == "text/plain":
                        body1 = part.get_payload(decode=True).decode()
                    elif part.get_content_type() == "text/html":
                        body1 = html2text.html2text(part.get_payload(decode=True).decode())
            else:
                body1 = email_message.get_payload(decode=True).decode()

            email_data = {
                "subject": subject,
                "sender": sender,
                "body": body1,
                "date_received": date_received,
                "email_id": message_id
            }

            unread_emails.append(email_data)
        except Exception as e:
            print(f"Error processing email: {str(e)}")

            imap_conn.store(message_id, '-FLAGS', '(\Seen)')

    # Move these lines outside the for loop
    imap_conn.close()

    return unread_emails


def download_attachments_with_threading(imap_user_email, imap_password, mail_server, email_ids, save_path):

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for email_id in email_ids:
            try:
                with imaplib.IMAP4_SSL(mail_server) as mail:
                    mail.login(imap_user_email, imap_password)
                    mail.select("inbox")

                    _, data = mail.fetch(email_id, "(RFC822)")
                    raw_email = data[0][1]
                    email_message = email.message_from_bytes(raw_email)

                    for part in email_message.walk():
                        if part.get_content_maintype() == 'multipart' or part.get("Content-Disposition") is None:
                            continue
                        filename = part.get_filename()
                        if filename:
                            filename, encoding = decode_header(filename)[0]
                            if isinstance(filename, bytes):
                                filename = filename.decode(encoding or "utf-8")

                            full_path = save_path + filename
                            with open(full_path, "wb") as attachment:
                                attachment.write(part.get_payload(decode=True))


            except Exception as e:
                print(f"Error extracting attachments for email_id {email_id}: {str(e)}")


imapUserEmail = "kiruthika.b@vrdella.com"
imapPassword = "renm kixf nlxy avbx"
mailServer = "imap.gmail.com"
savePath = "C:\\users\\vrdella\\Desktop\\attachment\\"


unread_emails=get_unread_emails(imapUserEmail, imapPassword)
email_ids = [email_data['email_id'] for email_data in unread_emails]
download_attachments_with_threading(imapUserEmail, imapPassword, mailServer, email_ids, savePath)
