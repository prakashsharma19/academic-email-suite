import pandas as pd
import pytz
from io import StringIO
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
import requests
from app import load_config, sanitize_author_name

# Minimal email send functions using config from app

def send_email_via_smtp2go(recipient, subject, body_html, body_text, unsubscribe_link, reply_to=None):
    config = load_config()
    api_url = "https://api.smtp2go.com/v3/email/send"
    data = {
        "api_key": config['smtp2go']['api_key'],
        "sender": f"{config['smtp2go']['sender']}",
        "to": [recipient],
        "subject": subject,
        "text_body": body_text,
        "html_body": body_html,
    }
    if reply_to:
        data['reply_to'] = reply_to
    try:
        result = requests.post(api_url, json=data).json()
        return result.get('data', {}).get('succeeded', 0) == 1
    except Exception:
        return False

def initialize_firebase():
    config = load_config()
    if not firebase_admin._apps:
        cred = credentials.Certificate({
            "type": config['firebase']['type'],
            "project_id": config['firebase']['project_id'],
            "private_key_id": config['firebase']['private_key_id'],
            "private_key": config['firebase']['private_key'],
            "client_email": config['firebase']['client_email'],
            "client_id": config['firebase']['client_id'],
            "auth_uri": config['firebase']['auth_uri'],
            "token_uri": config['firebase']['token_uri'],
            "auth_provider_x509_cert_url": config['firebase']['auth_provider_x509_cert_url'],
            "client_x509_cert_url": config['firebase']['client_x509_cert_url'],
            "universe_domain": config['firebase']['universe_domain'],
        })
        firebase_admin.initialize_app(cred)
    return firestore.client()

def download_from_firebase(filename):
    db = initialize_firebase()
    doc = db.collection("email_files").document(filename).get()
    if doc.exists:
        return doc.to_dict().get("content", "")
    return None

def send_report(journal, count, when):
    subject = "Scheduling Email Report"
    body = f"{journal} = {count} Emails sent successfully time {when}"
    send_email_via_smtp2go("contact@cpsharma.com", subject, body, body, "")

def process_campaign(doc_id, data):
    csv = download_from_firebase(data['recipient_file'])
    if not csv:
        return
    df = pd.read_csv(StringIO(csv))
    success = 0
    unsubscribe_base = "https://pphmjopenaccess.com/unsubscribe?email="
    for _, row in df.iterrows():
        name = sanitize_author_name(str(row.get('name', '')))
        email_content = data['email_body'].replace("$$Author_Name$$", name)
        email_content = email_content.replace("$$Author_Email$$", row.get('email', ''))
        unsubscribe_link = f"{unsubscribe_base}{row.get('email', '')}"
        email_content = email_content.replace("$$Unsubscribe_Link$$", unsubscribe_link)
        plain_text = email_content.replace("<br>", "\n")
        subject = data['email_subjects'][0] if data['email_subjects'] else ""
        if send_email_via_smtp2go(row.get('email', ''), subject, email_content, plain_text, unsubscribe_link):
            success += 1
    db = initialize_firebase()
    db.collection('scheduled_campaigns').document(doc_id).update({
        'status': 'completed',
        'completed_at': datetime.utcnow(),
        'emails_sent': success,
    })
    ist = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%I:%M%p')
    send_report(data['journal_name'], success, ist)

def main():
    db = initialize_firebase()
    now = datetime.utcnow()
    docs = db.collection('scheduled_campaigns').where('status', '==', 'scheduled').where('scheduled_time', '<=', now).stream()
    for doc in docs:
        process_campaign(doc.id, doc.to_dict())

if __name__ == '__main__':
    main()
