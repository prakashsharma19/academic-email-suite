import streamlit as st
import boto3
import pandas as pd
import datetime
import time
import requests
import json
import os
import pytz
from datetime import datetime, timedelta
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account
from streamlit_ace import st_ace
import firebase_admin
from firebase_admin import credentials, firestore
import matplotlib.pyplot as plt

import threading

# App Configuration
st.set_page_config(
    page_title="PPH Email Manager", 
    layout="wide",
    page_icon="📧",
    menu_items={
        'About': "### Academic Email Management Suite\n\nDeveloped by Prakash (contact@cpsharma.com)"
    }
)

# Light Theme with Footer
def set_light_theme():
    light_theme = """
    <style>
    :root {
        --primary-color: #4a8af4;
        --background-color: #ffffff;
        --secondary-background-color: #f0f2f6;
        --text-color: #31333f;
        --font: sans-serif;
    }
    .stApp {
        background-color: var(--background-color);
    }
    .css-1d391kg, .css-1y4p8pa {
        background-color: var(--secondary-background-color);
    }
    .css-1aumxhk {
        color: var(--text-color);
    }
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: #f0f2f6;
        color: #31333f;
        text-align: center;
        padding: 10px;
        font-size: 0.8em;
        border-top: 1px solid #ddd;
    }
    .footer a {
        color: #4a8af4;
        text-decoration: none;
        font-weight: bold;
    }
    /* Button Colors */
    .stDownloadButton button {
        border-color: #4CAF50 !important;
        color: white !important;
        background-color: #4CAF50 !important;
    }
    .stDownloadButton button:hover {
        background-color: #45a049 !important;
    }
    .bad-email-btn button {
        background-color: #f44336 !important;
        border-color: #f44336 !important;
    }
    .bad-email-btn button:hover {
        background-color: #d32f2f !important;
    }
    .good-email-btn button {
        background-color: #4CAF50 !important;
        border-color: #4CAF50 !important;
    }
    .good-email-btn button:hover {
        background-color: #388E3C !important;
    }
    .risky-email-btn button {
        background-color: #9C27B0 !important;
        border-color: #9C27B0 !important;
    }
    .risky-email-btn button:hover {
        background-color: #7B1FA2 !important;
    }
    </style>
    <div class="footer">
        This app is made by <a href="https://www.cpsharma.com" target="_blank">Prakash</a>. 
        Contact for any help at <a href="https://www.cpsharma.com" target="_blank">cpsharma.com</a>
    </div>
    """
    st.markdown(light_theme, unsafe_allow_html=True)

set_light_theme()

# Authentication System
def check_auth():
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.title("PPH Email Manager - Login")
        
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.form_submit_button("Login"):
                if username == "admin" and password == "prakash123@":
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Invalid credentials")
        
        st.stop()
    
    if st.sidebar.button("Logout"):
        st.session_state.authenticated = False
        st.rerun()

# Initialize session state
def init_session_state():
    if 'ses_client' not in st.session_state:
        st.session_state.ses_client = None
    if 'firebase_storage' not in st.session_state:
        st.session_state.firebase_storage = None
    if 'firebase_initialized' not in st.session_state:
        st.session_state.firebase_initialized = False
    if 'selected_journal' not in st.session_state:
        st.session_state.selected_journal = None
    if 'email_service' not in st.session_state:
        st.session_state.email_service = "SMTP2GO"
    if 'campaign_history' not in st.session_state:
        st.session_state.campaign_history = []
    if 'journal_reply_addresses' not in st.session_state:
        st.session_state.journal_reply_addresses = {}
    if 'verified_emails' not in st.session_state:
        st.session_state.verified_emails = pd.DataFrame()
    if 'verified_txt_content' not in st.session_state:
        st.session_state.verified_txt_content = ""
    if 'firebase_files' not in st.session_state:
        st.session_state.firebase_files = []
    if 'firebase_files_verification' not in st.session_state:
        st.session_state.firebase_files_verification = []
    if 'current_verification_list' not in st.session_state:
        st.session_state.current_verification_list = None
    if 'verification_stats' not in st.session_state:
        st.session_state.verification_stats = {}
    if 'verification_start_time' not in st.session_state:
        st.session_state.verification_start_time = None
    if 'verification_progress' not in st.session_state:
        st.session_state.verification_progress = 0
    if 'active_campaign' not in st.session_state:
        st.session_state.active_campaign = None
    if 'campaign_paused' not in st.session_state:
        st.session_state.campaign_paused = False
    if 'campaign_cancelled' not in st.session_state:
        st.session_state.campaign_cancelled = False
    if 'template_content' not in st.session_state:
        st.session_state.template_content = {}
    if 'journal_subjects' not in st.session_state:
        st.session_state.journal_subjects = {}
    if 'sender_email' not in st.session_state:
        st.session_state.sender_email = config['smtp2go']['sender']

# Journal Data
JOURNALS = [
    "Computer Science and Artificial Intelligence",
    "Advanced Studies in Artificial Intelligence",
    "Advances in Computer Science and Engineering",
    "Far East Journal of Experimental and Theoretical Artificial Intelligence",
    "Advances and Applications in Fluid Mechanics",
    "Advances in Fuzzy Sets and Systems",
    "Far East Journal of Electronics and Communications",
    "Far East Journal of Mathematical Education",
    "Far East Journal of Mechanical Engineering and Physics",
    "International Journal of Nutrition and Dietetics",
    "International Journal of Numerical Methods and Applications",
    "International Journal of Materials Engineering and Technology",
    "JP Journal of Solids and Structures",
    "Advances and Applications in Discrete Mathematics",
    "Advances and Applications in Statistics",
    "Far East Journal of Applied Mathematics",
    "Far East Journal of Dynamical Systems",
    "Far East Journal of Mathematical Sciences (FJMS)",
    "Far East Journal of Theoretical Statistics",
    "JP Journal of Algebra, Number Theory and Applications",
    "JP Journal of Geometry and Topology",
    "JP Journal of Biostatistics",
    "JP Journal of Fixed Point Theory and Applications",
    "JP Journal of Heat and Mass Transfer",
    "Surveys in Mathematics and Mathematical Sciences",
    "Universal Journal of Mathematics and Mathematical Sciences"
]

# Default email template
def get_journal_template(journal_name):
    # Check if we have a saved template in session state
    if journal_name in st.session_state.template_content:
        return st.session_state.template_content[journal_name]
    
    # Otherwise return default template
    return f"""<div style="font-family: Arial, sans-serif; line-height: 1.6; max-width: 600px; margin: 0 auto;">
    <div style="margin-bottom: 20px;">
        <p>To</p>
        <p>$$Author_Name$$<br>
        $$Author_Address$$</p>
    </div>
    
    <p>Dear $$Author_Name$$,</p>
    
    <p>We are pleased to invite you to submit your research work to <strong>{journal_name}</strong>.</p>
    
    <p>Your recent work in $$Department$$ at $$University$$, $$Country$$ aligns well with our journal's scope.</p>
    
    <h3 style="color: #2a6496;">Important Dates:</h3>
    <ul>
        <li>Submission Deadline: [Date]</li>
        <li>Notification of Acceptance: [Date]</li>
        <li>Publication Date: [Date]</li>
    </ul>
    
    <p>For submission guidelines, please visit our website: <a href="[Journal Website]">[Journal Website]</a></p>
    
    <p>We look forward to your valuable contribution.</p>
    
    <p>Best regards,<br>
    Editorial Team<br>
    {journal_name}</p>
    
    <div style="margin-top: 30px; font-size: 0.8em; color: #666;">
        <p>If you no longer wish to receive these emails, please <a href="$$Unsubscribe_Link$$">unsubscribe here</a>.</p>
    </div>
</div>"""

# Load configuration from environment variables
@st.cache_data
def load_config():
    config = {
        'aws': {
            'access_key': os.getenv("AWS_ACCESS_KEY_ID", ""),
            'secret_key': os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            'region': os.getenv("AWS_REGION", "us-east-1")
        },
        'millionverifier': {
            'api_key': os.getenv("MILLIONVERIFIER_API_KEY", "")
        },
        'firebase': {
            'type': os.getenv("FIREBASE_TYPE", ""),
            'project_id': os.getenv("FIREBASE_PROJECT_ID", ""),
            'private_key_id': os.getenv("FIREBASE_PRIVATE_KEY_ID", ""),
            'private_key': os.getenv("FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n'),
            'client_email': os.getenv("FIREBASE_CLIENT_EMAIL", ""),
            'client_id': os.getenv("FIREBASE_CLIENT_ID", ""),
            'auth_uri': os.getenv("FIREBASE_AUTH_URI", ""),
            'token_uri': os.getenv("FIREBASE_TOKEN_URI", ""),
            'auth_provider_x509_cert_url': os.getenv("FIREBASE_AUTH_PROVIDER_CERT_URL", ""),
            'client_x509_cert_url': os.getenv("FIREBASE_CLIENT_CERT_URL", ""),
            'universe_domain': os.getenv("FIREBASE_UNIVERSE_DOMAIN", "googleapis.com")
        },
        'smtp2go': {
            'api_key': os.getenv("SMTP2GO_API_KEY", ""),
            'sender': os.getenv("SMTP2GO_SENDER_EMAIL", "noreply@cpsharma.com"),
            'template_id': os.getenv("SMTP2GO_TEMPLATE_ID", "")
        },
        'webhook': {
            'url': os.getenv("WEBHOOK_URL", "")
        }
    }
    return config

config = load_config()
init_session_state()

# Initialize Firebase with better error handling
def initialize_firebase():
    try:
        if firebase_admin._apps:
            st.session_state.firebase_initialized = True
            return True
            
        # Ensure private key is properly formatted
        private_key = config['firebase']['private_key']
        if not private_key.startswith('-----BEGIN PRIVATE KEY-----'):
            private_key = '-----BEGIN PRIVATE KEY-----\n' + private_key + '\n-----END PRIVATE KEY-----'

        cred = credentials.Certificate({
            "type": config['firebase']['type'],
            "project_id": config['firebase']['project_id'],
            "private_key_id": config['firebase']['private_key_id'],
            "private_key": private_key,
            "client_email": config['firebase']['client_email'],
            "client_id": config['firebase']['client_id'],
            "auth_uri": config['firebase']['auth_uri'],
            "token_uri": config['firebase']['token_uri'],
            "auth_provider_x509_cert_url": config['firebase']['auth_provider_x509_cert_url'],
            "client_x509_cert_url": config['firebase']['client_x509_cert_url'],
            "universe_domain": config['firebase']['universe_domain']
        })
        
        firebase_admin.initialize_app(cred)
        st.session_state.firebase_initialized = True
        return True
    except Exception as e:
        st.error(f"Firebase initialization failed: {str(e)}")
        return False

def get_firestore_db():
    if not initialize_firebase():
        st.error("Firebase initialization failed")
        return None
    return firestore.client()

# Initialize SES Client with better error handling
def initialize_ses():
    try:
        if not config['aws']['access_key'] or not config['aws']['secret_key']:
            st.error("AWS credentials not configured")
            return None
            
        ses_client = boto3.client(
            'ses',
            aws_access_key_id=config['aws']['access_key'],
            aws_secret_access_key=config['aws']['secret_key'],
            region_name=config['aws']['region']
        )
        st.session_state.ses_client = ses_client
        return ses_client
    except Exception as e:
        st.error(f"SES initialization failed: {str(e)}")
        return None

# SMTP2GO Email Sending Function - Updated from working code
def send_email_via_smtp2go(recipient, subject, body_html, body_text, unsubscribe_link, reply_to=None):
    try:
        api_url = "https://api.smtp2go.com/v3/email/send"
        
        headers = {
            "List-Unsubscribe": f"<{unsubscribe_link}>",
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"
        }
        
        data = {
            "api_key": config['smtp2go']['api_key'],
            "sender": config['smtp2go']['sender'],
            "to": [recipient],
            "subject": subject,
            "text_body": body_text,
            "html_body": body_html,
            "custom_headers": [
                {
                    "header": "List-Unsubscribe",
                    "value": f"<{unsubscribe_link}>"
                },
                {
                    "header": "List-Unsubscribe-Post",
                    "value": "List-Unsubscribe=One-Click"
                }
            ]
        }
        
        if reply_to:
            data['reply_to'] = reply_to
        
        response = requests.post(api_url, json=data)
        result = response.json()
        
        if result.get('data', {}).get('succeeded', 0) == 1:
            return True, result.get('data', {}).get('email_id', '')
        else:
            st.error(f"SMTP2GO Error: {result.get('error', 'Unknown error')}")
            return False, None
    except Exception as e:
        st.error(f"Failed to send email via SMTP2GO: {str(e)}")
        return False, None

def send_ses_email(ses_client, sender, recipient, subject, body_html, body_text, unsubscribe_link, reply_to=None):
    try:
        if not ses_client:
            st.error("SES client not initialized")
            return None, None
            
        message = {
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': body_text,
                    'Charset': 'UTF-8'
                },
                'Html': {
                    'Data': body_html,
                    'Charset': 'UTF-8'
                }
            }
        }
        
        if reply_to:
            message['ReplyToAddresses'] = [reply_to]
        
        response = ses_client.send_email(
            Source=sender,
            Destination={
                'ToAddresses': [recipient],
            },
            Message=message,
            Tags=[{
                'Name': 'unsubscribe',
                'Value': unsubscribe_link
            }]
        )
        return response, response['MessageId']
    except Exception as e:
        st.error(f"Failed to send email: {str(e)}")
        return None, None

# Verification Functions
def verify_email(email, api_key):
    url = f"https://api.millionverifier.com/api/v3/?api={api_key}&email={email}"
    try:
        response = requests.get(url)
        data = response.json()
        return data
    except Exception as e:
        st.error(f"Verification failed: {str(e)}")
        return None

def check_millionverifier_quota(api_key):
    """Check actual remaining credits using MillionVerifier's official API"""
    url = f"https://api.millionverifier.com/api/v3/credits?api={api_key}"
    try:
        response = requests.get(url)
        data = response.json()
        if 'credits' in data:
            return data['credits']
        else:
            st.error(f"API Error: {data.get('error', 'Unknown error')}")
            return 0
    except Exception as e:
        st.error(f"Failed to check quota: {str(e)}")
        return 0

def process_email_list(file_content, api_key):
    try:
        # Parse the text file with the specific format
        entries = []
        current_entry = {}
        
        for line in file_content.split('\n'):
            line = line.strip()
            if line:
                if '@' in line and '.' in line and ' ' not in line:  # Likely email
                    current_entry['email'] = line
                    entries.append(current_entry)
                    current_entry = {}
                elif not current_entry.get('name', ''):
                    current_entry['name'] = line
                elif not current_entry.get('department', ''):
                    current_entry['department'] = line
                elif not current_entry.get('university', ''):
                    current_entry['university'] = line
                elif not current_entry.get('country', ''):
                    current_entry['country'] = line
        
        df = pd.DataFrame(entries)
        
        if df.empty:
            return pd.DataFrame(columns=['name', 'department', 'university', 'country', 'email', 'verification_result'])
        
        # Verify emails
        results = []
        total_emails = len(df)
        st.session_state.verification_start_time = time.time()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, email in enumerate(df['email']):
            result = verify_email(email, api_key)
            if result:
                results.append(result)
            else:
                results.append({'result': 'error'})
            
            # Update progress
            progress = (i + 1) / total_emails
            st.session_state.verification_progress = progress
            progress_bar.progress(progress)
            
            # Calculate estimated time remaining
            elapsed_time = time.time() - st.session_state.verification_start_time
            if i > 0:
                estimated_total_time = elapsed_time / progress
                remaining_time = estimated_total_time - elapsed_time
                status_text.text(f"Processing {i+1} of {total_emails} emails. Estimated time remaining: {int(remaining_time)} seconds")
            
            time.sleep(0.1)  # Rate limiting
        
        df['verification_result'] = [r.get('result', 'error') if r else 'error' for r in results]
        
        # Calculate verification stats
        total = len(df)
        good = len(df[df['verification_result'].str.lower().isin(['valid', 'ok', 'good'])])
        bad = len(df[df['verification_result'] == 'invalid'])
        risky = len(df[df['verification_result'].str.lower().isin(['unknown', 'risky', 'accept_all'])])
        
        st.session_state.verification_stats = {
            'total': total,
            'good': good,
            'bad': bad,
            'risky': risky,
            'good_percent': round((good / total) * 100, 1) if total > 0 else 0,
            'bad_percent': round((bad / total) * 100, 1) if total > 0 else 0,
            'risky_percent': round((risky / total) * 100, 1) if total > 0 else 0
        }
        
        return df
    except Exception as e:
        st.error(f"Failed to process email list: {str(e)}")
        return pd.DataFrame()

def generate_report_file(df, report_type):
    """Generate different types of report files"""
    if df.empty:
        return ""
    
    output = ""
    if report_type == "good":
        valid_statuses = ['valid', 'ok', 'good']
        filtered_df = df[df['verification_result'].str.lower().isin(valid_statuses)]
    elif report_type == "bad":
        filtered_df = df[df['verification_result'] == 'invalid']
    elif report_type == "risky":
        risky_statuses = ['unknown', 'risky', 'accept_all']
        filtered_df = df[df['verification_result'].str.lower().isin(risky_statuses)]
    else:
        filtered_df = df
    
    for _, row in filtered_df.iterrows():
        output += f"{row.get('name', '')}\n"
        output += f"{row.get('department', '')}\n"
        output += f"{row.get('university', '')}\n"
        output += f"{row.get('country', '')}\n"
        output += f"{row.get('email', '')}\n\n"
    
    return output.strip()

# Firebase Storage Functions
def upload_to_firebase(file_content, filename):
    try:
        db = get_firestore_db()
        if not db:
            return False
            
        doc_ref = db.collection("email_files").document(filename)
        doc_ref.set({
            "content": file_content,
            "uploaded_at": datetime.now(),
            "uploaded_by": "admin"
        })
        return True
    except Exception as e:
        st.error(f"Failed to upload file: {str(e)}")
        return False

def download_from_firebase(filename):
    try:
        db = get_firestore_db()
        if not db:
            return None
            
        doc_ref = db.collection("email_files").document(filename)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict().get("content", "")
        return None
    except Exception as e:
        st.error(f"Failed to download file: {str(e)}")
        return None

def list_firebase_files():
    try:
        db = get_firestore_db()
        if not db:
            return []
            
        files_ref = db.collection("email_files")
        files = []
        for doc in files_ref.stream():
            files.append(doc.id)
        return files
    except Exception as e:
        st.error(f"Failed to list files: {str(e)}")
        return []

# Firebase Cloud Functions for campaign management
def save_template_to_firebase(journal_name, template_content):
    try:
        db = get_firestore_db()
        if not db:
            return False
            
        doc_ref = db.collection("email_templates").document(journal_name)
        doc_ref.set({
            "content": template_content,
            "last_updated": datetime.now(),
            "updated_by": "admin"
        })
        
        # Also update session state
        st.session_state.template_content[journal_name] = template_content
        return True
    except Exception as e:
        st.error(f"Failed to save template: {str(e)}")
        return False

def load_template_from_firebase(journal_name):
    try:
        db = get_firestore_db()
        if not db:
            return None
            
        doc_ref = db.collection("email_templates").document(journal_name)
        doc = doc_ref.get()
        if doc.exists:
            content = doc.to_dict().get("content", "")
            st.session_state.template_content[journal_name] = content
            return content
        return None
    except Exception as e:
        st.error(f"Failed to load template: {str(e)}")
        return None

def add_subject_to_firebase(journal_name, subject):
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("journal_subjects").document(journal_name)
        doc = doc_ref.get()
        subjects = doc.to_dict().get("subjects", []) if doc.exists else []
        if subject not in subjects:
            subjects.append(subject)
        doc_ref.set({
            "subjects": subjects,
            "last_updated": datetime.now(),
            "updated_by": "admin"
        })
        st.session_state.journal_subjects[journal_name] = subjects
        return True
    except Exception as e:
        st.error(f"Failed to save subject: {str(e)}")
        return False

def load_subjects_from_firebase(journal_name):
    try:
        db = get_firestore_db()
        if not db:
            return []

        doc_ref = db.collection("journal_subjects").document(journal_name)
        doc = doc_ref.get()
        if doc.exists:
            subjects = doc.to_dict().get("subjects", [])
            st.session_state.journal_subjects[journal_name] = subjects
            return subjects
        return []
    except Exception as e:
        st.error(f"Failed to load subjects: {str(e)}")
        return []

def save_campaign_state(campaign_data):
    try:
        db = get_firestore_db()
        if not db:
            return False
            
        doc_ref = db.collection("active_campaigns").document(str(campaign_data['campaign_id']))
        doc_ref.set(campaign_data)
        return True
    except Exception as e:
        st.error(f"Failed to save campaign state: {str(e)}")
        return False

def get_active_campaigns():
    try:
        db = get_firestore_db()
        if not db:
            return []
            
        campaigns_ref = db.collection("active_campaigns")
        campaigns = []
        for doc in campaigns_ref.stream():
            campaigns.append(doc.to_dict())
        return campaigns
    except Exception as e:
        st.error(f"Failed to get active campaigns: {str(e)}")
        return []

def delete_campaign(campaign_id):
    try:
        db = get_firestore_db()
        if not db:
            return False
            
        doc_ref = db.collection("active_campaigns").document(str(campaign_id))
        doc_ref.delete()
        return True
    except Exception as e:
        st.error(f"Failed to delete campaign: {str(e)}")
        return False

def update_campaign_progress(campaign_id, current_index, emails_sent):
    try:
        db = get_firestore_db()
        if not db:
            return False
            
        doc_ref = db.collection("active_campaigns").document(str(campaign_id))
        doc_ref.update({
            "current_index": current_index,
            "emails_sent": emails_sent,
            "last_updated": datetime.now()
        })
        return True
    except Exception as e:
        st.error(f"Failed to update campaign progress: {str(e)}")
        return False


# Email Campaign Section
def email_campaign_section():
    st.header("Email Campaign Management")
    
    
    # Journal Selection
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_journal = st.selectbox("Select Journal", JOURNALS, key="journal_select")
    with col2:
        new_journal = st.text_input("Add New Journal", key="new_journal")
        if new_journal and st.button("Add Journal"):
            if new_journal not in JOURNALS:
                JOURNALS.append(new_journal)
                st.session_state.selected_journal = new_journal
                if new_journal not in st.session_state.journal_reply_addresses:
                    st.session_state.journal_reply_addresses[new_journal] = ""
                st.rerun()
    
    st.session_state.selected_journal = selected_journal
    
    # Load template from Firebase if available
    if selected_journal not in st.session_state.template_content:
        loaded_template = load_template_from_firebase(selected_journal)
        if loaded_template:
            st.session_state.template_content[selected_journal] = loaded_template

    if selected_journal not in st.session_state.journal_subjects:
        load_subjects_from_firebase(selected_journal)
    
    # Campaign settings in the sidebar
    with st.sidebar.expander("Campaign Settings", expanded=False):
        st.session_state.email_service = st.radio(
            "Select Email Service",
            ["SMTP2GO", "Amazon SES"],
            index=0 if st.session_state.email_service == "SMTP2GO" else 1,
            key="email_service_select"
        )

        reply_address = st.text_input(
            f"Reply-to Address for {selected_journal}",
            value=st.session_state.journal_reply_addresses.get(selected_journal, ""),
            key=f"reply_{selected_journal}"
        )
        if st.button("Save Reply Address", key="save_reply_address"):
            st.session_state.journal_reply_addresses[selected_journal] = reply_address
            st.success("Reply address saved!")

        st.text_input(
            "Sender Email",
            value=st.session_state.sender_email,
            key="sender_email"
        )

    # Journal Subject Management
    with st.expander("Journal Subjects"):
        subjects = st.session_state.journal_subjects.get(selected_journal, [])
        if subjects:
            st.write("Saved Subjects", subjects)
        else:
            st.write("No subjects added yet")
        new_subject = st.text_input("Add Subject", key=f"subject_{selected_journal}")
        if st.button("Save Subject"):
            if new_subject:
                if add_subject_to_firebase(selected_journal, new_subject):
                    st.success("Subject saved!")
    
    # Email Template Editor with ACE Editor
    with st.expander("Email Template Editor"):
        template = get_journal_template(st.session_state.selected_journal)

        col1, col2 = st.columns(2)
        with col1:
            email_subject = st.text_input(
                "Email Subject",
                f"Call for Papers - {st.session_state.selected_journal}"
            )

        editor_col, preview_col = st.columns(2)

        with editor_col:
            st.markdown("**Template Editor**")
            email_body = st_ace(
                value=template,
                language="html",
                theme="chrome",
                font_size=14,
                tab_size=2,
                wrap=True,
                show_gutter=True,
                key=f"editor_{selected_journal}",
                height=400
            )

            if st.button("Save Template"):
                if save_template_to_firebase(selected_journal, email_body):
                    st.success("Template saved to cloud!")

            st.info("""Available template variables:
            - $$Author_Name$$: Author's full name
            - $$Author_Address$$: All address lines before email
            - $$AuthorLastname$$: Author's last name
            - $$Department$$: Author's department
            - $$University$$: Author's university
            - $$Country$$: Author's country
            - $$Author_Email$$: Author's email
            - $$Journal_Name$$: Selected journal name
            - $$Unsubscribe_Link$$: Unsubscribe link""")

        with preview_col:
            st.markdown("**Preview**")
            preview_html = email_body.replace("$$Author_Name$$", "Professor John Doe")
            preview_html = preview_html.replace(
                "$$Author_Address$$",
                "Department of Computer Science<br>Harvard University<br>United States"
            )
            preview_html = preview_html.replace("$$AuthorLastname$$", "Doe")
            preview_html = preview_html.replace("$$Department$$", "Computer Science")
            preview_html = preview_html.replace("$$University$$", "Harvard University")
            preview_html = preview_html.replace("$$Country$$", "United States")
            preview_html = preview_html.replace("$$Author_Email$$", "john.doe@harvard.edu")
            preview_html = preview_html.replace(
                "$$Journal_Name$$",
                st.session_state.selected_journal
            )
            preview_html = preview_html.replace(
                "$$Unsubscribe_Link$$",
                "https://pphmjopenaccess.com/unsubscribe?email=john.doe@harvard.edu"
            )

            st.markdown(preview_html, unsafe_allow_html=True)
    
    # File Upload
    st.subheader("Recipient List")
    file_source = st.radio("Select file source", ["Local Upload", "Cloud Storage"])
    
    if file_source == "Local Upload":
        uploaded_file = st.file_uploader("Upload recipient list (CSV or TXT)", type=["csv", "txt"])
        if uploaded_file:
            if uploaded_file.name.endswith('.txt'):
                # Process the text file with the specific format
                file_content = uploaded_file.read().decode('utf-8')
                entries = []
                current_entry = {}
                
                for line in file_content.split('\n'):
                    line = line.strip()
                    if line:
                        if '@' in line and '.' in line and ' ' not in line:  # Likely email
                            current_entry['email'] = line
                            entries.append(current_entry)
                            current_entry = {}
                        elif not current_entry.get('name', ''):
                            current_entry['name'] = line
                        elif not current_entry.get('department', ''):
                            current_entry['department'] = line
                        elif not current_entry.get('university', ''):
                            current_entry['university'] = line
                        elif not current_entry.get('country', ''):
                            current_entry['country'] = line
                
                df = pd.DataFrame(entries)
            else:
                df = pd.read_csv(uploaded_file)
            
            st.session_state.current_recipient_list = df
            st.dataframe(df.head())
            
            if st.button("Save to Firebase"):
                if uploaded_file.name.endswith('.txt'):
                    if upload_to_firebase(file_content, uploaded_file.name):
                        st.success("File uploaded to Firebase successfully!")
                else:
                    csv_content = df.to_csv(index=False)
                    if upload_to_firebase(csv_content, uploaded_file.name):
                        st.success("File uploaded to Firebase successfully!")
    else:
        if st.button("Refresh File List"):
            st.session_state.firebase_files = list_firebase_files()
        
        if 'firebase_files' in st.session_state and st.session_state.firebase_files:
            selected_file = st.selectbox("Select file from Firebase", st.session_state.firebase_files)
            
            if st.button("Load File"):
                file_content = download_from_firebase(selected_file)
                if file_content:
                    if selected_file.endswith('.txt'):
                        # Process the text file with the specific format
                        entries = []
                        current_entry = {}
                        
                        for line in file_content.split('\n'):
                            line = line.strip()
                            if line:
                                if '@' in line and '.' in line and ' ' not in line:  # Likely email
                                    current_entry['email'] = line
                                    entries.append(current_entry)
                                    current_entry = {}
                                elif not current_entry.get('name', ''):
                                    current_entry['name'] = line
                                elif not current_entry.get('department', ''):
                                    current_entry['department'] = line
                                elif not current_entry.get('university', ''):
                                    current_entry['university'] = line
                                elif not current_entry.get('country', ''):
                                    current_entry['country'] = line
                        
                        df = pd.DataFrame(entries)
                    else:
                        df = pd.read_csv(StringIO(file_content))
                    
                    st.session_state.current_recipient_list = df
                    st.dataframe(df.head())
        else:
            st.info("No files found in Cloud Storage")
    
    
    # Send Options
    if 'current_recipient_list' in st.session_state and not st.session_state.campaign_paused:
        st.subheader("Campaign Options")
        
        unsubscribe_base_url = st.text_input(
            "Unsubscribe Base URL",
            "https://pphmjopenaccess.com/unsubscribe?email="
        )

        subjects_for_journal = st.session_state.journal_subjects.get(selected_journal, [])
        selected_subjects = st.multiselect(
            "Select Subjects",
            subjects_for_journal,
            default=subjects_for_journal,
            key=f"subject_select_{selected_journal}"
        )
        
        send_option = st.radio("Send Option", ["Send Now", "Schedule"])
        
        if send_option == "Schedule":
            col1, col2 = st.columns(2)
            with col1:
                schedule_date = st.date_input("Schedule Date", datetime.now() + timedelta(days=1))
            with col2:
                schedule_time = st.time_input("Schedule Time", datetime.now().time())
            schedule_time = datetime.combine(schedule_date, schedule_time)
        
        if st.button("Start Campaign"):
            if st.session_state.email_service == "SMTP2GO" and not config['smtp2go']['api_key']:
                st.error("SMTP2GO API key not configured")
                return
            elif st.session_state.email_service == "Amazon SES":
                if not st.session_state.ses_client:
                    initialize_ses()
                if not st.session_state.ses_client:
                    st.error("SES client not initialized. Please configure SES first.")
                    return
            
            df = st.session_state.current_recipient_list
            total_emails = len(df)
            
            # Create campaign record in Firestore
            campaign_id = int(time.time())
            campaign_data = {
                'campaign_id': campaign_id,
                'journal_name': selected_journal,
                'email_subjects': selected_subjects,
                'email_body': email_body,
                'email_service': st.session_state.email_service,
                'sender_email': st.session_state.sender_email,
                'unsubscribe_base_url': unsubscribe_base_url,
                'reply_to': st.session_state.journal_reply_addresses.get(selected_journal, None),
                'recipient_list': df.to_dict('records'),
                'total_emails': total_emails,
                'current_index': 0,
                'emails_sent': 0,
                'status': 'active',
                'created_at': datetime.now(),
                'last_updated': datetime.now()
            }
            
            if not save_campaign_state(campaign_data):
                st.error("Failed to save campaign state")
                return
            
            st.session_state.active_campaign = campaign_data
            st.session_state.campaign_paused = False
            st.session_state.campaign_cancelled = False
            
            # Show progress UI
            progress_bar = st.progress(0)
            status_text = st.empty()
            cancel_button = st.button("Cancel Campaign")
            
            success_count = 0
            email_ids = []
            
            reply_to = st.session_state.journal_reply_addresses.get(selected_journal, None)
            
            for i, row in df.iterrows():
                if st.session_state.campaign_cancelled:
                    break
                
                # Build author address from all fields except email
                author_address = ""
                if row.get('department', ''):
                    author_address += f"{row['department']}<br>"
                if row.get('university', ''):
                    author_address += f"{row['university']}<br>"
                if row.get('country', ''):
                    author_address += f"{row['country']}<br>"
                
                email_content = email_body
                email_content = email_content.replace("$$Author_Name$$", str(row.get('name', '')))
                email_content = email_content.replace("$$Author_Address$$", author_address)
                
                # Extract last name if available
                if 'name' in row and isinstance(row['name'], str) and ' ' in row['name']:
                    last_name = row['name'].split()[-1]
                else:
                    last_name = ''
                email_content = email_content.replace("$$AuthorLastname$$", last_name)
                
                email_content = email_content.replace("$$Department$$", str(row.get('department', '')))
                email_content = email_content.replace("$$University$$", str(row.get('university', '')))
                email_content = email_content.replace("$$Country$$", str(row.get('country', '')))
                email_content = email_content.replace("$$Author_Email$$", str(row.get('email', '')))
                email_content = email_content.replace("$$Journal_Name$$", st.session_state.selected_journal)
                
                unsubscribe_link = f"{unsubscribe_base_url}{row.get('email', '')}"
                email_content = email_content.replace("$$Unsubscribe_Link$$", unsubscribe_link)
                
                plain_text = email_content.replace("<br>", "\n").replace("</p>", "\n\n").replace("<p>", "")
                
                subject_cycle = selected_subjects if selected_subjects else [email_subject]
                subject = subject_cycle[i % len(subject_cycle)]

                if st.session_state.email_service == "SMTP2GO":
                    success, email_id = send_email_via_smtp2go(
                        row.get('email', ''),
                        subject,
                        email_content,
                        plain_text,
                        unsubscribe_link,
                        reply_to
                    )
                else:
                    response, email_id = send_ses_email(
                        st.session_state.ses_client,
                        st.session_state.sender_email,
                        row.get('email', ''),
                        subject,
                        email_content,
                        plain_text,
                        unsubscribe_link,
                        reply_to
                    )
                    success = response is not None
                
                if success:
                    success_count += 1
                    if email_id:
                        email_ids.append(email_id)
                
                progress = (i + 1) / total_emails
                progress_bar.progress(progress)
                status_text.text(f"Processing {i+1} of {total_emails}: {row.get('email', '')}")
                
                # Update progress in Firestore
                update_campaign_progress(campaign_id, i+1, success_count)
                
                # Check for cancel button
                if cancel_button:
                    st.session_state.campaign_cancelled = True
                    st.warning("Campaign cancellation requested...")
                    break
                
                # Rate limiting
                time.sleep(0.1)
            
            # Mark campaign as completed if not cancelled
            if not st.session_state.campaign_cancelled:
                campaign_data = {
                    'status': 'completed',
                    'completed_at': datetime.now(),
                    'emails_sent': success_count
                }
                db = get_firestore_db()
                if db:
                    doc_ref = db.collection("active_campaigns").document(str(campaign_id))
                    doc_ref.update(campaign_data)
                
                # Record campaign details
                campaign_data = {
                    'timestamp': datetime.now(),
                    'journal': selected_journal,
                    'emails_sent': success_count,
                    'total_emails': total_emails,
                    'subject': ','.join(selected_subjects) if selected_subjects else email_subject,
                    'email_ids': ','.join(email_ids),
                    'service': st.session_state.email_service
                }
                st.session_state.campaign_history.append(campaign_data)
                
                st.success(f"Campaign completed! {success_count} of {total_emails} emails sent successfully.")
                show_email_analytics()
            else:
                st.warning(f"Campaign cancelled. {success_count} of {total_emails} emails were sent.")

# Email Verification Section
def email_verification_section():
    st.header("Email Verification")
    
    # Check verification quota using correct endpoint
    if config['millionverifier']['api_key']:
        with st.spinner("Checking verification quota..."):
            remaining_quota = check_millionverifier_quota(config['millionverifier']['api_key'])
            st.metric("Remaining Verification Credits", remaining_quota)
    else:
        st.warning("MillionVerifier API key not configured")
    
    # File Upload for Verification
    st.subheader("Email List Verification")
    file_source = st.radio("Select file source for verification", ["Local Upload", "Cloud Storage"])
    
    if file_source == "Local Upload":
        uploaded_file = st.file_uploader("Upload email list for verification (TXT format)", type=["txt"])
        if uploaded_file:
            file_content = uploaded_file.read().decode('utf-8')
            st.text_area("File Content Preview", file_content, height=150)
            
            if st.button("Verify Emails"):
                if not config['millionverifier']['api_key']:
                    st.error("Please configure MillionVerifier API Key first")
                    return
                
                with st.spinner("Verifying emails..."):
                    result_df = process_email_list(file_content, config['millionverifier']['api_key'])
                    if not result_df.empty:
                        st.session_state.verified_emails = result_df
                        st.dataframe(result_df)
                    else:
                        st.error("No valid emails found in the file")
    else:
        if st.button("Refresh File List for Verification"):
            st.session_state.firebase_files_verification = list_firebase_files()
        
        if 'firebase_files_verification' in st.session_state and st.session_state.firebase_files_verification:
            selected_file = st.selectbox("Select file to verify from Firebase", 
                                       st.session_state.firebase_files_verification)
            
            if st.button("Load File for Verification"):
                file_content = download_from_firebase(selected_file)
                if file_content:
                    st.text_area("File Content Preview", file_content, height=150)
                    st.session_state.current_verification_list = file_content
            
            if 'current_verification_list' in st.session_state and st.button("Start Verification"):
                if not config['millionverifier']['api_key']:
                    st.error("Please configure MillionVerifier API Key first")
                    return
                
                with st.spinner("Verifying emails..."):
                    result_df = process_email_list(st.session_state.current_verification_list, config['millionverifier']['api_key'])
                    if not result_df.empty:
                        st.session_state.verified_emails = result_df
                        st.dataframe(result_df)
                    else:
                        st.error("No valid emails found in the file")
        else:
            st.info("No files found in Cloud Storage")
    
    # Verification Results and Reports
    if not st.session_state.verified_emails.empty:
        st.subheader("Verification Results")
        
        # Display stats
        stats = st.session_state.verification_stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Good Emails", f"{stats['good']} ({stats['good_percent']}%)", 
                     help="Good emails are valid, existing emails. It is safe to send emails to them.")
        with col2:
            st.metric("Bad Emails", f"{stats['bad']} ({stats['bad_percent']}%)", 
                     help="Bad emails don't exist, don't email them!")
        with col3:
            st.metric("Risky Emails", f"{stats['risky']} ({stats['risky_percent']}%)", 
                     help="Risky emails may exist or not. Use with caution.")
        
        # Bar chart instead of pie chart
        fig, ax = plt.subplots(figsize=(8, 4))
        categories = ['Good', 'Bad', 'Risky']
        counts = [stats['good'], stats['bad'], stats['risky']]
        colors = ['#4CAF50', '#F44336', '#9C27B0']
        
        bars = ax.bar(categories, counts, color=colors)
        ax.set_title('Email Verification Results')
        ax.set_ylabel('Count')
        
        # Add value labels on top of bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom')
        
        st.pyplot(fig)
        
        # Download reports - side by side buttons
        st.subheader("Download Reports")
        
        # First row of buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            good_content = generate_report_file(st.session_state.verified_emails, "good")
            st.download_button(
                label="Good Emails Only",
                data=good_content,
                file_name="good_emails.txt",
                mime="text/plain",
                help="Download only emails verified as good",
                key="good_emails_btn",
                use_container_width=True
            )
        
        with col2:
            bad_content = generate_report_file(st.session_state.verified_emails, "bad")
            st.download_button(
                label="Bad Emails Only",
                data=bad_content,
                file_name="bad_emails.txt",
                mime="text/plain",
                help="Download only emails verified as bad",
                key="bad_emails_btn",
                use_container_width=True
            )
        
        with col3:
            risky_content = generate_report_file(st.session_state.verified_emails, "risky")
            st.download_button(
                label="Risky Emails Only",
                data=risky_content,
                file_name="risky_emails.txt",
                mime="text/plain",
                help="Download only emails verified as risky",
                key="risky_emails_btn",
                use_container_width=True
            )
        
        # Second row of buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            full_content = generate_report_file(st.session_state.verified_emails, "full")
            st.download_button(
                label="Full Report (TXT)",
                data=full_content,
                file_name="full_report.txt",
                mime="text/plain",
                help="Download complete report in TXT format",
                key="full_report_txt",
                use_container_width=True
            )
        
        with col2:
            st.download_button(
                label="Full Report (CSV)",
                data=st.session_state.verified_emails.to_csv(index=False),
                file_name="full_report.csv",
                mime="text/csv",
                help="Download complete report in CSV format",
                key="full_report_csv",
                use_container_width=True
            )

def analytics_section():
    st.header("Comprehensive Email Analytics")
    
    if st.session_state.email_service == "SMTP2GO":
        st.info("SMTP2GO Analytics Dashboard")
        
        # Fetch detailed analytics
        analytics_data = fetch_smtp2go_analytics()

        if analytics_data:
            stats_list = analytics_data.get('history')
            if not stats_list:
                st.info("No statistics available yet.")
                return

            # Calculate totals (API may not always provide a 'totals' key)
            if 'totals' in analytics_data:
                totals = analytics_data['totals']
            else:
                df_totals = pd.DataFrame(stats_list)
                totals = {
                    'sent': df_totals['sent'].sum(),
                    'delivered': df_totals['delivered'].sum(),
                    'opens_unique': df_totals['opens_unique'].sum(),
                    'clicks_unique': df_totals['clicks_unique'].sum(),
                    'hard_bounces': df_totals['hard_bounces'].sum(),
                    'soft_bounces': df_totals['soft_bounces'].sum()
                }

            # Overall metrics
            st.subheader("Overall Performance")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Sent", totals['sent'])
            with col2:
                delivery_rate = (totals['delivered'] / totals['sent'] * 100) if totals['sent'] else 0
                st.metric("Delivered", totals['delivered'], f"{delivery_rate:.1f}%")
            with col3:
                open_rate = (totals['opens_unique'] / totals['delivered'] * 100) if totals['delivered'] else 0
                st.metric("Opened", totals['opens_unique'], f"{open_rate:.1f}%")
            with col4:
                click_rate = (totals['clicks_unique'] / totals['opens_unique'] * 100) if totals['opens_unique'] else 0
                st.metric("Clicked", totals['clicks_unique'], f"{click_rate:.1f}%")
            with col5:
                st.metric("Bounced", totals['hard_bounces'] + totals['soft_bounces'])
            
            # Time series data
            st.subheader("Performance Over Time")
            df = pd.DataFrame(stats_list)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # Calculate rates
            df['delivery_rate'] = (df['delivered'] / df['sent']) * 100
            df['open_rate'] = (df['opens_unique'] / df['delivered']) * 100
            df['click_rate'] = (df['clicks_unique'] / df['opens_unique']) * 100
            
            tab1, tab2, tab3 = st.tabs(["Volume Metrics", "Engagement Rates", "Bounce & Complaints"])
            
            with tab1:
                st.line_chart(df[['sent', 'delivered', 'opens_unique', 'clicks_unique']])
            
            with tab2:
                st.line_chart(df[['delivery_rate', 'open_rate', 'click_rate']])
            
            with tab3:
                st.line_chart(df[['hard_bounces', 'soft_bounces', 'spam_complaints']])

            # Campaign details
            st.subheader("Recent Campaigns")
            if st.session_state.campaign_history:
                campaign_df = pd.DataFrame(st.session_state.campaign_history)
                
                # Add performance metrics to campaign history
                campaign_df['delivery_rate'] = campaign_df.apply(lambda x: (x['emails_sent'] / x['total_emails']) * 100, axis=1)
                
                st.dataframe(
                    campaign_df.sort_values('timestamp', ascending=False),
                    column_config={
                        "timestamp": "Date",
                        "journal": "Journal",
                        "emails_sent": "Sent",
                        "total_emails": "Total",
                        "delivery_rate": st.column_config.ProgressColumn(
                            "Delivery Rate",
                            format="%.1f%%",
                            min_value=0,
                            max_value=100,
                        ),
                        "subject": "Subject",
                        "service": "Service"
                    }
                )
            else:
                st.info("No campaign history available")

            # Show bounce details if available
            if analytics_data.get('bounces'):
                st.subheader("Recent Bounces")
                bounce_df = pd.DataFrame(analytics_data['bounces'])
                st.dataframe(bounce_df)
        else:
            st.info("No analytics data available yet. Please send some emails first.")
    else:
        if not st.session_state.ses_client:
            initialize_ses()
            
        if not st.session_state.ses_client:
            st.error("SES client not initialized")
            return
        
        try:
            stats = st.session_state.ses_client.get_send_statistics()
            datapoints = stats['SendDataPoints']
            
            if not datapoints:
                st.info("No email statistics available yet.")
                return
            
            df = pd.DataFrame(datapoints)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df.set_index('Timestamp', inplace=True)
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Delivery Attempts", df['DeliveryAttempts'].sum())
            with col2:
                delivery_rate = (df['DeliveryAttempts'].sum() - df['Bounces'].sum()) / df['DeliveryAttempts'].sum() * 100
                st.metric("Delivery Rate", f"{delivery_rate:.2f}%")
            with col3:
                st.metric("Bounces", df['Bounces'].sum())
            with col4:
                st.metric("Complaints", df['Complaints'].sum())
            
            st.line_chart(df[['DeliveryAttempts', 'Bounces', 'Complaints']])
            
            bounce_response = st.session_state.ses_client.list_bounces()
            if bounce_response['Bounces']:
                st.subheader("Bounce Details")
                bounce_df = pd.DataFrame(bounce_response['Bounces'])
                st.dataframe(bounce_df)
            
        except Exception as e:
            st.error(f"Failed to fetch analytics: {str(e)}")

def fetch_smtp2go_analytics():
    """Retrieve analytics details from SMTP2GO using POST requests."""
    try:
        api_key = config['smtp2go']['api_key']
        if not api_key:
            st.error("SMTP API key not configured")
            return None

        base_url = "https://api.smtp2go.com/v3"
        payload = {
            "api_key": api_key,
            "date_start": (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d"),
            "date_end": datetime.utcnow().strftime("%Y-%m-%d")
        }

        # Email history
        hist_resp = requests.post(f"{base_url}/stats/email_history", json=payload)
        hist_resp.raise_for_status()
        history_data = hist_resp.json().get("data", [])

        # Bounce statistics
        bounce_resp = requests.post(f"{base_url}/stats/email_bounces", json=payload)
        bounce_resp.raise_for_status()
        bounces_data = bounce_resp.json().get("data", [])

        # Recent activity/search
        search_resp = requests.post(f"{base_url}/activity/search", json=payload)
        search_resp.raise_for_status()
        activity_data = search_resp.json().get("data", [])

        return {
            "history": history_data,
            "bounces": bounces_data,
            "activity": activity_data
        }
    except Exception as e:
        st.error(f"Error fetching SMTP analytics: {str(e)}")
        return None

def show_email_analytics():
    st.subheader("Email Campaign Analytics Dashboard")
    
    if st.session_state.email_service == "SMTP2GO":
        analytics_data = fetch_smtp2go_analytics()

        if analytics_data:
            stats_list = analytics_data.get('history')
            if not stats_list:
                st.info("No statistics available yet.")
                return

            # Process data for display
            df = pd.DataFrame(stats_list)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # Calculate rates
            df['delivery_rate'] = (df['delivered'] / df['sent']) * 100
            df['open_rate'] = (df['opens_unique'] / df['delivered']) * 100
            df['click_rate'] = (df['clicks_unique'] / df['opens_unique']) * 100
            
            # Summary metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Sent", df['sent'].sum())
            with col2:
                st.metric("Delivered", df['delivered'].sum(), 
                         f"{df['delivery_rate'].mean():.1f}%")
            with col3:
                st.metric("Opened", df['opens_unique'].sum(), 
                         f"{df['open_rate'].mean():.1f}%")
            with col4:
                st.metric("Clicked", df['clicks_unique'].sum(), 
                         f"{df['click_rate'].mean():.1f}%")
            with col5:
                st.metric("Bounced", df['hard_bounces'].sum() + df['soft_bounces'].sum())
            
            # Time series charts
            st.subheader("Performance Over Time")
            tab1, tab2, tab3 = st.tabs(["Volume Metrics", "Engagement Rates", "Bounce & Complaints"])
            
            with tab1:
                st.line_chart(df[['sent', 'delivered', 'opens_unique', 'clicks_unique']])
            
            with tab2:
                st.line_chart(df[['delivery_rate', 'open_rate', 'click_rate']])
            
            with tab3:
                st.line_chart(df[['hard_bounces', 'soft_bounces', 'spam_complaints']])
            
            # Campaign details
            st.subheader("Recent Campaigns")
            if st.session_state.campaign_history:
                campaign_df = pd.DataFrame(st.session_state.campaign_history)
                st.dataframe(campaign_df.sort_values('timestamp', ascending=False))
            else:
                st.info("No campaign history available")

            if analytics_data.get('bounces'):
                st.subheader("Recent Bounces")
                bounce_df = pd.DataFrame(analytics_data['bounces'])
                st.dataframe(bounce_df)
        else:
            st.info("No analytics data available yet. Please send some emails first.")
    else:
        if not st.session_state.ses_client:
            st.error("SES client not initialized")
            return
        
        try:
            stats = st.session_state.ses_client.get_send_statistics()
            datapoints = stats['SendDataPoints']
            
            if not datapoints:
                st.info("No email statistics available yet.")
                return
            
            df = pd.DataFrame(datapoints)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df.set_index('Timestamp', inplace=True)
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Delivery Attempts", df['DeliveryAttempts'].sum())
            with col2:
                delivery_rate = (df['DeliveryAttempts'].sum() - df['Bounces'].sum()) / df['DeliveryAttempts'].sum() * 100
                st.metric("Delivery Rate", f"{delivery_rate:.2f}%")
            with col3:
                st.metric("Bounces", df['Bounces'].sum())
            with col4:
                st.metric("Complaints", df['Complaints'].sum())
            
            st.line_chart(df[['DeliveryAttempts', 'Bounces', 'Complaints']])
            
            bounce_response = st.session_state.ses_client.list_bounces()
            if bounce_response['Bounces']:
                st.subheader("Bounce Details")
                bounce_df = pd.DataFrame(bounce_response['Bounces'])
                st.dataframe(bounce_df)
            
        except Exception as e:
            st.error(f"Failed to fetch analytics: {str(e)}")

def main():
    # Check authentication
    check_auth()
    
    # Main app for authenticated users
    st.title(f"PPH Email Manager - Welcome admin")
    
    # Navigation with additional links
    with st.sidebar:
        app_mode = st.selectbox("Select Mode", ["Email Campaign", "Verify Emails", "Analytics"])
        st.markdown("---")
        st.markdown("### Quick Links")
        st.markdown("[📊 Email Reports](https://app-us.smtp2go.com/reports/activity/)", unsafe_allow_html=True)
        st.markdown("[📝 Entry Manager](https://pphmjcrm.streamlit.app)", unsafe_allow_html=True)
    
    # Initialize services
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    if app_mode == "Email Campaign":
        email_campaign_section()
    elif app_mode == "Verify Emails":
        email_verification_section()
    else:
        analytics_section()

if __name__ == "__main__":
    main()
