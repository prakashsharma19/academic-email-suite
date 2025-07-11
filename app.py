import streamlit as st
import boto3
import pandas as pd
import datetime
import time
import requests
import json
import os
import pytz
import re
import hashlib
from datetime import datetime, timedelta
from io import StringIO, BytesIO
import base64
import math
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
    initial_sidebar_state="collapsed",
    menu_items={
        'About': "### Academic Email Management Suite\n\nDeveloped by Prakash (contact@cpsharma.com)"
    }
)

# Light Theme with Footer
def set_light_theme():
    light_theme = """
    <style>
    :root {
        --primary-color: #0d6efd;
        --background-color: #ffffff;
        --secondary-background-color: #f8f9fa;
        --text-color: #212529;
        --font: 'Roboto', sans-serif;
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
        background-color: var(--secondary-background-color);
        color: var(--text-color);
        text-align: center;
        padding: 10px;
        font-size: 0.8em;
        border-top: 1px solid #ddd;
    }
    .footer a {
        color: var(--primary-color);
        text-decoration: none;
        font-weight: bold;
    }
    /* Button Colors */
    .stButton button,
    .stDownloadButton button,
    .bad-email-btn button,
    .good-email-btn button,
    .risky-email-btn button {
        background-color: #007bff !important;
        border-color: #007bff !important;
        color: white !important;
        transition: transform 0.05s ease;
    }
    .stButton button:hover,
    .stDownloadButton button:hover,
    .bad-email-btn button:hover,
    .good-email-btn button:hover,
    .risky-email-btn button:hover {
        background-color: #0069d9 !important;
        border-color: #0069d9 !important;
    }
    /* Send Ads button */
    .send-ads-btn button {
        background-color: #4CAF50 !important;
        color: white !important;
        border-color: #4CAF50 !important;
        transition: background-color 0.3s ease;
    }
    .send-ads-btn button:hover {
        background-color: #45a049 !important;
    }
    .stButton button:active,
    .stDownloadButton button:active,
    .bad-email-btn button:active,
    .good-email-btn button:active,
    .risky-email-btn button:active {
        transform: scale(0.97);
    }
    /* Sidebar appearance */
    section[data-testid="stSidebar"] {
        background-color: #e6f0ff;
        width: 200px !important;
    }
    /* App title positioning */
    .app-title {
        position: absolute;
        top: 10px;
        left: 10px;
        font-size: 1.5rem !important;
        margin: 0;
    }
    /* Multiselect tags wrap text */
    div[data-baseweb="tag"] span {
        white-space: normal !important;
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
                    st.session_state.username = username
                    st.rerun()
                else:
                    st.error("Invalid credentials")
        
        st.stop()
    
    logout_label = "Logout"
    if 'username' in st.session_state:
        logout_label += f" ({st.session_state.username})"
    if st.sidebar.button(logout_label):
        st.session_state.authenticated = False
        if 'username' in st.session_state:
            del st.session_state.username
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
    if 'sender_name' not in st.session_state:
        st.session_state.sender_name = config['sender_name']
    if 'blocked_domains' not in st.session_state:
        st.session_state.blocked_domains = []
    if 'blocked_emails' not in st.session_state:
        st.session_state.blocked_emails = []
    if 'block_settings_loaded' not in st.session_state:
        st.session_state.block_settings_loaded = False
    if 'journals_loaded' not in st.session_state:
        st.session_state.journals_loaded = False
    if 'editor_journals_loaded' not in st.session_state:
        st.session_state.editor_journals_loaded = False
    if 'template_spam_score' not in st.session_state:
        st.session_state.template_spam_score = {}
    if 'template_spam_report' not in st.session_state:
        st.session_state.template_spam_report = {}
    if 'template_spam_summary' not in st.session_state:
        st.session_state.template_spam_summary = {}
    if 'spam_check_cache' not in st.session_state:
        st.session_state.spam_check_cache = {}
    if 'last_refreshed_journal' not in st.session_state:
        st.session_state.last_refreshed_journal = None
    if 'show_journal_details' not in st.session_state:
        st.session_state.show_journal_details = False
    if 'selected_editor_journal' not in st.session_state:
        st.session_state.selected_editor_journal = None
    if 'editor_show_journal_details' not in st.session_state:
        st.session_state.editor_show_journal_details = False


def update_sender_name():
    """Update sender name with the currently selected journal."""
    journal = st.session_state.get("selected_journal")
    if journal:
        st.session_state.sender_name = f"{journal} - {config['sender_name']}"
    st.session_state.show_journal_details = False


def update_editor_sender_name():
    """Update sender name for editor invitation based on selected journal."""
    journal = st.session_state.get("selected_editor_journal")
    if journal:
        st.session_state.sender_name = f"{journal} - {config['sender_name']}"
    st.session_state.editor_show_journal_details = False


def read_uploaded_text(uploaded_file):
    """Decode uploaded file content using a variety of encodings."""
    data = uploaded_file.read()
    for enc in (
        "utf-8",
        "utf-8-sig",
        "utf-16",
        "utf-16le",
        "utf-16be",
        "latin-1",
    ):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")

# Utility
def sanitize_author_name(name: str) -> str:
    """Remove a leading 'Professor' from the provided name."""
    if isinstance(name, str):
        sanitized = re.sub(r"(?i)^professor\s+", "", name).strip()
        return sanitized
    return ""


def generate_clock_image(tz: str) -> str:
    """Return base64 encoded analog clock image for the given timezone."""
    now = datetime.now(pytz.timezone(tz))
    fig, ax = plt.subplots(figsize=(1.2, 1.2))
    ax.axis("off")
    circle = plt.Circle((0, 0), 1, fill=False, linewidth=2, color="black")
    ax.add_artist(circle)
    for i in range(12):
        angle = math.radians(i * 30)
        x_out, y_out = math.sin(angle), math.cos(angle)
        ax.plot([0.9 * x_out, x_out], [0.9 * y_out, y_out], color="black", lw=1)
    hour = (now.hour % 12) + now.minute / 60.0
    minute = now.minute + now.second / 60.0
    h_angle = math.radians(hour * 30)
    m_angle = math.radians(minute * 6)
    ax.plot([0, 0.5 * math.sin(h_angle)], [0, 0.5 * math.cos(h_angle)], color="black", lw=2)
    ax.plot([0, 0.8 * math.sin(m_angle)], [0, 0.8 * math.cos(m_angle)], color="black", lw=1)
    ax.set_xlim(-1, 1)
    ax.set_ylim(-1, 1)
    ax.set_aspect("equal")
    buf = BytesIO()
    plt.tight_layout(pad=0.1)
    fig.savefig(buf, format="png", transparent=True, bbox_inches='tight', pad_inches=0.05)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def display_world_clocks():
    """Show analog and digital clocks for common time zones using HTML/CSS."""
    zones = [
        ("USA", "America/New_York"),
        ("Indonesia", "Asia/Jakarta"),
        ("United Kingdom", "Europe/London"),
        ("Germany", "Europe/Berlin"),
        ("UAE", "Asia/Dubai"),
        ("China", "Asia/Shanghai"),
        ("Japan", "Asia/Tokyo"),
        ("Australia", "Australia/Sydney"),
        ("Brazil", "America/Sao_Paulo"),
        ("South Africa", "Africa/Johannesburg"),
    ]

    style = """
    <style>
    .aes-clock-container {display:flex;gap:10px;flex-wrap:wrap;}
    .aes-clock {position:relative;width:60px;height:60px;border:2px solid #000;border-radius:50%;margin:auto;}
    .aes-hour {position:absolute;top:50%;left:50%;width:20px;height:3px;background:#000;transform-origin:0% 50%;}
    .aes-minute {position:absolute;top:50%;left:50%;width:28px;height:2px;background:#000;transform-origin:0% 50%;}
    </style>
    """

    html = f"{style}<div class='aes-clock-container'>"
    for label, tz in zones:
        now = datetime.now(pytz.timezone(tz))
        digital = now.strftime("%I:%M %p")
        color = "green" if 7 <= now.hour < 20 else "black"
        hour_angle = (now.hour % 12 + now.minute / 60) * 30 - 90
        minute_angle = now.minute * 6 - 90
        html += (
            "<div style='text-align:center;'>"
            "<div class='aes-clock'>"
            f"<div class='aes-hour' style='transform:rotate({hour_angle}deg)'></div>"
            f"<div class='aes-minute' style='transform:rotate({minute_angle}deg)'></div>"
            "</div>"
            f"<div style='font-size:12px'>{label}</div>"
            f"<div style='font-size:12px;color:{color}'>{digital}</div>"
            "</div>"
        )
    html += "</div><div style='font-size:14px;margin-top:4px;color:red;'>Note: Sending emails in working hours improves read rate.</div>"
    st.markdown(html, unsafe_allow_html=True)

# Journal Data
JOURNALS = [
    "Advances and Applications in Fluid Mechanics",
    "Advances in Fuzzy Sets and Systems",
    "Far East Journal of Electronics and Communications",
    "Far East Journal of Mathematical Education",
    "International Journal of Nutrition and Dietetics",
    "International Journal of Numerical Methods and Applications",
    "International Journal of Materials Engineering and Technology",
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
    "Universal Journal of Mathematics and Mathematical Sciences",
    "Advances in Food & Dairy Sciences and Their Emerging Technologies (FDSET)"
]

# Editor Invitation journal list
EDITOR_JOURNALS = [
    "Mechanical Engineering and Physics",
    "Biological Sciences and Biotechnology",
    "Food & Dairy Sciences and their Emerging Technologies",
    "Editors - Far East Journal of Mathematical Sciences (FJMS)",
    "Oceanography, Marine Science and their Resources",  
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
        'sender_name': os.getenv("SENDER_NAME", "Pushpa Publishing House"),
        'webhook': {
            'url': os.getenv("WEBHOOK_URL", "")
        }
    }
    return config

config = load_config()
DEFAULT_UNSUBSCRIBE_BASE_URL = "https://pphmjopenaccess.com/unsubscribe?email="
SPAMMY_WORDS = [
    "offer", "discount", "free", "win", "winner", "cash", "prize",
    "buy now", "cheap", "limited time", "money", "urgent"
]
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
        
        sender_email = config['smtp2go']['sender']
        sender_name = st.session_state.sender_name
        formatted_sender = formataddr((sender_name, sender_email))
        data = {
            "api_key": config['smtp2go']['api_key'],
            "sender": formatted_sender,
            "sender_name": sender_name,
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

        if response.status_code != 200:
            st.error(
                f"SMTP2GO HTTP {response.status_code}: {response.text.strip()}"
            )
            return False, None

        try:
            result = response.json()
        except ValueError:
            st.error(
                f"SMTP2GO returned invalid JSON: {response.text.strip()}"
            )
            return False, None

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

def analyze_subject_csv(df):
    """Return subject wise delivery, open and click rates."""
    if df.empty or not {'Subject', 'Event', 'EmailID'}.issubset(df.columns):
        return pd.DataFrame()

    df['Subject'] = df['Subject'].astype(str)
    df['Event'] = df['Event'].astype(str)

    summary = []
    for subject, grp in df.groupby('Subject'):
        delivered = grp[grp['Event'].str.contains('deliver', case=False, na=False)]['EmailID'].nunique()
        opened = grp[grp['Event'].str.contains('open', case=False, na=False)]['EmailID'].nunique()
        clicked = grp[grp['Event'].str.contains('click', case=False, na=False)]['EmailID'].nunique()

        open_rate = (opened / delivered * 100) if delivered else 0
        click_rate = (clicked / opened * 100) if opened else 0

        summary.append({
            'Subject': subject,
            'Delivered': delivered,
            'Opened': opened,
            'Clicked': clicked,
            'Open Rate (%)': round(open_rate, 1),
            'Click Rate (%)': round(click_rate, 1)
        })

    result = pd.DataFrame(summary)
    if not result.empty:
        result.sort_values('Delivered', ascending=False, inplace=True)
    return result

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

# Journal management functions
def load_journals_from_firebase():
    """Load the list of journals from Firestore.

    If the document does not exist, it will be created using the default
    ``JOURNALS`` list. Loaded journals are merged with the defaults so that
    existing journals are preserved when new ones are added.
    """
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("journals").document("journal_list")
        doc = doc_ref.get()

        global JOURNALS
        if doc.exists:
            data = doc.to_dict()
            journals = data.get("journals", [])
            # Merge loaded journals with defaults and remove duplicates
            JOURNALS = sorted(set(JOURNALS + journals))
        else:
            # Document doesn't exist; initialise with default journals
            doc_ref.set({
                "journals": JOURNALS,
                "last_updated": datetime.now(),
                "updated_by": "admin",
            })
        return True
    except Exception as e:
        st.error(f"Failed to load journals: {str(e)}")
        return False


def add_journal_to_firebase(journal_name):
    """Add a journal to Firestore and update the in-memory list."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("journals").document("journal_list")
        doc = doc_ref.get()

        global JOURNALS
        # Start with existing journals from Firestore or the defaults
        journals = doc.to_dict().get("journals", JOURNALS.copy()) if doc.exists else JOURNALS.copy()

        if journal_name not in journals:
            journals.append(journal_name)
            # Ensure list is stored without duplicates
            doc_ref.set({
                "journals": sorted(set(journals)),
                "last_updated": datetime.now(),
                "updated_by": "admin",
            })

        if journal_name not in JOURNALS:
            JOURNALS.append(journal_name)
        return True
    except Exception as e:
        st.error(f"Failed to save journal: {str(e)}")
        return False

# Editor Invitation journal management
def load_editor_journals_from_firebase():
    """Load Editor Invitation journals from Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("editor_journals").document("journal_list")
        doc = doc_ref.get()

        global EDITOR_JOURNALS
        if doc.exists:
            data = doc.to_dict()
            journals = data.get("journals", [])
            EDITOR_JOURNALS = sorted(set(EDITOR_JOURNALS + journals))
        else:
            doc_ref.set({
                "journals": EDITOR_JOURNALS,
                "last_updated": datetime.now(),
                "updated_by": "admin",
            })
        return True
    except Exception as e:
        st.error(f"Failed to load editor journals: {str(e)}")
        return False


def add_editor_journal_to_firebase(journal_name):
    """Add a journal to the Editor Invitation list in Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("editor_journals").document("journal_list")
        doc = doc_ref.get()

        global EDITOR_JOURNALS
        journals = doc.to_dict().get("journals", EDITOR_JOURNALS.copy()) if doc.exists else EDITOR_JOURNALS.copy()

        if journal_name not in journals:
            journals.append(journal_name)
            doc_ref.set({
                "journals": sorted(set(journals)),
                "last_updated": datetime.now(),
                "updated_by": "admin",
            })

        if journal_name not in EDITOR_JOURNALS:
            EDITOR_JOURNALS.append(journal_name)
        return True
    except Exception as e:
        st.error(f"Failed to save editor journal: {str(e)}")
        return False

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

def update_subject_in_firebase(journal_name, old_subject, new_subject):
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("journal_subjects").document(journal_name)
        doc = doc_ref.get()
        subjects = doc.to_dict().get("subjects", []) if doc.exists else []
        if old_subject in subjects:
            subjects[subjects.index(old_subject)] = new_subject
            doc_ref.set({
                "subjects": subjects,
                "last_updated": datetime.now(),
                "updated_by": "admin"
            })
            st.session_state.journal_subjects[journal_name] = subjects
            return True
        return False
    except Exception as e:
        st.error(f"Failed to update subject: {str(e)}")
        return False

def delete_subject_from_firebase(journal_name, subject):
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("journal_subjects").document(journal_name)
        doc = doc_ref.get()
        subjects = doc.to_dict().get("subjects", []) if doc.exists else []
        if subject in subjects:
            subjects.remove(subject)
            doc_ref.set({
                "subjects": subjects,
                "last_updated": datetime.now(),
                "updated_by": "admin"
            })
            st.session_state.journal_subjects[journal_name] = subjects
            return True
        return False
    except Exception as e:
        st.error(f"Failed to delete subject: {str(e)}")
        return False

def save_block_settings():
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("block_settings").document("settings")
        doc_ref.set({
            "blocked_domains": st.session_state.blocked_domains,
            "blocked_emails": st.session_state.blocked_emails,
            "last_updated": datetime.now(),
            "updated_by": "admin",
        })
        return True
    except Exception as e:
        st.error(f"Failed to save block settings: {str(e)}")
        return False

def load_block_settings():
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("block_settings").document("settings")
        doc = doc_ref.get()
        if doc.exists:
            data = doc.to_dict()
            st.session_state.blocked_domains = data.get("blocked_domains", [])
            st.session_state.blocked_emails = data.get("blocked_emails", [])
        return True
    except Exception as e:
        st.error(f"Failed to load block settings: {str(e)}")
        return False

def is_email_blocked(email):
    email_lower = email.lower()
    domain = email_lower.split('@')[-1]
    if email_lower in [e.lower() for e in st.session_state.blocked_emails]:
        return True
    for d in st.session_state.blocked_domains:
        if d.lower() in domain:
            return True
    return False

def highlight_spam_words(text):
    words_found = []
    highlighted = text
    for word in SPAMMY_WORDS:
        pattern = re.compile(re.escape(word), re.IGNORECASE)
        if pattern.search(text):
            words_found.append(word)
            highlighted = pattern.sub(lambda m: f"<span style='color:red'>{m.group(0)}</span>", highlighted)
    return words_found, highlighted

# Check template spam score using Postmark Spamcheck API
from email.utils import formataddr, formatdate, make_msgid

def check_postmark_spam(template_html, subject="Test"):
    try:
        headers = [
            "From: test@example.com",
            "To: test@example.com",
            f"Subject: {subject}",
            f"Date: {formatdate(localtime=True)}",
            f"Message-ID: {make_msgid()}",
        ]
        message = "\n".join(headers) + "\n\n" + template_html
        response = requests.post(
            "https://spamcheck.postmarkapp.com/filter",
            json={"email": message, "options": "long"},
            timeout=10,
        )
        if response.status_code == 200:
            data = response.json()
            score = data.get("score")
            try:
                score = float(score)
            except (TypeError, ValueError):
                score = None
            return score, data.get("report")
        else:
            st.error(f"Spamcheck failed with status code {response.status_code}")
            return None, None
    except Exception as e:
        st.error(f"Spamcheck request failed: {str(e)}")
        return None, None

def clean_spam_report(report: str) -> str:
    """Remove irrelevant lines from the SpamAssassin report."""
    if not report:
        return ""
    skip_keywords = [
        "URIBL_BLOCKED",
        "URIBL_DBL_BLOCKED",
        "URIBL_ZEN_BLOCKED",
        "NO_RELAYS",
        "NO_RECEIVED",
    ]
    lines = [
        line
        for line in report.splitlines()
        if not any(k in line for k in skip_keywords)
    ]
    return "\n".join(lines).strip()


def spam_score_summary(score: float | None) -> str:
    """Return a short human-readable summary for the spam score."""
    if score is None:
        return "Unable to evaluate spam score."
    if score == 0:
        return "Your template is spam free."
    if score < 5:
        return "Your template has some spam elements, check before sending."
    return "Your template may be flagged as spam. Review the content."

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

# Persist completed campaign details to Firestore
def save_campaign_history(campaign_data):
    try:
        db = get_firestore_db()
        if not db:
            return False

        db.collection("campaign_history").add(campaign_data)
        return True
    except Exception as e:
        st.error(f"Failed to save campaign history: {str(e)}")
        return False


def load_campaign_history():
    try:
        db = get_firestore_db()
        if not db:
            return []

        hist_ref = db.collection("campaign_history")
        history = []
        for doc in hist_ref.stream():
            history.append(doc.to_dict())
        history.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
        st.session_state.campaign_history = history
        return history
    except Exception as e:
        st.error(f"Failed to load campaign history: {str(e)}")
        return []


# Helper to refresh template and subjects when journal changes
def refresh_journal_data():
    """Reload template and subjects for the currently selected journal."""
    journal = st.session_state.get("selected_journal")
    if not journal:
        return

    template = load_template_from_firebase(journal)
    if template is not None:
        st.session_state.template_content[journal] = template

    subjects = load_subjects_from_firebase(journal)
    if subjects is not None:
        st.session_state.journal_subjects[journal] = subjects

    st.session_state.last_refreshed_journal = journal

# Refresh data for Editor Invitation journals
def refresh_editor_journal_data():
    """Reload template and subjects for the currently selected editor journal."""
    journal = st.session_state.get("selected_editor_journal")
    if not journal:
        return

    template = load_template_from_firebase(journal)
    if template is not None:
        st.session_state.template_content[journal] = template

    subjects = load_subjects_from_firebase(journal)
    if subjects is not None:
        st.session_state.journal_subjects[journal] = subjects

    st.session_state.last_refreshed_journal = journal

# Email Campaign Section
def email_campaign_section():
    st.header("Email Campaign Management")

    if not st.session_state.block_settings_loaded:
        load_block_settings()
        st.session_state.block_settings_loaded = True

    if not st.session_state.journals_loaded:
        load_journals_from_firebase()
        st.session_state.journals_loaded = True

    if st.session_state.selected_journal is None:
        st.session_state.selected_journal = JOURNALS[0]
        st.session_state.show_journal_details = False
        update_sender_name()

    # Journal Selection
    col1 = st.columns(1)[0]
    with col1:
        selected_journal = st.selectbox(
            "Select Journal",
            JOURNALS,
            index=JOURNALS.index(st.session_state.selected_journal)
            if st.session_state.selected_journal in JOURNALS else 0,
            on_change=update_sender_name,
            key="selected_journal",
        )
        button_label = "Hide Subjects & Templete" if st.session_state.show_journal_details else "Load Subjects & Templete"
        if st.button(button_label):
            if st.session_state.show_journal_details:
                st.session_state.show_journal_details = False
            else:
                refresh_journal_data()
                st.session_state.show_journal_details = True
            st.experimental_rerun()

    
    
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
        st.text_input(
            "Sender Name",
            value=st.session_state.sender_name,
            key="sender_name"
        )

        blocked_domains_text = st.text_area(
            "Blocked Domains (one per line)",
            "\n".join(st.session_state.blocked_domains),
            key="blocked_domains_text"
        )
        blocked_emails_text = st.text_area(
            "Blocked Emails (one per line)",
            "\n".join(st.session_state.blocked_emails),
            key="blocked_emails_text"
        )
        if st.button("Save Block Settings", key="save_block_settings"):
            st.session_state.blocked_domains = [d.strip() for d in blocked_domains_text.splitlines() if d.strip()]
            st.session_state.blocked_emails = [e.strip() for e in blocked_emails_text.splitlines() if e.strip()]
            if save_block_settings():
                st.success("Block settings saved!")

    if st.session_state.show_journal_details:
        # Journal Subject Management
        st.subheader("Journal Subjects")
        subjects = st.session_state.journal_subjects.get(selected_journal, [])
        if subjects:
            for idx, subj in enumerate(subjects):
                col1, col2, col3 = st.columns([8, 1, 1])
                edited = col1.text_input(
                    f"Subject {idx+1}",
                    subj,
                    key=f"edit_subj_{selected_journal}_{idx}"
                )
                if edited:
                    spam_words, highlighted_edit = highlight_spam_words(edited)
                    if spam_words:
                        col1.warning("Your email may get spammed with this subject:")
                        col1.markdown(highlighted_edit, unsafe_allow_html=True)
                if col2.button("Update", key=f"update_subj_{selected_journal}_{idx}"):
                    if edited and edited != subj:
                        if update_subject_in_firebase(selected_journal, subj, edited):
                            st.success("Subject updated!")
                            st.experimental_rerun()
                if col3.button("Delete", key=f"delete_subj_{selected_journal}_{idx}"):
                    if delete_subject_from_firebase(selected_journal, subj):
                        st.success("Subject deleted!")
                        st.experimental_rerun()
        else:
            st.write("No subjects added yet")

        new_subject = st.text_input("Add Subject", key=f"subject_{selected_journal}")
        if new_subject:
            spam_words, highlighted_new = highlight_spam_words(new_subject)
            if spam_words:
                st.warning("Your email may get spammed with this subject:")
                st.markdown(highlighted_new, unsafe_allow_html=True)
        if st.button("Save Subject", key=f"save_subj_{selected_journal}"):
            if new_subject:
                if add_subject_to_firebase(selected_journal, new_subject):
                    st.success("Subject saved!")

        # Email Template Editor with ACE Editor
        st.subheader("Email Template Editor")
        template = get_journal_template(st.session_state.selected_journal)

        col1, col2 = st.columns(2)
        with col1:
            email_subject = st.text_input(
                "Email Subject",
                f"Call for Papers - {st.session_state.selected_journal}",
                key=f"email_subject_{selected_journal}"
            )
            if email_subject:
                spam_words, highlighted = highlight_spam_words(email_subject)
                if spam_words:
                    st.warning("Your email may get spammed with this subject:")
                    st.markdown(highlighted, unsafe_allow_html=True)

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

            if st.button("Check Spam Score"):
                cache_key = hashlib.md5((email_subject + email_body).encode("utf-8")).hexdigest()
                if cache_key in st.session_state.spam_check_cache:
                    score, report = st.session_state.spam_check_cache[cache_key]
                else:
                    score, report = check_postmark_spam(email_body, email_subject)
                    if score is not None:
                        st.session_state.spam_check_cache[cache_key] = (score, report)
                if score is not None:
                    st.session_state.template_spam_score[selected_journal] = score
                    st.session_state.template_spam_report[selected_journal] = clean_spam_report(report)
                    st.session_state.template_spam_summary[selected_journal] = spam_score_summary(score)

            if selected_journal in st.session_state.template_spam_score:
                score = st.session_state.template_spam_score[selected_journal]
                summary = st.session_state.template_spam_summary.get(selected_journal, "")
                if score < 4:
                    color = "green"
                elif score < 6:
                    color = "orange"
                else:
                    color = "red"
                st.markdown(
                    f"**Spam Score:** <span style='color:{color}'>{score}</span>",
                    unsafe_allow_html=True,
                )
                st.markdown("_Spam score under 5 is generally considered good (0 is best)._", unsafe_allow_html=True)
                if summary:
                    st.markdown(f"**{summary}**")
                st.text_area(
                    "Detail Report",
                    st.session_state.template_spam_report[selected_journal],
                    height=150,
                )

            st.info("""Available template variables:
            - $$Author_Name$$: Author's full name
            - $$Author_Address$$: All address lines before email
            - $$AuthorLastname$$: Author's last name (can be used in the subject)
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
            journal_name = st.session_state.get("selected_journal") or ""
            preview_html = preview_html.replace(
                "$$Journal_Name$$",
                journal_name
            )
            preview_html = preview_html.replace(
                "$$Unsubscribe_Link$$",
                "https://pphmjopenaccess.com/unsubscribe?email=john.doe@harvard.edu"
            )


            st.markdown(preview_html, unsafe_allow_html=True)

    # File Upload
    st.subheader("Recipient List")
    col_src, col_clock = st.columns([1, 2])
    with col_src:
        file_source = st.radio("Select file source", ["Local Upload", "Cloud Storage"])
    with col_clock:
        display_world_clocks()

    if file_source == "Local Upload":
        uploaded_file = st.file_uploader("Upload recipient list (CSV or TXT)", type=["csv", "txt"])
        if uploaded_file:
            if uploaded_file.name.endswith('.txt'):
                # Process the text file with the specific format
                file_content = read_uploaded_text(uploaded_file)
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
            st.info(f"Total emails loaded: {len(df)}")
            refresh_journal_data()

            if st.button("Save to Cloud"):
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
                        st.info(f"Total emails loaded: {len(df)}")
                        refresh_journal_data()
            else:
                st.info("No files found in Cloud Storage")
        
        
    # Send Options
    if 'current_recipient_list' in st.session_state and not st.session_state.campaign_paused:
        st.subheader("Campaign Options")
        st.markdown(f"**Journal:** {selected_journal}")

        unsubscribe_base_url = DEFAULT_UNSUBSCRIBE_BASE_URL

        subjects_for_journal = st.session_state.journal_subjects.get(selected_journal, [])
        selected_subjects = st.multiselect(
            "Select Subjects",
            subjects_for_journal,
            default=subjects_for_journal,
            key=f"subject_select_{selected_journal}"
        )

        st.markdown("<div class='send-ads-btn'>", unsafe_allow_html=True)
        send_ads_clicked = st.button("Send Ads", key="send_ads")
        st.markdown("</div>", unsafe_allow_html=True)
        if send_ads_clicked:
            if st.session_state.email_service == "SMTP2GO" and not config['smtp2go']['api_key']:
                st.error("SMTP2GO API key not configured")
                return
            elif st.session_state.email_service == "Amazon SES":
                if not st.session_state.ses_client:
                    initialize_ses()
                if not st.session_state.ses_client:
                    st.error("SES client not initialized. Please configure SES first.")
                    return

            email_body = st.session_state.get(
                f"editor_{selected_journal}", get_journal_template(selected_journal)
            )
            if email_body is None:
                email_body = get_journal_template(selected_journal)
            email_subject = st.session_state.get(
                f"email_subject_{selected_journal}", f"Call for Papers - {selected_journal}"
            )

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

                recipient_email = row.get('email', '')
                if is_email_blocked(recipient_email):
                    progress = (i + 1) / total_emails
                    progress_bar.progress(progress)
                    status_text.text(f"Skipping {i+1} of {total_emails}: {recipient_email} (blocked)")
                    update_campaign_progress(campaign_id, i+1, success_count)
                    continue

                # Build author address from all fields except email
                author_address = ""
                if row.get('department', ''):
                    author_address += f"{row['department']}<br>"
                if row.get('university', ''):
                    author_address += f"{row['university']}<br>"
                if row.get('country', ''):
                    author_address += f"{row['country']}<br>"

                email_content = email_body or ""
                sanitized_name = sanitize_author_name(str(row.get('name', '')))
                email_content = email_content.replace("$$Author_Name$$", sanitized_name)
                email_content = email_content.replace("$$Author_Address$$", author_address)

                # Extract last name if available
                if sanitized_name and ' ' in sanitized_name:
                    last_name = sanitized_name.split()[-1]
                else:
                    last_name = ''
                email_content = email_content.replace("$$AuthorLastname$$", last_name)

                email_content = email_content.replace("$$Department$$", str(row.get('department', '')))
                email_content = email_content.replace("$$University$$", str(row.get('university', '')))
                email_content = email_content.replace("$$Country$$", str(row.get('country', '')))
                email_content = email_content.replace("$$Author_Email$$", str(row.get('email', '')))
                journal_name = st.session_state.get("selected_journal") or ""
                email_content = email_content.replace("$$Journal_Name$$", journal_name)

                unsubscribe_link = f"{unsubscribe_base_url}{row.get('email', '')}"
                email_content = email_content.replace("$$Unsubscribe_Link$$", unsubscribe_link)

                plain_text = email_content.replace("<br>", "\n").replace("</p>", "\n\n").replace("<p>", "")

                subject_cycle = selected_subjects if selected_subjects else [email_subject]
                subject = subject_cycle[i % len(subject_cycle)]
                subject = subject.replace("$$AuthorLastname$$", last_name)

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
                        f"{st.session_state.sender_name} <{st.session_state.sender_email}>",
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
                save_campaign_history(campaign_data)

                progress_bar.progress(1.0)
                status_text.text("Campaign completed")
                st.success(f"Campaign completed! {success_count} of {total_emails} emails sent successfully.")

            else:
                st.warning(f"Campaign cancelled. {success_count} of {total_emails} emails were sent.")


def editor_invitation_section():
    st.header("Editor Invitation")

    if not st.session_state.block_settings_loaded:
        load_block_settings()
        st.session_state.block_settings_loaded = True

    if not st.session_state.editor_journals_loaded:
        load_editor_journals_from_firebase()
        st.session_state.editor_journals_loaded = True

    if st.session_state.selected_editor_journal is None:
        st.session_state.selected_editor_journal = EDITOR_JOURNALS[0]
        st.session_state.editor_show_journal_details = False
        update_editor_sender_name()

    # Journal Selection
    col1 = st.columns(1)[0]
    with col1:
        selected_editor_journal = st.selectbox(
            "Select Journal",
            EDITOR_JOURNALS,
            index=EDITOR_JOURNALS.index(st.session_state.selected_editor_journal)
            if st.session_state.selected_editor_journal in EDITOR_JOURNALS else 0,
            on_change=update_editor_sender_name,
            key="selected_editor_journal",
        )
        button_label = "Hide Subjects & Templete" if st.session_state.editor_show_journal_details else "Load Subjects & Templete"
        if st.button(button_label):
            if st.session_state.editor_show_journal_details:
                st.session_state.editor_show_journal_details = False
            else:
                refresh_editor_journal_data()
                st.session_state.editor_show_journal_details = True
            st.experimental_rerun()


    # Campaign settings in the sidebar
    with st.sidebar.expander("Campaign Settings", expanded=False):
        st.session_state.email_service = st.radio(
            "Select Email Service",
            ["SMTP2GO", "Amazon SES"],
            index=0 if st.session_state.email_service == "SMTP2GO" else 1,
            key="email_service_select_editor",
        )

        reply_address = st.text_input(
            f"Reply-to Address for {selected_editor_journal}",
            value=st.session_state.journal_reply_addresses.get(selected_editor_journal, ""),
            key=f"reply_editor_{selected_editor_journal}"
        )
        if st.button("Save Reply Address", key="save_reply_address_editor"):
            st.session_state.journal_reply_addresses[selected_editor_journal] = reply_address
            st.success("Reply address saved!")

        st.text_input(
            "Sender Email",
            value=st.session_state.sender_email,
            key="sender_email"
        )
        st.text_input(
            "Sender Name",
            value=st.session_state.sender_name,
            key="sender_name"
        )

        blocked_domains_text = st.text_area(
            "Blocked Domains (one per line)",
            "\n".join(st.session_state.blocked_domains),
            key="blocked_domains_text_editor"
        )
        blocked_emails_text = st.text_area(
            "Blocked Emails (one per line)",
            "\n".join(st.session_state.blocked_emails),
            key="blocked_emails_text_editor"
        )
        if st.button("Save Block Settings", key="save_block_settings_editor"):
            st.session_state.blocked_domains = [d.strip() for d in blocked_domains_text.splitlines() if d.strip()]
            st.session_state.blocked_emails = [e.strip() for e in blocked_emails_text.splitlines() if e.strip()]
            if save_block_settings():
                st.success("Block settings saved!")

    if st.session_state.editor_show_journal_details:
        st.subheader("Journal Subjects")
        subjects = st.session_state.journal_subjects.get(selected_editor_journal, [])
        if subjects:
            for idx, subj in enumerate(subjects):
                col1, col2, col3 = st.columns([8, 1, 1])
                edited = col1.text_input(
                    f"Subject {idx+1}",
                    subj,
                    key=f"edit_subj_editor_{selected_editor_journal}_{idx}"
                )
                if edited:
                    spam_words, highlighted_edit = highlight_spam_words(edited)
                    if spam_words:
                        col1.warning("Your email may get spammed with this subject:")
                        col1.markdown(highlighted_edit, unsafe_allow_html=True)
                if col2.button("Update", key=f"update_subj_editor_{selected_editor_journal}_{idx}"):
                    if edited and edited != subj:
                        if update_subject_in_firebase(selected_editor_journal, subj, edited):
                            st.success("Subject updated!")
                            st.experimental_rerun()
                if col3.button("Delete", key=f"delete_subj_editor_{selected_editor_journal}_{idx}"):
                    if delete_subject_from_firebase(selected_editor_journal, subj):
                        st.success("Subject deleted!")
                        st.experimental_rerun()
        else:
            st.write("No subjects added yet")

        new_subject = st.text_input("Add Subject", key=f"subject_editor_{selected_editor_journal}")
        if new_subject:
            spam_words, highlighted_new = highlight_spam_words(new_subject)
            if spam_words:
                st.warning("Your email may get spammed with this subject:")
                st.markdown(highlighted_new, unsafe_allow_html=True)
        if st.button("Save Subject", key=f"save_subj_editor_{selected_editor_journal}"):
            if new_subject:
                if add_subject_to_firebase(selected_editor_journal, new_subject):
                    st.success("Subject saved!")

        st.subheader("Email Template Editor")
        template = get_journal_template(st.session_state.selected_editor_journal)

        col1, col2 = st.columns(2)
        with col1:
            email_subject = st.text_input(
                "Email Subject",
                f"Invitation to Join the Editorial Board of {st.session_state.selected_editor_journal}",
                key=f"email_subject_editor_{selected_editor_journal}"
            )
            if email_subject:
                spam_words, highlighted = highlight_spam_words(email_subject)
                if spam_words:
                    st.warning("Your email may get spammed with this subject:")
                    st.markdown(highlighted, unsafe_allow_html=True)

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
                key=f"editor_editor_{selected_editor_journal}",
                height=400
            )

            if st.button("Save Template", key="save_template_editor"):
                if save_template_to_firebase(selected_editor_journal, email_body):
                    st.success("Template saved to cloud!")

            if st.button("Check Spam Score", key="check_spam_editor"):
                cache_key = hashlib.md5((email_subject + email_body).encode("utf-8")).hexdigest()
                if cache_key in st.session_state.spam_check_cache:
                    score, report = st.session_state.spam_check_cache[cache_key]
                else:
                    score, report = check_postmark_spam(email_body, email_subject)
                    if score is not None:
                        st.session_state.spam_check_cache[cache_key] = (score, report)
                if score is not None:
                    st.session_state.template_spam_score[selected_editor_journal] = score
                    st.session_state.template_spam_report[selected_editor_journal] = clean_spam_report(report)
                    st.session_state.template_spam_summary[selected_editor_journal] = spam_score_summary(score)

            if selected_editor_journal in st.session_state.template_spam_score:
                score = st.session_state.template_spam_score[selected_editor_journal]
                summary = st.session_state.template_spam_summary.get(selected_editor_journal, "")
                if score < 4:
                    color = "green"
                elif score < 6:
                    color = "orange"
                else:
                    color = "red"
                st.markdown(
                    f"**Spam Score:** <span style='color:{color}'>{score}</span>",
                    unsafe_allow_html=True,
                )
                st.markdown("_Spam score under 5 is generally considered good (0 is best)._", unsafe_allow_html=True)
                if summary:
                    st.markdown(f"**{summary}**")
                st.text_area(
                    "Detail Report",
                    st.session_state.template_spam_report[selected_editor_journal],
                    height=150,
                )

            st.info("""Available template variables:
            - $$Author_Name$$: Author's full name
            - $$Author_Address$$: All address lines before email
            - $$AuthorLastname$$: Author's last name (can be used in the subject)
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
            journal_name = st.session_state.get("selected_editor_journal") or ""
            preview_html = preview_html.replace(
                "$$Journal_Name$$",
                journal_name
            )
            preview_html = preview_html.replace(
                "$$Unsubscribe_Link$$",
                "https://pphmjopenaccess.com/unsubscribe?email=john.doe@harvard.edu"
            )

            st.markdown(preview_html, unsafe_allow_html=True)

    st.subheader("Recipient List")
    col_src, col_clock = st.columns([1, 2])
    with col_src:
        file_source = st.radio("Select file source", ["Local Upload", "Cloud Storage"], key="recipient_source_editor")
    with col_clock:
        display_world_clocks()

    if file_source == "Local Upload":
        uploaded_file = st.file_uploader("Upload recipient list (CSV or TXT)", type=["csv", "txt"], key="recipient_upload_editor")
        if uploaded_file:
            if uploaded_file.name.endswith('.txt'):
                file_content = read_uploaded_text(uploaded_file)
                entries = []
                current_entry = {}
                for line in file_content.split('\n'):
                    line = line.strip()
                    if line:
                        if '@' in line and '.' in line and ' ' not in line:
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
            st.info(f"Total emails loaded: {len(df)}")
            refresh_editor_journal_data()

            if st.button("Save to Cloud", key="save_recipient_editor"):
                if uploaded_file.name.endswith('.txt'):
                    if upload_to_firebase(file_content, uploaded_file.name):
                        st.success("File uploaded to Firebase successfully!")
                else:
                    csv_content = df.to_csv(index=False)
                    if upload_to_firebase(csv_content, uploaded_file.name):
                        st.success("File uploaded to Firebase successfully!")
    else:
        if st.button("Refresh File List", key="refresh_file_list_editor"):
            st.session_state.firebase_files = list_firebase_files()

        if 'firebase_files' in st.session_state and st.session_state.firebase_files:
            selected_file = st.selectbox("Select file from Firebase", st.session_state.firebase_files, key="select_file_editor")

            if st.button("Load File", key="load_file_editor"):
                file_content = download_from_firebase(selected_file)
                if file_content:
                    if selected_file.endswith('.txt'):
                        entries = []
                        current_entry = {}
                        for line in file_content.split('\n'):
                            line = line.strip()
                            if line:
                                if '@' in line and '.' in line and ' ' not in line:
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
                    st.info(f"Total emails loaded: {len(df)}")
                    refresh_editor_journal_data()
        else:
            st.info("No files found in Cloud Storage")

    if 'current_recipient_list' in st.session_state and not st.session_state.campaign_paused:
        st.subheader("Campaign Options")
        st.markdown(f"**Journal:** {selected_editor_journal}")

        unsubscribe_base_url = DEFAULT_UNSUBSCRIBE_BASE_URL

        subjects_for_journal = st.session_state.journal_subjects.get(selected_editor_journal, [])
        selected_subjects = st.multiselect(
            "Select Subjects",
            subjects_for_journal,
            default=subjects_for_journal,
            key=f"subject_select_editor_{selected_editor_journal}"
        )

        st.markdown("<div class='send-ads-btn'>", unsafe_allow_html=True)
        send_invitation_clicked = st.button("Send Invitation", key="send_invitation")
        st.markdown("</div>", unsafe_allow_html=True)
        if send_invitation_clicked:
            if st.session_state.email_service == "SMTP2GO" and not config['smtp2go']['api_key']:
                st.error("SMTP2GO API key not configured")
                return
            elif st.session_state.email_service == "Amazon SES":
                if not st.session_state.ses_client:
                    initialize_ses()
                if not st.session_state.ses_client:
                    st.error("SES client not initialized. Please configure SES first.")
                    return

            email_body = st.session_state.get(
                f"editor_{selected_editor_journal}", get_journal_template(selected_editor_journal)
            )
            if email_body is None:
                email_body = get_journal_template(selected_editor_journal)
            email_subject = st.session_state.get(
                f"email_subject_{selected_editor_journal}",
                f"Invitation to Join the Editorial Board of {selected_editor_journal}"
            )

            df = st.session_state.current_recipient_list
            total_emails = len(df)

            campaign_id = int(time.time())
            campaign_data = {
                'campaign_id': campaign_id,
                'journal_name': selected_editor_journal,
                'email_subjects': selected_subjects,
                'email_body': email_body,
                'email_service': st.session_state.email_service,
                'sender_email': st.session_state.sender_email,
                'reply_to': st.session_state.journal_reply_addresses.get(selected_editor_journal, None),
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

            progress_bar = st.progress(0)
            status_text = st.empty()
            cancel_button = st.button("Cancel Campaign", key="cancel_campaign_editor")

            success_count = 0
            email_ids = []

            reply_to = st.session_state.journal_reply_addresses.get(selected_editor_journal, None)

            for i, row in df.iterrows():
                if st.session_state.campaign_cancelled:
                    break

                recipient_email = row.get('email', '')
                if is_email_blocked(recipient_email):
                    progress = (i + 1) / total_emails
                    progress_bar.progress(progress)
                    status_text.text(f"Skipping {i+1} of {total_emails}: {recipient_email} (blocked)")
                    update_campaign_progress(campaign_id, i+1, success_count)
                    continue

                author_address = ""
                if row.get('department', ''):
                    author_address += f"{row['department']}<br>"
                if row.get('university', ''):
                    author_address += f"{row['university']}<br>"
                if row.get('country', ''):
                    author_address += f"{row['country']}<br>"

                email_content = email_body or ""
                sanitized_name = sanitize_author_name(str(row.get('name', '')))
                email_content = email_content.replace("$$Author_Name$$", sanitized_name)
                email_content = email_content.replace("$$Author_Address$$", author_address)

                if sanitized_name and ' ' in sanitized_name:
                    last_name = sanitized_name.split()[-1]
                else:
                    last_name = ''
                email_content = email_content.replace("$$AuthorLastname$$", last_name)

                email_content = email_content.replace("$$Department$$", str(row.get('department', '')))
                email_content = email_content.replace("$$University$$", str(row.get('university', '')))
                email_content = email_content.replace("$$Country$$", str(row.get('country', '')))
                email_content = email_content.replace("$$Author_Email$$", str(row.get('email', '')))
                journal_name = st.session_state.get("selected_editor_journal") or ""
                email_content = email_content.replace("$$Journal_Name$$", journal_name)

                unsubscribe_link = f"{unsubscribe_base_url}{row.get('email', '')}"
                email_content = email_content.replace("$$Unsubscribe_Link$$", unsubscribe_link)

                plain_text = email_content.replace("<br>", "\n").replace("</p>", "\n\n").replace("<p>", "")

                subject_cycle = selected_subjects if selected_subjects else [email_subject]
                subject = subject_cycle[i % len(subject_cycle)]
                subject = subject.replace("$$AuthorLastname$$", last_name)

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
                        f"{st.session_state.sender_name} <{st.session_state.sender_email}>",
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

                update_campaign_progress(campaign_id, i+1, success_count)

                if cancel_button:
                    st.session_state.campaign_cancelled = True
                    st.warning("Campaign cancellation requested...")
                    break

                time.sleep(0.1)

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

                campaign_data = {
                    'timestamp': datetime.now(),
                    'journal': selected_editor_journal,
                    'emails_sent': success_count,
                    'total_emails': total_emails,
                    'subject': ','.join(selected_subjects) if selected_subjects else email_subject,
                    'email_ids': ','.join(email_ids),
                    'service': st.session_state.email_service
                }
                st.session_state.campaign_history.append(campaign_data)
                save_campaign_history(campaign_data)

                progress_bar.progress(1.0)
                status_text.text("Campaign completed")
                st.success(f"Campaign completed! {success_count} of {total_emails} emails sent successfully.")
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
            file_content = read_uploaded_text(uploaded_file)
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
        plt.style.use("seaborn-v0_8-whitegrid")
        fig, ax = plt.subplots(figsize=(6, 3))
        categories = ['Good', 'Bad', 'Risky']
        counts = [stats['good'], stats['bad'], stats['risky']]
        colors = ['#4CAF50', '#F44336', '#9C27B0']
        
        bars = ax.bar(categories, counts, color=colors, edgecolor='black')
        ax.set_title('Email Verification Results')
        ax.set_ylabel('Count')

        # Add value labels on top of bars
        ax.bar_label(bars, padding=3)
        plt.tight_layout()
        
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

        if not st.session_state.campaign_history:
            load_campaign_history()

        if analytics_data:
            stats_list = analytics_data.get('history')
            if isinstance(stats_list, dict):
                if 'history' in stats_list:
                    stats_list = stats_list['history']
                elif 'stats' in stats_list:
                    stats_list = stats_list['stats']
                elif 'data' in stats_list:
                    stats_list = stats_list['data']
                else:
                    stats_list = [stats_list]
            if not stats_list:
                st.info("No statistics available yet.")
                return

            rename_map = {
                'opens': 'opens_unique',
                'open_unique': 'opens_unique',
                'unique_opens': 'opens_unique',
                'clicks': 'clicks_unique',
                'click_unique': 'clicks_unique',
                'unique_clicks': 'clicks_unique',
                'hard_bounce': 'hard_bounces',
                'soft_bounce': 'soft_bounces',
                'requests': 'sent',
                'emails_sent': 'sent',
                'processed': 'sent',
                'sent_total': 'sent',
                'delivered_total': 'delivered',
                'emails_delivered': 'delivered',
                'bounces': 'hard_bounces'
            }

            # Calculate totals (API may not always provide a 'totals' key)
            if 'totals' in analytics_data:
                totals = analytics_data['totals']
            else:
                df_totals = pd.DataFrame(stats_list)

                for old, new in rename_map.items():
                    if old in df_totals.columns and new not in df_totals.columns:
                        df_totals.rename(columns={old: new}, inplace=True)

                totals = {
                    'sent': df_totals['sent'].sum() if 'sent' in df_totals else 0,
                    'delivered': df_totals['delivered'].sum() if 'delivered' in df_totals else 0,
                    'opens_unique': df_totals['opens_unique'].sum() if 'opens_unique' in df_totals else 0,
                    'clicks_unique': df_totals['clicks_unique'].sum() if 'clicks_unique' in df_totals else 0,
                    'hard_bounces': df_totals['hard_bounces'].sum() if 'hard_bounces' in df_totals else 0,
                    'soft_bounces': df_totals['soft_bounces'].sum() if 'soft_bounces' in df_totals else 0,
                    'bounces': df_totals['bounces'].sum() if 'bounces' in df_totals else 0
                }

            # Overall metrics
            st.subheader("Overall Performance")
            col1, col2, col3, col4, col5, col6 = st.columns(6)
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
                bounce_total = totals.get('hard_bounces', 0) + totals.get('soft_bounces', 0)
                if bounce_total == 0:
                    bounce_total = totals.get('bounces', 0)
                st.metric("Bounced", bounce_total)
            with col6:
                bounce_rate = (bounce_total / totals['sent'] * 100) if totals['sent'] else 0
                if bounce_rate < 4:
                    status_color = "green"
                    status_text = "Healthy"
                elif bounce_rate < 7:
                    status_color = "orange"
                    status_text = "Warning"
                else:
                    status_color = "red"
                    status_text = "Unhealthy"
                st.markdown(f"**Status:** <span style='color:{status_color}'>{status_text}</span>", unsafe_allow_html=True)
            
            # Time series data
            st.subheader("Performance Over Time")
            df = pd.DataFrame(stats_list)

            for old, new in rename_map.items():
                if old in df.columns and new not in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            # Some API responses may not include a 'date' column (e.g. when only
            # totals are returned).  Gracefully handle these cases by checking
            # for alternate timestamp fields before attempting to convert to a
            # datetime index.
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            elif 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
            else:
                st.info("Date information not available in analytics data.")
            
            # Calculate rates
            if 'sent' in df and 'delivered' in df:
                df['delivery_rate'] = (df['delivered'] / df['sent']) * 100
            else:
                df['delivery_rate'] = 0
            if 'opens_unique' in df and 'delivered' in df:
                df['open_rate'] = (df['opens_unique'] / df['delivered']) * 100
            else:
                df['open_rate'] = 0
            if 'clicks_unique' in df and 'opens_unique' in df:
                df['click_rate'] = (df['clicks_unique'] / df['opens_unique']) * 100
            else:
                df['click_rate'] = 0
            
            tab1, tab2, tab3 = st.tabs(["Volume Metrics", "Engagement Rates", "Bounce & Complaints"])

            with tab1:
                cols = [c for c in ['sent', 'delivered', 'opens_unique', 'clicks_unique'] if c in df]
                if cols:
                    st.line_chart(df[cols])

            with tab2:
                cols = [c for c in ['delivery_rate', 'open_rate', 'click_rate'] if c in df]
                if cols:
                    st.line_chart(df[cols])

            with tab3:
                cols = [c for c in ['hard_bounces', 'soft_bounces', 'spam_complaints'] if c in df]
                if cols:
                    st.line_chart(df[cols])

            # Campaign details
            st.subheader("Recent Campaigns")
            if st.session_state.campaign_history:
                campaign_df = pd.DataFrame(st.session_state.campaign_history)

                # Calculate delivery rate and convert timestamp to IST
                campaign_df['delivery_rate'] = campaign_df.apply(
                    lambda x: (x['emails_sent'] / x['total_emails']) * 100,
                    axis=1
                )
                ist = pytz.timezone('Asia/Kolkata')
                campaign_df['timestamp'] = pd.to_datetime(campaign_df['timestamp'], utc=True).dt.tz_convert(ist)
                campaign_df['timestamp'] = campaign_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')

                # Prepare display dataframe in desired order
                display_df = campaign_df[
                    ['journal', 'timestamp', 'total_emails', 'emails_sent', 'delivery_rate', 'subject']
                ].copy()
                display_df.rename(columns={
                    'journal': 'Journal Name',
                    'timestamp': 'Date',
                    'total_emails': 'Total',
                    'emails_sent': 'Sent',
                    'delivery_rate': 'Delivery Rate',
                    'subject': 'Subject'
                }, inplace=True)

                st.dataframe(
                    display_df.sort_values('Date', ascending=False),
                    column_config={
                        'Delivery Rate': st.column_config.ProgressColumn(
                            'Delivery Rate',
                            format='%.1f%%',
                            min_value=0,
                            max_value=100
                        )
                    }
                )
            else:
                st.info("No campaign history available")

            # Show bounce details if available
            if analytics_data.get('bounces'):
                st.subheader("Bounces")
                bounce_data = analytics_data['bounces']
                # The bounce endpoint may return aggregated values rather than
                # a list of records. Handle both possibilities gracefully.
                if isinstance(bounce_data, dict):
                    bounce_df = pd.DataFrame([bounce_data])
                else:
                    bounce_df = pd.DataFrame(bounce_data)
                st.dataframe(bounce_df)

            st.markdown("---")
            st.subheader("Subject-wise CSV Analysis")
            csv_file = st.file_uploader("Upload SMTP2Go Activity CSV", type=["csv"], key="activity_csv")
            if csv_file and st.button("Analyze CSV"):
                csv_df = pd.read_csv(csv_file)
                result = analyze_subject_csv(csv_df)
                if result.empty:
                    st.warning("Uploaded CSV is missing required data.")
                else:
                    st.dataframe(result)
                    rates = result.set_index('Subject')[['Open Rate (%)', 'Click Rate (%)']]
                    st.bar_chart(rates)
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
        hist_json = hist_resp.json()
        history_data = hist_json.get("data", hist_json)
        if isinstance(history_data, dict):
            history_data = history_data.get("history", history_data.get("stats", history_data.get("data", [])))

        # Bounce statistics
        bounce_resp = requests.post(f"{base_url}/stats/email_bounces", json=payload)
        bounce_resp.raise_for_status()
        bounce_json = bounce_resp.json()
        bounces_data = bounce_json.get("data", bounce_json)
        if isinstance(bounces_data, dict):
            bounces_data = bounces_data.get("bounces", bounces_data.get("data", []))

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

        if not st.session_state.campaign_history:
            load_campaign_history()

        if analytics_data:
            stats_list = analytics_data.get('history')
            if isinstance(stats_list, dict):
                stats_list = [stats_list]
            if not stats_list:
                st.info("No statistics available yet.")
                return

            # Process data for display
            df = pd.DataFrame(stats_list)

            # The history endpoint may sometimes return only totals without a
            # specific date field. Handle that scenario gracefully by checking
            # for possible timestamp columns before indexing.
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            elif 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
            else:
                st.info("Date information not available in analytics data.")
            
            # Calculate rates
            if 'sent' in df and 'delivered' in df:
                df['delivery_rate'] = (df['delivered'] / df['sent']) * 100
            else:
                df['delivery_rate'] = 0
            if 'opens_unique' in df and 'delivered' in df:
                df['open_rate'] = (df['opens_unique'] / df['delivered']) * 100
            else:
                df['open_rate'] = 0
            if 'clicks_unique' in df and 'opens_unique' in df:
                df['click_rate'] = (df['clicks_unique'] / df['opens_unique']) * 100
            else:
                df['click_rate'] = 0
            
            # Summary metrics
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            with col1:
                st.metric("Total Sent", df['sent'].sum() if 'sent' in df else 0)
            with col2:
                delivered_total = df['delivered'].sum() if 'delivered' in df else 0
                st.metric("Delivered", delivered_total,
                         f"{df['delivery_rate'].mean():.1f}%")
            with col3:
                opened_total = df['opens_unique'].sum() if 'opens_unique' in df else 0
                st.metric("Opened", opened_total,
                         f"{df['open_rate'].mean():.1f}%")
            with col4:
                clicked_total = df['clicks_unique'].sum() if 'clicks_unique' in df else 0
                st.metric("Clicked", clicked_total,
                         f"{df['click_rate'].mean():.1f}%")
            with col5:
                bounce_total = 0
                if 'hard_bounces' in df:
                    bounce_total += df['hard_bounces'].sum()
                if 'soft_bounces' in df:
                    bounce_total += df['soft_bounces'].sum()
                st.metric("Bounced", bounce_total)
            with col6:
                if df['sent'].sum() > 0:
                    bounce_rate = (bounce_total / df['sent'].sum()) * 100
                else:
                    bounce_rate = 0
                if bounce_rate < 4:
                    status_color = "green"
                    status_text = "Healthy"
                elif bounce_rate < 7:
                    status_color = "orange"
                    status_text = "Warning"
                else:
                    status_color = "red"
                    status_text = "Unhealthy"
                st.markdown(f"**Status:** <span style='color:{status_color}'>{status_text}</span>", unsafe_allow_html=True)
            
            # Time series charts
            st.subheader("Performance Over Time")
            tab1, tab2, tab3 = st.tabs(["Volume Metrics", "Engagement Rates", "Bounce & Complaints"])

            with tab1:
                cols = [c for c in ['sent', 'delivered', 'opens_unique', 'clicks_unique'] if c in df]
                if cols:
                    st.line_chart(df[cols])

            with tab2:
                cols = [c for c in ['delivery_rate', 'open_rate', 'click_rate'] if c in df]
                if cols:
                    st.line_chart(df[cols])

            with tab3:
                cols = [c for c in ['hard_bounces', 'soft_bounces', 'spam_complaints'] if c in df]
                if cols:
                    st.line_chart(df[cols])
            
            # Campaign details
            st.subheader("Recent Campaigns")
            if st.session_state.campaign_history:
                campaign_df = pd.DataFrame(st.session_state.campaign_history)
                st.dataframe(campaign_df.sort_values('timestamp', ascending=False))
            else:
                st.info("No campaign history available")

            if analytics_data.get('bounces'):
                st.subheader("Bounces")
                bounce_data = analytics_data['bounces']
                if isinstance(bounce_data, dict):
                    bounce_df = pd.DataFrame([bounce_data])
                else:
                    bounce_df = pd.DataFrame(bounce_data)
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

    # Navigation with additional links and heading in the sidebar
    with st.sidebar:
        st.markdown("## PPH Email Manager")
        app_mode = st.selectbox("Select Mode", ["Email Campaign", "Editor Invitation", "Verify Emails", "Analytics"])
        st.markdown("---")
        st.markdown("### Quick Links")
        st.markdown("[📊 Email Reports](https://app-us.smtp2go.com/reports/activity/)", unsafe_allow_html=True)
        st.markdown("[📝 Entry Manager](https://pphentry.onrender.com)", unsafe_allow_html=True)
    
    # Initialize services
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    if app_mode == "Email Campaign":
        email_campaign_section()
    elif app_mode == "Editor Invitation":
        editor_invitation_section()
    elif app_mode == "Verify Emails":
        email_verification_section()
    else:
        analytics_section()

if __name__ == "__main__":
    main()
