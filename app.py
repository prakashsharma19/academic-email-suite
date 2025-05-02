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

# App Configuration
st.set_page_config(
    page_title="PPH Email Manager", 
    layout="wide",
    page_icon="✉️",
    menu_items={
        'About': "### Academic Email Management Suite\n\nDeveloped by Prakash (cpsharma.com)"
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
    if 'smtp2go_initialized' not in st.session_state:
        st.session_state.smtp2go_initialized = False
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

init_session_state()

# Journal Data
JOURNALS = [
    "Computer Science and Artificial Intelligence",
    "Advanced Studies in Artificial Intelligence",
    "Advances in Computer Science and Engineering",
    "Far East Journal of Experimental and Theoretical Artificial Intelligence",
    "Advances and Applications in Fluid Mechanics",
    "Advances in Fuzzy Sets and Systems",
    "Far East Journal of Electronics and Communications",
    "Far East Journal of Mechanical Engineering and Physics",
    "International Journal of Nutrition and Dietetics",
    "International Journal of Materials Engineering and Technology",
    "JP Journal of Solids and Structures",
    "Advances and Applications in Discrete Mathematics",
    "Advances and Applications in Statistics",
    "Far East Journal of Applied Mathematics",
    "Far East Journal of Dynamical Systems",
    "Far East Journal of Mathematical Sciences (FJMS)",
    "Far East Journal of Theoretical Statistics",
    "JP Journal of Algebra, Number Theory and Applications",
    "JP Journal of Biostatistics",
    "JP Journal of Fixed Point Theory and Applications",
    "JP Journal of Heat and Mass Transfer",
    "Surveys in Mathematics and Mathematical Sciences",
    "Universal Journal of Mathematics and Mathematical Sciences"
]

# Default email template
def get_journal_template(journal_name):
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
            'client_x509_cert_url': os.getenv("FIREBASE_CLIENT_CERT_URL", "")
        },
        'smtp2go': {
            'api_key': os.getenv("SMTP2GO_API_KEY", ""),
            'sender': os.getenv("SMTP2GO_SENDER_EMAIL", "noreply@cpsharma.com")
        },
        'webhook': {
            'url': os.getenv("WEBHOOK_URL", "")
        }
    }
    return config

config = load_config()

# Initialize Firebase Storage
def initialize_firebase():
    try:
        if not all(config['firebase'].values()):
            st.error("Firebase credentials not fully configured. Please check environment variables.")
            return None
            
        creds_dict = {
            "type": config['firebase']['type'],
            "project_id": config['firebase']['project_id'],
            "private_key_id": config['firebase']['private_key_id'],
            "private_key": config['firebase']['private_key'],
            "client_email": config['firebase']['client_email'],
            "client_id": config['firebase']['client_id'],
            "auth_uri": config['firebase']['auth_uri'],
            'token_uri': config['firebase']['token_uri'],
            "auth_provider_x509_cert_url": config['firebase']['auth_provider_x509_cert_url'],
            "client_x509_cert_url": config['firebase']['client_x509_cert_url']
        }
        
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        storage_client = storage.Client(credentials=credentials)
        
        st.session_state.firebase_storage = storage_client
        st.session_state.firebase_initialized = True
        return storage_client
    except Exception as e:
        st.error(f"Firebase initialization failed: {str(e)}")
        return None

# Initialize SES Client
def initialize_ses():
    try:
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

# Initialize SMTP2GO
def initialize_smtp2go():
    try:
        if config['smtp2go']['api_key']:
            st.session_state.smtp2go_initialized = True
            return True
        else:
            st.error("SMTP API key not configured")
            return False
    except Exception as e:
        st.error(f"SMTP2GO initialization failed: {str(e)}")
        return False

# Firebase Storage Functions
def upload_to_firebase(file, file_name, folder="email_lists"):
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    try:
        bucket = st.session_state.firebase_storage.bucket()
        blob = bucket.blob(f"{folder}/{file_name}")
        
        if isinstance(file, StringIO):
            content = file.getvalue().encode('utf-8')
        else:
            content = file.getvalue()
            
        blob.upload_from_string(content, content_type='text/csv')
        return True
    except Exception as e:
        st.error(f"Failed to upload file: {str(e)}")
        return False

def download_from_firebase(file_name, folder="email_lists"):
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    try:
        bucket = st.session_state.firebase_storage.bucket()
        blob = bucket.blob(f"{folder}/{file_name}")
        content = blob.download_as_text()
        return content
    except Exception as e:
        st.error(f"Failed to download file: {str(e)}")
        return None

def list_firebase_files(folder="email_lists"):
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    try:
        bucket = st.session_state.firebase_storage.bucket()
        blobs = bucket.list_blobs(prefix=folder)
        return [blob.name.split('/')[-1] for blob in blobs if not blob.name.endswith('/')]
    except Exception as e:
        st.error(f"Failed to list files: {str(e)}")
        return []

# Email Functions
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
        for email in df['email']:
            result = verify_email(email, api_key)
            if result:
                results.append(result)
            else:
                results.append({'result': 'error'})
            time.sleep(0.1)  # Rate limiting
        
        df['verification_result'] = [r.get('result', 'error') if r else 'error' for r in results]
        
        return df
    except Exception as e:
        st.error(f"Failed to process email list: {str(e)}")
        return pd.DataFrame()

def generate_verified_txt_file(df):
    """Convert verified emails back to original text format"""
    if df.empty:
        return ""
    
    output = ""
    valid_statuses = ['valid', 'ok', 'good']  # Allowed statuses
    
    for _, row in df.iterrows():
        if row['verification_result'].lower() in valid_statuses:
            output += f"{row.get('name', '')}\n"
            output += f"{row.get('department', '')}\n"
            output += f"{row.get('university', '')}\n"
            output += f"{row.get('country', '')}\n"
            output += f"{row.get('email', '')}\n\n"  # Extra newline between entries
    
    return output.strip()  # Remove final extra newline

# Analytics Functions
def fetch_smtp2go_analytics():
    try:
        if not config['smtp2go']['api_key']:
            st.error("SMTP API key not configured")
            return None
        
        # Fetch stats from SMTP2GO
        stats_url = "https://api.smtp2go.com/v3/stats/email_summary"
        params = {
            'api_key': config['smtp2go']['api_key'],
            'days': 30
        }
        
        response = requests.get(stats_url, params=params)
        data = response.json()
        
        if data.get('data'):
            return data['data']
        else:
            st.error(f"Failed to fetch SMTP analytics: {data.get('error', 'Unknown error')}")
            return None
    except Exception as e:
        st.error(f"Error fetching SMTP analytics: {str(e)}")
        return None

def show_email_analytics():
    st.subheader("Email Campaign Analytics Dashboard")
    
    if st.session_state.email_service == "SMTP2GO":
        analytics_data = fetch_smtp2go_analytics()
        
        if analytics_data:
            # Process data for display
            df = pd.DataFrame(analytics_data['stats'])
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
            tab1, tab2, tab3 = st.tabs(["Volume Metrics", "Engagement Rates", "Bounce & C