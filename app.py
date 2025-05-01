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
    page_title="Academic Email Marketing Suite", 
    layout="wide",
    page_icon="✉️",
    menu_items={
        'About': "### Academic Email Marketing Suite\n\nDeveloped by Prakash (cpsharma.com)"
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
        st.title("Academic Email Marketing Suite - Login")
        
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
    if 'verified_content' not in st.session_state:
        st.session_state.verified_content = ""

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
        
        # Remove empty values from the dictionary
        creds_dict = {k: v for k, v in creds_dict.items() if v}
        
        if not creds_dict.get('private_key'):
            st.error("Firebase private key not found in environment variables")
            return None
            
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
            st.error("SMTP2GO API key not configured")
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
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            st.error(f"Verification API error: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Verification failed: {str(e)}")
        return None

def check_millionverifier_quota(api_key):
    url = f"https://api.millionverifier.com/api/v3/?api={api_key}&cmd=remaining"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                return data.get('remaining', 0)
            elif isinstance(data, int):
                return data
            else:
                return 0
        else:
            st.error(f"Quota check failed: {response.status_code}")
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
                    if current_entry:  # Only add if we have some data
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
            return pd.DataFrame(columns=['name', 'department', 'university', 'country', 'email', 'verification_result', 'verification_details'])
        
        results = []
        for email in df['email']:
            result = verify_email(email, api_key)
            results.append(result)
            time.sleep(0.1)  # Rate limiting
        
        df['verification_result'] = [r.get('result', 'error') if r else 'error' for r in results]
        df['verification_details'] = [str(r) if r else 'error' for r in results]
        
        return df
    except Exception as e:
        st.error(f"Failed to process email list: {str(e)}")
        return None

# Analytics Functions
def fetch_smtp2go_analytics():
    try:
        if not config['smtp2go']['api_key']:
            st.error("SMTP2GO API key not configured")
            return None
        
        # Fetch stats from SMTP2GO
        stats_url = "https://api.smtp2go.com/v3/stats/email_summary"
        data = {
            'api_key': config['smtp2go']['api_key'],
            'days': 30
        }
        
        response = requests.post(stats_url, json=data)
        if response.status_code == 200:
            data = response.json()
            if data.get('data'):
                return data['data']
            else:
                st.error(f"Failed to fetch SMTP2GO analytics: {data.get('error', 'Unknown error')}")
                return None
        else:
            st.error(f"SMTP2GO API error: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error fetching SMTP2GO analytics: {str(e)}")
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
    
    # Email Service Selection
    st.session_state.email_service = st.radio(
        "Select Email Service",
        ["SMTP2GO", "Amazon SES"],
        index=0 if st.session_state.email_service == "SMTP2GO" else 1
    )
    
    # Journal Reply Address Configuration
    with st.expander("Journal Reply Address Configuration"):
        reply_address = st.text_input(
            f"Reply-to Address for {selected_journal}",
            value=st.session_state.journal_reply_addresses.get(selected_journal, ""),
            key=f"reply_{selected_journal}"
        )
        if st.button("Save Reply Address"):
            st.session_state.journal_reply_addresses[selected_journal] = reply_address
            st.success("Reply address saved!")
    
    # Email Template Editor with ACE Editor
    st.subheader("Email Template Editor")
    template = get_journal_template(st.session_state.selected_journal)
    
    col1, col2 = st.columns(2)
    with col1:
        email_subject = st.text_input("Email Subject", 
                                   f"Call for Papers - {st.session_state.selected_journal}")
    
    # ACE Editor
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
        
        st.info("""Available template variables:
        - $$Author_Name$$: Author's full name
        - $$Author_Address$$: All address lines before email
        - $$Department$$: Author's department
        - $$University$$: Author's university
        - $$Country$$: Author's country
        - $$Author_Email$$: Author's email
        - $$Journal_Name$$: Selected journal name
        - $$Unsubscribe_Link$$: Unsubscribe link""")
    
    with preview_col:
        st.markdown("**Preview**")
        preview_html = email_body.replace("$$Author_Name$$", "Professor John Doe")
        preview_html = preview_html.replace("$$Author_Address$$", "Department of Computer Science<br>Harvard University<br>United States")
        preview_html = preview_html.replace("$$Department$$", "Computer Science")
        preview_html = preview_html.replace("$$University$$", "Harvard University")
        preview_html = preview_html.replace("$$Country$$", "United States")
        preview_html = preview_html.replace("$$Author_Email$$", "john.doe@harvard.edu")
        preview_html = preview_html.replace("$$Journal_Name$$", st.session_state.selected_journal)
        preview_html = preview_html.replace("$$Unsubscribe_Link$$", "https://pphmjopenaccess.com/unsubscribe?email=john.doe@harvard.edu")
        
        st.markdown(preview_html, unsafe_allow_html=True)
    
    # File Upload
    st.subheader("Recipient List")
    file_source = st.radio("Select file source", ["Local Upload", "Firebase Storage"])
    
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
                    if upload_to_firebase(StringIO(file_content), uploaded_file.name):
                        st.success("File uploaded to Firebase successfully!")
                else:
                    if upload_to_firebase(uploaded_file, uploaded_file.name):
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
            st.info("No files found in Firebase Storage")
    
    # Send Options
    if 'current_recipient_list' in st.session_state:
        st.subheader("Campaign Options")
        
        sender_email = st.text_input("Sender Email", config['smtp2go']['sender'] if st.session_state.email_service == "SMTP2GO" else "")
        unsubscribe_base_url = st.text_input("Unsubscribe Base URL", 
                                           "https://pphmjopenaccess.com/unsubscribe?email=")
        
        send_option = st.radio("Send Option", ["Send Now", "Schedule"])
        
        if send_option == "Schedule":
            schedule_time = st.datetime_input("Schedule Time", 
                                            datetime.now() + timedelta(days=1))
        
        if st.button("Start Campaign"):
            if st.session_state.email_service == "SMTP2GO" and not st.session_state.smtp2go_initialized:
                st.error("SMTP2GO not initialized. Please check your configuration.")
                return
            elif st.session_state.email_service == "Amazon SES" and not st.session_state.ses_client:
                st.error("SES client not initialized. Please configure SES first.")
                return
            
            df = st.session_state.current_recipient_list
            total_emails = len(df)
            progress_bar = st.progress(0)
            status_text = st.empty()
            success_count = 0
            email_ids = []
            
            reply_to = st.session_state.journal_reply_addresses.get(selected_journal, None)
            
            for i, row in df.iterrows():
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
                email_content = email_content.replace("$$Department$$", str(row.get('department', '')))
                email_content = email_content.replace("$$University$$", str(row.get('university', '')))
                email_content = email_content.replace("$$Country$$", str(row.get('country', '')))
                email_content = email_content.replace("$$Author_Email$$", str(row.get('email', '')))
                email_content = email_content.replace("$$Journal_Name$$", st.session_state.selected_journal)
                
                unsubscribe_link = f"{unsubscribe_base_url}{row.get('email', '')}"
                email_content = email_content.replace("$$Unsubscribe_Link$$", unsubscribe_link)
                
                plain_text = email_content.replace("<br>", "\n").replace("</p>", "\n\n").replace("<p>", "")
                
                if st.session_state.email_service == "SMTP2GO":
                    success, email_id = send_email_via_smtp2go(
                        row.get('email', ''),
                        email_subject,
                        email_content,
                        plain_text,
                        unsubscribe_link,
                        reply_to
                    )
                else:
                    response, email_id = send_ses_email(
                        st.session_state.ses_client,
                        sender_email,
                        row.get('email', ''),
                        email_subject,
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
                
                # Rate limiting
                time.sleep(0.1)
            
            # Record campaign details
            campaign_data = {
                'timestamp': datetime.now(),
                'journal': selected_journal,
                'emails_sent': success_count,
                'total_emails': total_emails,
                'subject': email_subject,
                'email_ids': ','.join(email_ids),
                'service': st.session_state.email_service
            }
            st.session_state.campaign_history.append(campaign_data)
            
            st.success(f"Campaign completed! {success_count} of {total_emails} emails sent successfully.")
            show_email_analytics()

# Email Verification Section
def email_verification_section():
    st.header("Email Verification")
    
    # Check verification quota
    if config['millionverifier']['api_key']:
        with st.spinner("Checking verification quota..."):
            remaining_quota = check_millionverifier_quota(config['millionverifier']['api_key'])
            st.metric("Remaining Verification Credits", remaining_quota)
    else:
        st.warning("MillionVerifier API key not configured")
    
    # File Upload for Verification
    st.subheader("Email List Verification")
    file_source = st.radio("Select file source for verification", ["Local Upload", "Firebase Storage"])
    
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
                    if result_df is not None and not result_df.empty:
                        st.session_state.verified_emails = result_df
                        st.dataframe(result_df)
                        
                        # Filter out invalid emails
                        valid_emails = result_df[result_df['verification_result'].str.lower() == 'valid']
                        
                        if not valid_emails.empty:
                            # Convert back to original format
                            output_content = ""
                            for _, row in valid_emails.iterrows():
                                # Build the address block
                                output_content += f"{row['name']}\n"
                                if pd.notna(row.get('department', '')):
                                    output_content += f"{row['department']}\n"
                                if pd.notna(row.get('university', '')):
                                    output_content += f"{row['university']}\n"
                                if pd.notna(row.get('country', '')):
                                    output_content += f"{row['country']}\n"
                                output_content += f"{row['email']}\n\n"
                            
                            st.session_state.verified_content = output_content
                            verified_filename = f"verified_{uploaded_file.name}"
                            
                            # Display download button
                            st.download_button(
                                "Download Verified List",
                                output_content,
                                file_name=verified_filename,
                                mime="text/plain"
                            )
                            
                            if st.button("Save Verified List to Firebase"):
                                if upload_to_firebase(StringIO(output_content), verified_filename):
                                    st.success("Verified file uploaded to Firebase!")
                        else:
                            st.warning("No valid emails found in the list")
                    else:
                        st.error("Failed to process email list or no emails found")
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
                    if result_df is not None and not result_df.empty:
                        st.session_state.verified_emails = result_df
                        st.dataframe(result_df)
                        
                        # Filter out invalid emails
                        valid_emails = result_df[result_df['verification_result'].str.lower() == 'valid']
                        
                        if not valid_emails.empty:
                            # Convert back to original format
                            output_content = ""
                            for _, row in valid_emails.iterrows():
                                output_content += f"{row['name']}\n"
                                if pd.notna(row.get('department', '')):
                                    output_content += f"{row['department']}\n"
                                if pd.notna(row.get('university', '')):
                                    output_content += f"{row['university']}\n"
                                if pd.notna(row.get('country', '')):
                                    output_content += f"{row['country']}\n"
                                output_content += f"{row['email']}\n\n"
                            
                            st.session_state.verified_content = output_content
                            verified_filename = f"verified_{selected_file}"
                            
                            st.download_button(
                                "Download Verified List",
                                output_content,
                                file_name=verified_filename,
                                mime="text/plain"
                            )
                            
                            if st.button("Save Verified List to Firebase"):
                                if upload_to_firebase(StringIO(output_content), verified_filename):
                                    st.success("Verified file uploaded to Firebase!")
                        else:
                            st.warning("No valid emails found in the list")
                    else:
                        st.error("Failed to process email list or no emails found")
        else:
            st.info("No files found in Firebase Storage")
    
    # Verification Analytics
    if 'verified_emails' in st.session_state:
        st.subheader("Verification Results")
        df = st.session_state.verified_emails
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Emails", len(df))
        with col2:
            valid = len(df[df['verification_result'].str.lower() == 'valid'])
            st.metric("Valid Emails", valid)
        with col3:
            invalid = len(df[df['verification_result'].str.lower() == 'invalid'])
            st.metric("Invalid Emails", invalid)
        with col4:
            st.metric("Quality Score", f"{round(valid/len(df)*100 if len(df) > 0 else 0, 1)}%")
        
        result_counts = df['verification_result'].value_counts()
        st.bar_chart(result_counts)

def analytics_section():
    st.header("Comprehensive Email Analytics")
    
    if st.session_state.email_service == "SMTP2GO":
        st.info("SMTP2GO Analytics Dashboard")
        
        # Fetch detailed analytics
        analytics_data = fetch_smtp2go_analytics()
        
        if analytics_data and 'totals' in analytics_data:
            # Overall metrics
            st.subheader("Overall Performance")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Sent", analytics_data['totals'].get('sent', 0))
            with col2:
                delivered = analytics_data['totals'].get('delivered', 0)
                sent = analytics_data['totals'].get('sent', 1)
                delivery_rate = (delivered / sent) * 100 if sent > 0 else 0
                st.metric("Delivered", delivered, f"{delivery_rate:.1f}%")
            with col3:
                opens_unique = analytics_data['totals'].get('opens_unique', 0)
                open_rate = (opens_unique / delivered) * 100 if delivered > 0 else 0
                st.metric("Opened", opens_unique, f"{open_rate:.1f}%")
            with col4:
                clicks_unique = analytics_data['totals'].get('clicks_unique', 0)
                click_rate = (clicks_unique / opens_unique) * 100 if opens_unique > 0 else 0
                st.metric("Clicked", clicks_unique, f"{click_rate:.1f}%")
            with col5:
                bounces = analytics_data['totals'].get('hard_bounces', 0) + analytics_data['totals'].get('soft_bounces', 0)
                st.metric("Bounced", bounces)
            
            # Time series data
            if 'stats' in analytics_data and analytics_data['stats']:
                st.subheader("Performance Over Time")
                df = pd.DataFrame(analytics_data['stats'])
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
            else:
                st.info("No time series data available")
            
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
        else:
            st.info("No analytics data available yet. Please send some emails first.")
    else:
        st.info("Amazon SES Analytics would be displayed here")
        show_email_analytics()

def main():
    # Check authentication
    check_auth()
    
    # Main app for authenticated users
    st.title(f"Academic Email Marketing Suite - Welcome admin")
    
    # Navigation
    app_mode = st.sidebar.selectbox("Select Mode", ["Email Campaign", "Verify Emails", "Analytics"])
    
    # Initialize services
    if not st.session_state.ses_client and st.session_state.email_service == "Amazon SES":
        initialize_ses()
    
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    if not st.session_state.smtp2go_initialized and st.session_state.email_service == "SMTP2GO":
        initialize_smtp2go()
    
    if app_mode == "Email Campaign":
        email_campaign_section()
    elif app_mode == "Verify Emails":
        email_verification_section()
    else:
        analytics_section()

if __name__ == "__main__":
    main()
