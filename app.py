import streamlit as st
import boto3
import pandas as pd
import datetime
import time
import requests
import json
import os
import sys
import subprocess
from datetime import datetime, timedelta
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import base64
from pathlib import Path

# App Configuration - Light Theme
st.set_page_config(
    page_title="Academic Email Marketing Suite", 
    layout="wide",
    page_icon="✉️"
)

# Apply light theme
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
    </style>
    """
    st.markdown(light_theme, unsafe_allow_html=True)

set_light_theme()

# Authentication Configuration
def setup_authentication():
    with open('auth_config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = Authenticator(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )
    
    return authenticator

# Initialize session state variables
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
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'current_user' not in st.session_state:
        st.session_state.current_user = None

init_session_state()

# Load configuration from secrets or environment variables
@st.cache_data
def load_config():
    config = {
        'aws': {
            'access_key': os.getenv("AWS_ACCESS_KEY_ID", st.secrets.get("aws.ACCESS_KEY_ID", "")),
            'secret_key': os.getenv("AWS_SECRET_ACCESS_KEY", st.secrets.get("aws.SECRET_ACCESS_KEY", "")),
            'region': os.getenv("AWS_REGION", st.secrets.get("aws.REGION", "us-east-1"))
        },
        'millionverifier': {
            'api_key': os.getenv("MILLIONVERIFIER_API_KEY", st.secrets.get("millionverifier.API_KEY", ""))
        },
        'firebase': {
            'type': os.getenv("FIREBASE_TYPE", st.secrets.get("firebase.type", "")),
            'project_id': os.getenv("FIREBASE_PROJECT_ID", st.secrets.get("firebase.project_id", "")),
            'private_key_id': os.getenv("FIREBASE_PRIVATE_KEY_ID", st.secrets.get("firebase.private_key_id", "")),
            'private_key': os.getenv("FIREBASE_PRIVATE_KEY", st.secrets.get("firebase.private_key", "").replace('\\n', '\n')),
            'client_email': os.getenv("FIREBASE_CLIENT_EMAIL", st.secrets.get("firebase.client_email", "")),
            'client_id': os.getenv("FIREBASE_CLIENT_ID", st.secrets.get("firebase.client_id", "")),
            'auth_uri': os.getenv("FIREBASE_AUTH_URI", st.secrets.get("firebase.auth_uri", "")),
            'token_uri': os.getenv("FIREBASE_TOKEN_URI", st.secrets.get("firebase.token_uri", "")),
            'auth_provider_x509_cert_url': os.getenv("FIREBASE_AUTH_PROVIDER_CERT_URL", st.secrets.get("firebase.auth_provider_x509_cert_url", "")),
            'client_x509_cert_url': os.getenv("FIREBASE_CLIENT_CERT_URL", st.secrets.get("firebase.client_x509_cert_url", ""))
        },
        'smtp2go': {
            'api_key': os.getenv("SMTP2GO_API_KEY", st.secrets.get("smtp2go.API_KEY", "")),
            'sender': os.getenv("SMTP2GO_SENDER_EMAIL", st.secrets.get("smtp2go.SENDER_EMAIL", ""))
        }
    }
    return config

config = load_config()

# Initialize Firebase Storage
def initialize_firebase():
    try:
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
            st.error("SMTP2GO API key not configured")
            return False
    except Exception as e:
        st.error(f"SMTP2GO initialization failed: {str(e)}")
        return False

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

# Default email templates with improved formatting
def get_journal_template(journal_name):
    templates = {
        "default": """<div style="font-family: Arial, sans-serif; line-height: 1.6; max-width: 600px; margin: 0 auto;">
    <div style="margin-bottom: 20px;">
        <p>To</p>
        <p>$$Author_Name$$<br>
        $$Department$$<br>
        $$University$$<br>
        $$Country$$<br>
        $$Author_Email$$</p>
    </div>
    
    <p>Dear $$Author_Name$$,</p>
    
    <p>We are pleased to invite you to submit your research work to <strong>$$Journal_Name$$</strong>.</p>
    
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
    $$Journal_Name$$</p>
    
    <div style="margin-top: 30px; font-size: 0.8em; color: #666;">
        <p>If you no longer wish to receive these emails, please <a href="$$Unsubscribe_Link$$">unsubscribe here</a>.</p>
    </div>
</div>"""
    }
    return templates.get(journal_name, templates['default'])

# Firebase Storage Functions
def upload_to_firebase(file, file_name, folder="email_lists"):
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    try:
        bucket = st.session_state.firebase_storage.bucket()
        blob = bucket.blob(f"{folder}/{file_name}")
        blob.upload_from_string(file.getvalue(), content_type='text/csv')
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
def send_email_via_smtp2go(recipient, subject, body_html, body_text, unsubscribe_link):
    try:
        api_url = "https://api.smtp2go.com/v3/email/send"
        
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
                }
            ]
        }
        
        response = requests.post(api_url, json=data)
        result = response.json()
        
        if result.get('data', {}).get('succeeded', 0) == 1:
            return True
        else:
            st.error(f"SMTP2GO Error: {result.get('error', 'Unknown error')}")
            return False
    except Exception as e:
        st.error(f"Failed to send email via SMTP2GO: {str(e)}")
        return False

def send_ses_email(ses_client, sender, recipient, subject, body_html, body_text, unsubscribe_link):
    try:
        response = ses_client.send_email(
            Source=sender,
            Destination={
                'ToAddresses': [recipient],
            },
            Message={
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
            },
            Tags=[{
                'Name': 'unsubscribe',
                'Value': unsubscribe_link
            }]
        )
        return response
    except Exception as e:
        st.error(f"Failed to send email: {str(e)}")
        return None

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
    url = f"https://api.millionverifier.com/api/v3/?api={api_key}&cmd=remaining"
    try:
        response = requests.get(url)
        data = response.json()
        return data.get('remaining', 0)
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
            return pd.DataFrame(columns=['name', 'department', 'university', 'country', 'email', 'verification_result', 'verification_details'])
        
        results = []
        for email in df['email']:
            result = verify_email(email, api_key)
            results.append(result)
            time.sleep(0.1)  # Rate limiting
        
        df['verification_result'] = [r.get('result', 'error') for r in results]
        df['verification_details'] = [str(r) for r in results]
        
        return df
    except Exception as e:
        st.error(f"Failed to process email list: {str(e)}")
        return None

# Analytics Functions
def show_email_analytics():
    st.subheader("Email Campaign Analytics Dashboard")
    
    if st.session_state.email_service == "SMTP2GO":
        # SMTP2GO Analytics (would need to implement API calls to their analytics endpoint)
        st.info("SMTP2GO Analytics would be displayed here. This requires additional API integration.")
        
        # Placeholder data for demonstration
        analytics_data = {
            'Date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
            'Sent': [120, 150, 180, 200, 220],
            'Delivered': [118, 147, 175, 195, 215],
            'Opened': [85, 110, 130, 150, 170],
            'Clicked': [45, 60, 75, 85, 95],
            'Bounced': [2, 3, 5, 5, 5]
        }
        
        df = pd.DataFrame(analytics_data)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Sent", df['Sent'].sum())
        with col2:
            st.metric("Delivery Rate", f"{round(df['Delivered'].sum()/df['Sent'].sum()*100, 2)}%")
        with col3:
            st.metric("Open Rate", f"{round(df['Opened'].sum()/df['Delivered'].sum()*100, 2)}%")
        with col4:
            st.metric("Click Rate", f"{round(df['Clicked'].sum()/df['Opened'].sum()*100, 2)}%")
        
        st.line_chart(df[['Sent', 'Delivered', 'Opened', 'Clicked', 'Bounced']])
        
        st.subheader("Performance Metrics")
        st.bar_chart(df[['Delivery Rate', 'Open Rate', 'Click Rate']].mean())
        
    else:
        # SES Analytics
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
        selected_journal = st.selectbox("Select Journal", JOURNALS)
    with col2:
        new_journal = st.text_input("Add New Journal")
        if new_journal and st.button("Add Journal"):
            if new_journal not in JOURNALS:
                JOURNALS.append(new_journal)
                st.session_state.selected_journal = new_journal
                st.experimental_rerun()
    
    st.session_state.selected_journal = selected_journal
    
    # Email Service Selection
    st.session_state.email_service = st.radio(
        "Select Email Service",
        ["SMTP2GO", "Amazon SES"],
        index=0 if st.session_state.email_service == "SMTP2GO" else 1
    )
    
    # Email Template Editor
    st.subheader("Email Template Editor")
    template = get_journal_template(st.session_state.selected_journal)
    
    col1, col2 = st.columns(2)
    with col1:
        email_subject = st.text_input("Email Subject", 
                                   f"Call for Papers - {st.session_state.selected_journal}")
    
    # WYSIWYG Editor (simplified)
    editor_col, preview_col = st.columns(2)
    
    with editor_col:
        email_body = st.text_area("Email Body (HTML)", template, height=400)
        
        st.info("""Available template variables:
        - $$Author_Name$$: Author's full name
        - $$Department$$: Author's department
        - $$University$$: Author's university
        - $$Country$$: Author's country
        - $$Author_Email$$: Author's email
        - $$Journal_Name$$: Selected journal name
        - $$Unsubscribe_Link$$: Unsubscribe link""")
    
    with preview_col:
        st.markdown("**Preview**")
        preview_html = email_body.replace("$$Author_Name$$", "Professor John Doe")
        preview_html = preview_html.replace("$$Department$$", "Computer Science")
        preview_html = preview_html.replace("$$University$$", "Harvard University")
        preview_html = preview_html.replace("$$Country$$", "United States")
        preview_html = preview_html.replace("$$Author_Email$$", "john.doe@harvard.edu")
        preview_html = preview_html.replace("$$Journal_Name$$", st.session_state.selected_journal)
        preview_html = preview_html.replace("$$Unsubscribe_Link$$", "https://example.com/unsubscribe?email=john.doe@harvard.edu")
        
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
            
            st.dataframe(df.head())
            
            if st.button("Save to Firebase"):
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
                                           "https://yourdomain.com/unsubscribe?email=")
        
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
            
            for i, row in df.iterrows():
                email_content = email_body
                email_content = email_content.replace("$$Author_Name$$", str(row.get('name', '')))
                email_content = email_content.replace("$$Department$$", str(row.get('department', '')))
                email_content = email_content.replace("$$University$$", str(row.get('university', '')))
                email_content = email_content.replace("$$Country$$", str(row.get('country', '')))
                email_content = email_content.replace("$$Author_Email$$", str(row.get('email', '')))
                email_content = email_content.replace("$$Journal_Name$$", st.session_state.selected_journal)
                
                unsubscribe_link = f"{unsubscribe_base_url}{row.get('email', '')}"
                email_content = email_content.replace("$$Unsubscribe_Link$$", unsubscribe_link)
                
                if st.session_state.email_service == "SMTP2GO":
                    success = send_email_via_smtp2go(
                        row.get('email', ''),
                        email_subject,
                        email_content,
                        email_content.replace("<br>", "\n").replace("</p>", "\n").replace("<p>", ""),
                        unsubscribe_link
                    )
                else:
                    response = send_ses_email(
                        st.session_state.ses_client,
                        sender_email,
                        row.get('email', ''),
                        email_subject,
                        email_content,
                        email_content.replace("<br>", "\n").replace("</p>", "\n").replace("<p>", ""),
                        unsubscribe_link
                    )
                    success = response is not None
                
                if success:
                    success_count += 1
                
                progress = (i + 1) / total_emails
                progress_bar.progress(progress)
                status_text.text(f"Processing {i+1} of {total_emails}: {row.get('email', '')}")
            
            st.success(f"Campaign completed! {success_count} of {total_emails} emails sent successfully.")
            show_email_analytics()

# Email Verification Section
def email_verification_section():
    st.header("Email Verification")
    
    # Check verification quota
    if config['millionverifier']['api_key']:
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
                    if result_df is not None:
                        st.session_state.verified_emails = result_df
                        st.dataframe(result_df)
                        
                        # Filter out invalid emails
                        valid_emails = result_df[result_df['verification_result'] == 'valid']
                        
                        # Convert back to original format
                        output_content = ""
                        for _, row in valid_emails.iterrows():
                            output_content += f"{row['name']}\n{row['department']}\n{row['university']}\n{row['country']}\n{row['email']}\n\n"
                        
                        verified_filename = f"verified_{uploaded_file.name}"
                        st.download_button(
                            "Download Verified List",
                            output_content,
                            verified_filename,
                            "text/plain"
                        )
                        
                        if st.button("Save Verified List to Firebase"):
                            if upload_to_firebase(StringIO(output_content), verified_filename):
                                st.success("Verified file uploaded to Firebase!")
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
                    if result_df is not None:
                        st.session_state.verified_emails = result_df
                        st.dataframe(result_df)
                        
                        # Filter out invalid emails
                        valid_emails = result_df[result_df['verification_result'] == 'valid']
                        
                        # Convert back to original format
                        output_content = ""
                        for _, row in valid_emails.iterrows():
                            output_content += f"{row['name']}\n{row['department']}\n{row['university']}\n{row['country']}\n{row['email']}\n\n"
                        
                        verified_filename = f"verified_{selected_file}"
                        st.download_button(
                            "Download Verified List",
                            output_content,
                            verified_filename,
                            "text/plain"
                        )
                        
                        if st.button("Save Verified List to Firebase"):
                            if upload_to_firebase(StringIO(output_content), verified_filename):
                                st.success("Verified file uploaded to Firebase!")
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
            valid = len(df[df['verification_result'] == 'valid'])
            st.metric("Valid Emails", valid)
        with col3:
            invalid = len(df[df['verification_result'] == 'invalid'])
            st.metric("Invalid Emails", invalid)
        with col4:
            st.metric("Quality Score", f"{round(valid/len(df)*100 if len(df) > 0 else 0, 1)}%")
        
        result_counts = df['verification_result'].value_counts()
        st.bar_chart(result_counts)

# Main App
def main():
    # Authentication
    authenticator = setup_authentication()
    
    if not st.session_state.authenticated:
        st.title("Academic Email Marketing Suite - Login")
        
        col1, col2, col3 = st.columns(3)
        with col2:
            login_form = st.empty()
            
            with login_form.container():
                name, authentication_status, username = authenticator.login('Login', 'main')
                
                if authentication_status:
                    st.session_state.authenticated = True
                    st.session_state.current_user = username
                    login_form.empty()
                    st.experimental_rerun()
                elif authentication_status is False:
                    st.error('Username/password is incorrect')
                elif authentication_status is None:
                    st.warning('Please enter your username and password')
        
            if st.button("Sign up"):
                try:
                    if authenticator.register_user('Register user', preauthorization=False):
                        st.success('User registered successfully')
                        # Save the updated config
                        with open('auth_config.yaml', 'w') as file:
                            yaml.dump(authenticator.config, file, default_flow_style=False)
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Add Google login option (would need proper OAuth implementation)
        st.markdown("---")
        st.markdown("### Or sign in with:")
        col1, col2, col3 = st.columns(3)
        with col2:
            if st.button("Google"):
                st.info("Google login would be implemented here with proper OAuth")
        
        return
    
    # Main authenticated app
    st.title(f"Academic Email Marketing Suite - Welcome {st.session_state.current_user}")
    
    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.authenticated = False
        st.session_state.current_user = None
        st.experimental_rerun()
    
    # Navigation
    app_mode = st.sidebar.selectbox("Select Mode", ["Email Campaign", "Verify Emails"])
    
    # Initialize services
    if not st.session_state.ses_client and st.session_state.email_service == "Amazon SES":
        initialize_ses()
    
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    
    if not st.session_state.smtp2go_initialized and st.session_state.email_service == "SMTP2GO":
        initialize_smtp2go()
    
    if app_mode == "Email Campaign":
        email_campaign_section()
    else:
        email_verification_section()

if __name__ == "__main__":
    main()
