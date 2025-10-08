# ---------------------------------
# Webhook server setup
# ---------------------------------
from flask import Flask, request, jsonify
from flask_cors import CORS
from threading import Thread
from waitress import serve

# Initialize webhook Flask app
webhook_app = Flask(__name__)

# Allow access from your main website domain
CORS(webhook_app, resources={r"/*": {"origins": ["https://pphmjopenaccess.com", "https://www.pphmjopenaccess.com"]}})

# Start webhook server in background
def start_webhook():
    print("🚀 Starting webhook server on port 8000")
    serve(webhook_app, host="0.0.0.0", port=8000)


# ---------------------------------


import streamlit as st
import streamlit.components.v1 as components
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
import hmac
import logging
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
from flask import abort, redirect

import threading
import copy
from urllib.parse import urlencode, urlsplit


logger = logging.getLogger("academic_email_suite")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

UNSUBSCRIBED_CACHE = {"records": [], "emails": set(), "loaded": False}
UNSUBSCRIBED_CACHE_LOCK = threading.Lock()

MAILGUN_SIGNATURE_TOLERANCE_SECONDS = 300

EMAIL_VALIDATION_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

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
    /* Resume icon button */
    .resume-btn button {
        background: url('https://github.com/prakashsharma19/academic-email-suite/blob/main/resumeicon.png?raw=true') no-repeat center center !important;
        background-size: contain !important;
        width: 40px !important;
        height: 40px !important;
        padding: 0 !important;
        color: transparent !important;
    }
    .resume-btn button:hover {
        background-color: transparent !important;
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
        st.session_state.email_service = "MAILGUN"
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
    if 'unsubscribed_users' not in st.session_state:
        st.session_state.unsubscribed_users = []
    if 'unsubscribed_email_lookup' not in st.session_state:
        st.session_state.unsubscribed_email_lookup = set()
    if 'unsubscribed_users_loaded' not in st.session_state:
        st.session_state.unsubscribed_users_loaded = False
    if 'current_verification_list' not in st.session_state:
        st.session_state.current_verification_list = None
    if 'verification_stats' not in st.session_state:
        st.session_state.verification_stats = {}
    if 'verification_start_time' not in st.session_state:
        st.session_state.verification_start_time = None
    if 'verification_progress' not in st.session_state:
        st.session_state.verification_progress = 0
    if 'auto_download_good' not in st.session_state:
        st.session_state.auto_download_good = False
    if 'auto_download_low_risk' not in st.session_state:
        st.session_state.auto_download_low_risk = False
    if 'auto_download_high_risk' not in st.session_state:
        st.session_state.auto_download_high_risk = False
    if 'good_download_content' not in st.session_state:
        st.session_state.good_download_content = None
    if 'good_download_file_name' not in st.session_state:
        st.session_state.good_download_file_name = None
    if 'low_risk_download_content' not in st.session_state:
        st.session_state.low_risk_download_content = None
    if 'low_risk_download_file_name' not in st.session_state:
        st.session_state.low_risk_download_file_name = None
    if 'high_risk_download_content' not in st.session_state:
        st.session_state.high_risk_download_content = None
    if 'high_risk_download_file_name' not in st.session_state:
        st.session_state.high_risk_download_file_name = None
    if 'current_recipient_file' not in st.session_state:
        st.session_state.current_recipient_file = None
    if 'current_verification_file' not in st.session_state:
        st.session_state.current_verification_file = None
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
        st.session_state.sender_email = (
            config.get('mailgun', {}).get('sender')
            or config['smtp2go']['sender']
        )
    if 'sender_name' not in st.session_state:
        st.session_state.sender_name = config['sender_name']
    if 'sender_base_name' not in st.session_state:
        st.session_state.sender_base_name = config['sender_name']
    if 'sender_name_loaded' not in st.session_state:
        st.session_state.sender_name_loaded = False
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
    if 'verification_resume_data' not in st.session_state:
        st.session_state.verification_resume_data = None
    if 'verification_resume_log_id' not in st.session_state:
        st.session_state.verification_resume_log_id = None


def invalidate_unsubscribed_cache():
    global UNSUBSCRIBED_CACHE
    with UNSUBSCRIBED_CACHE_LOCK:
        UNSUBSCRIBED_CACHE = {"records": [], "emails": set(), "loaded": False}
    try:
        st.session_state.unsubscribed_users_loaded = False
    except RuntimeError:
        # Accessing session state outside the Streamlit context will raise a RuntimeError.
        pass


def verify_mailgun_signature(signing_key, timestamp, token, signature):
    if not signing_key:
        return False, "Mailgun signing key not configured"
    if not all([timestamp, token, signature]):
        return False, "Missing signature parameters"
    try:
        timestamp_int = int(float(timestamp))
    except (TypeError, ValueError):
        return False, "Invalid timestamp"

    current_ts = int(time.time())
    if abs(current_ts - timestamp_int) > MAILGUN_SIGNATURE_TOLERANCE_SECONDS:
        return False, "Expired signature timestamp"

    expected = hmac.new(
        signing_key.encode("utf-8"),
        msg=f"{timestamp_int}{token}".encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, signature):
        return False, "Signature mismatch"

    return True, None


def set_email_subscription_status(email, unsubscribed, event_payload=None):
    email_normalized = (email or "").strip().lower()
    if not email_normalized:
        logger.warning(
            "Attempted to update subscription status with empty email (unsubscribed=%s)",
            unsubscribed,
        )
        return False

    db = get_firestore_db()
    if not db:
        logger.error(
            "Firestore client unavailable while updating subscription for %s", email_normalized
        )
        return False

    now = datetime.utcnow()
    record = {
        "email": email_normalized,
        "unsubscribed": unsubscribed,
        "updated_at": now,
    }

    if unsubscribed:
        unsubscribed_at = now
        event_type = None
        mailing_list = None
        reason = None
        tags = None
        if isinstance(event_payload, dict):
            event_type = event_payload.get("event")
            tags = event_payload.get("tags")
            event_ts = event_payload.get("timestamp")
            if event_ts:
                try:
                    unsubscribed_at = datetime.utcfromtimestamp(float(event_ts))
                except Exception:
                    logger.debug("Unable to parse event timestamp for %s", email_normalized)
            mailing_list_data = event_payload.get("mailing-list")
            if isinstance(mailing_list_data, dict):
                mailing_list = mailing_list_data.get("address")
            elif mailing_list_data:
                mailing_list = mailing_list_data
            reason = (
                event_payload.get("reason")
                or event_payload.get("description")
                or (event_payload.get("body", {}) or {}).get("description")
            )

        record.update(
            {
                "unsubscribed_at": unsubscribed_at,
                "event": event_type,
                "mailing_list": mailing_list,
                "reason": reason,
                "tags": tags,
            }
        )

        if isinstance(event_payload, dict):
            record["raw_event"] = event_payload
    else:
        record["resubscribed_at"] = now

    try:
        doc_ref = db.collection("unsubscribed_users").document(email_normalized)
        doc_ref.set({k: v for k, v in record.items() if v is not None}, merge=True)
        invalidate_unsubscribed_cache()
        if unsubscribed:
            logger.info("Marked %s as unsubscribed", email_normalized)
        else:
            logger.info("Marked %s as resubscribed", email_normalized)
        return True
    except Exception as exc:
        logger.exception(
            "Failed to update subscription status for %s (unsubscribed=%s): %s",
            email_normalized,
            unsubscribed,
            exc,
        )
        return False


def set_email_unsubscribed(email, event_payload=None):
    return set_email_subscription_status(email, True, event_payload)


def set_email_resubscribed(email):
    return set_email_subscription_status(email, False)


def _build_unsubscribe_page_url(email="", extra_params=None):
    params = {}
    if extra_params:
        params.update({key: value for key, value in extra_params.items() if value})

    email_clean = (email or "").strip()
    if email_clean:
        params.setdefault("email", email_clean)

    if params:
        separator = "&" if "?" in UNSUBSCRIBE_PAGE_URL and not UNSUBSCRIBE_PAGE_URL.endswith("?") else "?"
        if UNSUBSCRIBE_PAGE_URL.endswith("?") or UNSUBSCRIBE_PAGE_URL.endswith("&"):
            separator = ""
        return f"{UNSUBSCRIBE_PAGE_URL}{separator}{urlencode(params)}"
    return UNSUBSCRIBE_PAGE_URL


def _normalize_unsubscribe_action(raw_action):
    action = (raw_action or "").strip().lower()
    if action in {"unsubscribe", "unsub", "optout", "opt-out"}:
        return "unsubscribe"
    if action in {"resubscribe", "subscribe", "resub", "optin", "opt-in"}:
        return "resubscribe"
    return ""


def _corsify_unsubscribe_response(response):
    """Apply CORS headers for unsubscribe API responses."""

    origin = request.headers.get("Origin") or ""
    allowed_origin = UNSUBSCRIBE_PAGE_ORIGIN

    if origin:
        # Allow any explicitly whitelisted origins, but gracefully fall back to
        # the configured unsubscribe page origin so that the response always
        # contains a valid header even when the origin does not match the list
        # exactly (for example, due to a trailing slash or differing scheme).
        if origin in ALLOWED_UNSUBSCRIBE_ORIGINS:
            allowed_origin = origin
        elif "*" in ALLOWED_UNSUBSCRIBE_ORIGINS:
            allowed_origin = origin

    response.headers["Access-Control-Allow-Origin"] = allowed_origin
    response.headers["Vary"] = "Origin"

    requested_headers = request.headers.get("Access-Control-Request-Headers")
    if requested_headers:
        response.headers["Access-Control-Allow-Headers"] = requested_headers
    else:
        response.headers["Access-Control-Allow-Headers"] = (
            "Content-Type, Authorization, X-Requested-With"
        )

    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Max-Age"] = "86400"
    return response


@webhook_app.after_request
def _add_unsubscribe_cors_headers(response):
    """Ensure CORS headers are added for unsubscribe endpoints automatically."""

    try:
        request_path = request.path or ""
    except RuntimeError:
        # Request context might not be available during app start-up.
        return response

    if request_path.startswith("/api/unsubscribe") or request_path.startswith("/unsubscribe"):
        return _corsify_unsubscribe_response(response)
    return response


def load_unsubscribed_users(force_refresh=False):
    if force_refresh:
        invalidate_unsubscribed_cache()

    with UNSUBSCRIBED_CACHE_LOCK:
        cache_loaded = UNSUBSCRIBED_CACHE["loaded"]
        cached_records = copy.deepcopy(UNSUBSCRIBED_CACHE["records"]) if cache_loaded else None
        cached_emails = set(UNSUBSCRIBED_CACHE["emails"]) if cache_loaded else set()

    if cache_loaded:
        st.session_state.unsubscribed_users = cached_records or []
        st.session_state.unsubscribed_email_lookup = cached_emails
        st.session_state.unsubscribed_users_loaded = True
        return cached_records or []

    db = get_firestore_db()
    if not db:
        return []

    try:
        unsubscribed_ref = db.collection("unsubscribed_users")
        records = []
        email_lookup = set()
        for doc in unsubscribed_ref.stream():
            data = doc.to_dict() or {}
            email_value = (data.get("email") or doc.id or "").strip().lower()
            if not email_value:
                continue
            unsubscribed_flag = data.get("unsubscribed", True)
            unsubscribed_at = data.get("unsubscribed_at") or data.get("updated_at")
            if isinstance(unsubscribed_at, datetime) and unsubscribed_at.tzinfo is not None:
                unsubscribed_at = unsubscribed_at.astimezone(pytz.UTC).replace(tzinfo=None)

            record = {
                "email": email_value,
                "unsubscribed": unsubscribed_flag,
                "unsubscribed_at": unsubscribed_at,
                "event": data.get("event"),
                "mailing_list": data.get("mailing_list"),
                "reason": data.get("reason"),
                "tags": data.get("tags"),
            }
            records.append(record)
            if unsubscribed_flag:
                email_lookup.add(email_value)

        records.sort(
            key=lambda item: item.get("unsubscribed_at") if isinstance(item.get("unsubscribed_at"), datetime) else datetime.min,
            reverse=True,
        )

        with UNSUBSCRIBED_CACHE_LOCK:
            UNSUBSCRIBED_CACHE["records"] = copy.deepcopy(records)
            UNSUBSCRIBED_CACHE["emails"] = set(email_lookup)
            UNSUBSCRIBED_CACHE["loaded"] = True

        st.session_state.unsubscribed_users = records
        st.session_state.unsubscribed_email_lookup = email_lookup
        st.session_state.unsubscribed_users_loaded = True
        return records
    except Exception as exc:
        logger.exception("Failed to load unsubscribed users: %s", exc)
        return []


def is_email_unsubscribed(email):
    if not email:
        return False
    lookup = st.session_state.get("unsubscribed_email_lookup", set())
    return email.lower() in lookup


@webhook_app.route("/unsubscribe", methods=["GET", "POST"])
def unsubscribe_webhook():
    if request.method == "GET":
        email = (request.args.get("email") or "").strip()
        redirect_url = _build_unsubscribe_page_url(email)
        response = redirect(redirect_url)
        response.headers["Cache-Control"] = "no-store"
        return response

    if request.is_json:
        return unsubscribe_api()

    form_action = _normalize_unsubscribe_action(request.form.get("action"))
    if form_action:
        email = (request.form.get("email") or "").strip()
        if not email:
            response = jsonify({
                "success": False,
                "message": "No email address was provided.",
            })
            response.status_code = 400
            return response

        if not EMAIL_VALIDATION_REGEX.match(email):
            response = jsonify({
                "success": False,
                "message": "The email address provided is invalid.",
            })
            response.status_code = 400
            return response

        if form_action == "unsubscribe":
            success = set_email_unsubscribed(email)
            if success:
                message = "You have been unsubscribed successfully and will be excluded from future campaigns."
            else:
                message = "We were unable to process your unsubscribe request at this time. Please try again later."
            unsubscribed = success
        else:
            success = set_email_resubscribed(email)
            if success:
                message = "You have been re-subscribed successfully. We will include you in future campaigns unless you opt out again."
            else:
                message = "We were unable to process your re-subscribe request at this time. Please try again later."
            unsubscribed = not success and is_email_unsubscribed(email)

        status_code = 200 if success else 500
        response = jsonify({
            "success": success,
            "message": message,
            "unsubscribed": bool(unsubscribed),
        })
        response.status_code = status_code
        return response

    signing_key = config.get("mailgun", {}).get("signing_key") or config.get("mailgun", {}).get("api_key")
    if not signing_key:
        abort(500, description="Mailgun signing key not configured")

    timestamp = request.form.get("timestamp")
    token = request.form.get("token")
    signature = request.form.get("signature")

    valid, error_message = verify_mailgun_signature(signing_key, timestamp, token, signature)
    if not valid:
        logger.warning("Rejected unsubscribe webhook due to signature error: %s", error_message)
        abort(403, description=error_message or "Invalid signature")

    event_json = {}
    event_data_raw = request.form.get("event-data")
    if event_data_raw:
        try:
            event_json = json.loads(event_data_raw)
        except json.JSONDecodeError:
            logger.warning("Invalid event-data JSON received from Mailgun")
            abort(400, description="Invalid event-data payload")
    else:
        body_json = request.get_json(silent=True) or {}
        event_json = body_json.get("event-data", body_json)

    event_type = (event_json.get("event") or "").lower()
    if event_type not in {"unsubscribed", "unsubscribe", "complained"}:
        logger.info("Ignoring webhook event of type '%s'", event_type)
        return jsonify({"status": "ignored", "reason": f"Unsupported event '{event_type}'"})

    email = (
        event_json.get("recipient")
        or event_json.get("address")
        or request.form.get("recipient")
        or request.form.get("address")
    )

    if not email:
        abort(400, description="Recipient email missing")

    if not set_email_unsubscribed(email, event_json):
        abort(500, description="Failed to persist unsubscribe event")

    return jsonify({"status": "success"})



def _coerce_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@webhook_app.route("/unsubscribe/page", methods=["GET"])
def unsubscribe_page():
    email = (request.args.get("email") or "").strip()
    redirect_url = _build_unsubscribe_page_url(email)
    response = redirect(redirect_url)
    response.headers["Cache-Control"] = "no-store"
    return response

@webhook_app.route("/api/unsubscribe", methods=["GET", "POST", "OPTIONS"])
def unsubscribe_api():
    if request.method == "GET":
        response = jsonify(
            {
                "success": True,
                "message": "Webhook is live",
            }
        )
        return _corsify_unsubscribe_response(response)

    if request.method == "OPTIONS":
        response = webhook_app.make_default_options_response()
        return _corsify_unsubscribe_response(response)

    if request.method != "POST":
        abort(405, description=f"Method Not Allowed ({request.method})")

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        response = jsonify(
            {
                "success": False,
                "message": "Request body must be a valid JSON object.",
            }
        )
        response.status_code = 400
        return _corsify_unsubscribe_response(response)

    email = (payload.get("email") or "").strip()
    if not email:
        response = jsonify(
            {
                "success": False,
                "message": "No email address was provided.",
            }
        )
        response.status_code = 400
        return _corsify_unsubscribe_response(response)

    if not EMAIL_VALIDATION_REGEX.match(email):
        response = jsonify(
            {
                "success": False,
                "message": "The email address provided is invalid.",
            }
        )
        response.status_code = 400
        return _corsify_unsubscribe_response(response)

    action = _normalize_unsubscribe_action(payload.get("action")) or "unsubscribe"

    if action not in {"unsubscribe", "resubscribe"}:
        response = jsonify(
            {
                "success": False,
                "message": "Unsupported action specified.",
            }
        )
        response.status_code = 400
        return _corsify_unsubscribe_response(response)

    if action == "unsubscribe":
        success = set_email_unsubscribed(email, payload)
        if success:
            message = (
                "You have been unsubscribed successfully and will be excluded from future campaigns."
            )
            unsubscribed = True
        else:
            message = (
                "We were unable to process your unsubscribe request at this time. Please try again later."
            )
            unsubscribed = is_email_unsubscribed(email)
    else:
        success = set_email_resubscribed(email)
        if success:
            message = (
                "You have been re-subscribed successfully. We will include you in future campaigns unless you opt out again."
            )
            unsubscribed = False
        else:
            message = (
                "We were unable to process your re-subscribe request at this time. Please try again later."
            )
            unsubscribed = is_email_unsubscribed(email)

    status_code = 200 if success else 500
    response = jsonify(
        {
            "success": bool(success),
            "message": message,
            "unsubscribed": bool(unsubscribed),
        }
    )
    response.status_code = status_code
    return _corsify_unsubscribe_response(response)

_WEBHOOK_THREAD = None
_WEBHOOK_THREAD_LOCK = threading.Lock()


def ensure_webhook_server():
    """Start the webhook server thread exactly once."""

    global _WEBHOOK_THREAD

    if _WEBHOOK_THREAD and _WEBHOOK_THREAD.is_alive():
        return

    with _WEBHOOK_THREAD_LOCK:
        if _WEBHOOK_THREAD and _WEBHOOK_THREAD.is_alive():
            return

        thread = Thread(target=start_webhook, daemon=True)
        thread.start()
        _WEBHOOK_THREAD = thread


def update_sender_name():
    """Update sender name with the currently selected journal."""
    journal = st.session_state.get("selected_journal")
    base = st.session_state.get("sender_base_name", config['sender_name'])
    if journal:
        st.session_state.sender_name = f"{journal} - {base}"
    else:
        st.session_state.sender_name = base
    st.session_state.show_journal_details = False


def update_editor_sender_name():
    """Update sender name for editor invitation based on selected journal."""
    journal = st.session_state.get("selected_editor_journal")
    base = st.session_state.get("sender_base_name", config['sender_name'])
    if journal:
        st.session_state.sender_name = f"{journal} - {base}"
    else:
        st.session_state.sender_name = base
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


def parse_email_entries(file_content: str) -> pd.DataFrame:
    """Return DataFrame of email entries allowing variable address lengths."""
    entries = []
    current_lines = []
    for line in file_content.split('\n'):
        line = line.strip()
        if not line:
            continue
        if '@' in line and '.' in line and ' ' not in line:
            name = current_lines[0] if current_lines else ''
            addr_parts = current_lines[1:] if len(current_lines) > 1 else []
            department = addr_parts[0] if len(addr_parts) >= 1 else ''
            university = addr_parts[1] if len(addr_parts) >= 2 else ''
            country = addr_parts[-1] if len(addr_parts) >= 3 else ''
            address_lines = '\n'.join(addr_parts)
            entries.append(
                {
                    'name': name,
                    'department': department,
                    'university': university,
                    'country': country,
                    'address_lines': address_lines,
                    'email': line,
                }
            )
            current_lines = []
        else:
            current_lines.append(line)

    return pd.DataFrame(
        entries,
        columns=['name', 'department', 'university', 'country', 'address_lines', 'email'],
    )


# Journal Data
JOURNALS = [
    "Advances and Applications in Fluid Mechanics",
    "Advances in Fuzzy Sets and Systems",
    "Far East Journal of Electronics and Communications",
    "Far East Journal of Mathematical Education",
    "International Journal of Nutrition and Dietetics",
    "International Journal of Numerical Methods and Applications",
    "Advances and Applications in Discrete Mathematics",
    "Advances and Applications in Statistics",
    "Far East Journal of Applied Mathematics",
    "Far East Journal of Dynamical Systems",
    "Far East Journal of Mathematical Sciences (FJMS)",
    "FJMS - Far East Journal of Mathematical Sciences (FJMS)",
    "Far East Journal of Theoretical Statistics",
    "JP Journal of Algebra, Number Theory and Applications",
    "JP Journal of Geometry and Topology",
    "JP Journal of Biostatistics",
    "JP Journal of Heat and Mass Transfer",
    "Universal Journal of Mathematics and Mathematical Sciences",
    "Far East Journal of Mechanical Engineering and Physics",
    "Advances in Materials Science and their Emerging Technologies",
    "Far East Journal of Endocrinology, Diabetes and Obesity",
    "Advances in Food & Dairy Sciences and Their Emerging Technologies (FDSET)",
    "Advances in Computer Science and Engineering",
    "Far East Journal of Experimental and Theoretical Artificial Intelligence",
    "Journal of Water Waves",
    "JP Journal of Solids and Structures",
    "Current Development in Oceanography",
    "OA - Advances and Applications in Fluid Mechanics",
    "OA - Advances in Fuzzy Sets and Systems",
    "OA - Far East Journal of Electronics and Communications",
    "OA - Far East Journal of Mathematical Education",
    "OA - International Journal of Nutrition and Dietetics",
    "OA - International Journal of Numerical Methods and Applications",
    "OA - Advances and Applications in Discrete Mathematics",
    "OA - Advances and Applications in Statistics",
    "OA - Far East Journal of Applied Mathematics",
    "OA - Far East Journal of Dynamical Systems",
    "OA - Far East Journal of Mathematical Sciences (FJMS)",
    "OA - Far East Journal of Theoretical Statistics",
    "OA - JP Journal of Algebra, Number Theory and Applications",
    "OA - JP Journal of Geometry and Topology",
    "OA - JP Journal of Biostatistics",
    "OA - JP Journal of Fixed Point Theory and Applications",
    "OA - JP Journal of Heat and Mass Transfer",
    "OA - Current Development in Oceanography",
    "OA - JP Journal of Solids and Structures",
    "OA - Far East Journal of Electronics and Communications",
    "OA - Advances in Computer Science and Engineering",
    "OA - Far East Journal of Experimental and Theoretical Artificial Intelligence",
    "OA - Far East Journal of Mechanical Engineering and Physics",
    "OA - Universal Journal of Mathematics and Mathematical Sciences"
]

# Editor Invitation journal list
EDITOR_JOURNALS = [
    "Editors Invitation - Far East Journal of Mechanical Engineering and Physics",
    "Editors Invitation - Advances in Materials Science and their Emerging Technologies",
    "Editors Invitation - Far East Journal of Endocrinology, Diabetes and Obesity",
    "Editors Invitation - Advances in Food & Dairy Sciences and their Emerging Technologies",
    "Editors Invitation - Far East Journal of Mathematical Sciences (FJMS)",
    "Editors Invitation - JP Journal of Heat and Mass Transfer",
    "Editors Invitation - JP Journal of Geometry and Topology",
    "Editors Invitation - Far East Journal of Theoretical Statistics",
    "Editors Invitation - Advances and Applications in Statistics",
    "Editors Invitation - Advances and Applications in Discrete Mathematics",
    "Editors Invitation - Advances and Applications in Fluid Mechanics",
    "Editors Invitation - Advances in Fuzzy Sets and Systems",
    "Editors Invitation - Far East Journal of Electronics and Communications",
    "Editors Invitation - Far East Journal of Mathematical Education",
    "Editors Invitation - International Journal of Nutrition and Dietetics",
    "Editors Invitation - International Journal of Numerical Methods and Applications",
    "Editors Invitation - Far East Journal of Applied Mathematics",
    "Editors Invitation - Far East Journal of Dynamical Systems",
    "Editors Invitation - JP Journal of Algebra, Number Theory and Applications",
    "Editors Invitation - JP Journal of Geometry and Topology",
    "Editors Invitation - JP Journal of Biostatistics",
    "Editors Invitation - JP Journal of Fixed Point Theory and Applications",
    "Editors Invitation - Universal Journal of Mathematics and Mathematical Sciences",
    "Editors Invitation - Advances in Computer Science and Engineering",
    "Editors Invitation - Far East Journal of Experimental and Theoretical Artificial Intelligence",
    "Editors Invitation - Journal of Water Waves",
    "Editors Invitation - JP Journal of Solids and Structures",
    "Editors Invitation - Current Development in Oceanography",
    "OE - Editors Invitation - Advances and Applications in Fluid Mechanics",
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
        'mailgun': {
            'api_key': os.getenv("MAILGUN_API_KEY", ""),
            'domain': os.getenv("MAILGUN_DOMAIN", ""),
            'sender': os.getenv("MAILGUN_SENDER_EMAIL", ""),
            'signing_key': os.getenv("MAILGUN_SIGNING_KEY", "")
        },
        'sender_name': os.getenv("SENDER_NAME", "Pushpa Publishing House"),
        'webhook': {
            'url': os.getenv("WEBHOOK_URL", "")
        },
        'unsubscribe': {
            'page_url': os.getenv("UNSUBSCRIBE_PAGE_URL", ""),
            'app_base_url': os.getenv("STREAMLIT_APP_BASE_URL", ""),
        }
    }
    return config

config = load_config()

# Determine the unsubscribe page location. Prefer configuration, fall back to
# the Streamlit app's own unsubscribe page so the experience stays within the
# product rather than redirecting to an external site.
_configured_unsubscribe_url = config.get("unsubscribe", {}).get("page_url", "").strip()
_configured_app_base_url = config.get("unsubscribe", {}).get("app_base_url", "").strip()

if _configured_unsubscribe_url:
    UNSUBSCRIBE_PAGE_URL = _configured_unsubscribe_url
else:
    base_url = _configured_app_base_url.rstrip("/") if _configured_app_base_url else ""
    if base_url:
        UNSUBSCRIBE_PAGE_URL = f"{base_url}/unsubscribe"
    else:
        UNSUBSCRIBE_PAGE_URL = "https://pphmjopenaccess.com/unsubscribe"

_unsubscribe_origin_parts = urlsplit(UNSUBSCRIBE_PAGE_URL)
UNSUBSCRIBE_PAGE_ORIGIN = (
    f"{_unsubscribe_origin_parts.scheme}://{_unsubscribe_origin_parts.netloc}"
    if _unsubscribe_origin_parts.scheme and _unsubscribe_origin_parts.netloc
    else "https://pphmjopenaccess.com"
)

ALLOWED_UNSUBSCRIBE_ORIGINS = {
    origin.strip()
    for origin in (
        UNSUBSCRIBE_PAGE_ORIGIN,
        *[
            value.strip()
            for value in os.getenv("UNSUBSCRIBE_ALLOWED_ORIGINS", "").split(",")
            if value.strip()
        ],
    )
    if origin.strip()
}
if not ALLOWED_UNSUBSCRIBE_ORIGINS:
    ALLOWED_UNSUBSCRIBE_ORIGINS = {UNSUBSCRIBE_PAGE_ORIGIN}

if UNSUBSCRIBE_PAGE_URL.endswith("?") or UNSUBSCRIBE_PAGE_URL.endswith("&"):
    DEFAULT_UNSUBSCRIBE_BASE_URL = f"{UNSUBSCRIBE_PAGE_URL}email="
else:
    separator = "&" if "?" in UNSUBSCRIBE_PAGE_URL else "?"
    DEFAULT_UNSUBSCRIBE_BASE_URL = f"{UNSUBSCRIBE_PAGE_URL}{separator}email="
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

# Ensure Firebase and the webhook server are ready before rendering any UI
if not st.session_state.get("firebase_initialized"):
    initialize_firebase()
ensure_webhook_server()

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


def send_email_via_mailgun(recipient, subject, body_html, body_text, unsubscribe_link, reply_to=None):
    try:
        api_key = config['mailgun']['api_key']
        domain = config['mailgun']['domain']
        sender_email = config['mailgun']['sender'] or st.session_state.sender_email

        if not api_key or not domain or not sender_email:
            st.error("Mailgun configuration not complete")
            return False, None

        sender_name = st.session_state.sender_name
        formatted_sender = formataddr((sender_name, sender_email))
        api_url = f"https://api.mailgun.net/v3/{domain}/messages"

        data = {
            "from": formatted_sender,
            "to": recipient,
            "subject": subject,
            "text": body_text,
            "html": body_html,
            "h:List-Unsubscribe": f"<{unsubscribe_link}>",
            "h:List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
            "o:unsubscribe": "true",
            "o:tracking": "true",
            "o:tracking-clicks": "true",
            "o:tracking-opens": "true",
        }

        if reply_to:
            data["h:Reply-To"] = reply_to

        response = requests.post(
            api_url,
            auth=("api", api_key),
            data=data,
            timeout=10,
        )

        if not response.ok:
            try:
                error_message = response.json().get("message", response.text.strip())
            except ValueError:
                error_message = response.text.strip()
            st.error(f"Mailgun HTTP {response.status_code}: {error_message}")
            return False, None

        try:
            result = response.json()
        except ValueError:
            result = {}

        message_id = result.get("id") or response.headers.get("X-Message-Id")
        return True, message_id
    except requests.exceptions.RequestException as e:
        st.error(f"Mailgun request failed: {str(e)}")
        return False, None
    except Exception as e:
        st.error(f"Failed to send email via Mailgun: {str(e)}")
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
    """Return remaining credits from MillionVerifier.

    The MillionVerifier credit endpoint sometimes returns the credit
    information in slightly different structures (integer, string, or nested
    inside another object).  The previous implementation only handled one
    very specific format which caused the UI to display an incorrect value –
    typically ``1`` – for the remaining credit balance.

    This function now normalises the API response and always returns an
    integer credit balance.  If the API call fails or the response format is
    unexpected, ``0`` is returned and a Streamlit error is displayed.
    """

    url = f"https://api.millionverifier.com/api/v3/credits?api={api_key}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Credits may appear under different keys or as strings.
        credits = (
            data.get("credits")
            or data.get("credit")
            or data.get("credits_left")
            or data.get("credit_left")
        )

        if credits is None:
            st.error(f"API Error: {data.get('error', 'Unknown error')}")
            return 0

        try:
            # Convert to integer in case the API returns a string/float
            return int(float(credits))
        except (ValueError, TypeError):
            st.error(f"Unexpected credit format: {credits}")
            return 0

    except Exception as e:
        st.error(f"Failed to check quota: {str(e)}")
        return 0

def process_email_list(file_content, api_key, log_id=None, resume_data=None):
    try:
        df = parse_email_entries(file_content)
        
        if df.empty:
            return pd.DataFrame(
                columns=['name', 'department', 'university', 'country', 'address_lines', 'email', 'verification_result']
            )
        
        # Verify emails
        results = []
        total_emails = len(df)
        st.session_state.verification_start_time = time.time()

        progress_bar = st.progress(0)
        status_text = st.empty()

        start_index = 0
        if resume_data:
            start_index = resume_data.get('current_index', 0)
            prev_results = resume_data.get('results', [])
            for idx, res in enumerate(prev_results):
                if idx < len(df):
                    df.loc[idx, 'verification_result'] = res
                    results.append({'result': res})
            progress_bar.progress(start_index / total_emails)
            st.session_state.verification_progress = start_index / total_emails

        for i in range(start_index, total_emails):
            email = df.loc[i, 'email']
            result = verify_email(email, api_key)
            if result:
                results.append(result)
            else:
                results.append({'result': 'error'})
            
            # Update progress
            progress = (i + 1) / total_emails
            st.session_state.verification_progress = progress
            progress_bar.progress(progress)
            if log_id:
                update_operation_log(log_id, progress=progress)
                save_verification_progress(
                    log_id,
                    file_content,
                    [r.get('result', 'error') if isinstance(r, dict) else r for r in results],
                    i + 1,
                    total_emails,
                )
            
            # Calculate estimated time remaining
            elapsed_time = time.time() - st.session_state.verification_start_time
            if i > 0:
                estimated_total_time = elapsed_time / progress
                remaining_time = estimated_total_time - elapsed_time
                status_text.text(f"Processing {i+1} of {total_emails} emails. Estimated time remaining: {int(remaining_time)} seconds")
            
            time.sleep(0.1)  # Rate limiting
        
        df['verification_result'] = [r.get('result', 'error') if r else 'error' for r in results]
        
        # Calculate verification stats
        results_lower = df['verification_result'].astype(str).str.lower()
        total = len(df)
        good_statuses = ['valid', 'ok', 'good']
        risky_statuses = ['unknown', 'risky', 'accept_all']
        low_risk_statuses = ['catch_all', 'catchall', 'catch-all']

        good = int(results_lower.isin(good_statuses).sum())
        bad = int((results_lower == 'invalid').sum())
        low_risk_mask = results_lower.isin(low_risk_statuses)
        low_risk = int(low_risk_mask.sum())
        risky = int((results_lower.isin(risky_statuses) & ~low_risk_mask).sum())

        st.session_state.verification_stats = {
            'total': total,
            'good': good,
            'bad': bad,
            'risky': risky,
            'low_risk': low_risk,
            'good_percent': round((good / total) * 100, 1) if total > 0 else 0,
            'bad_percent': round((bad / total) * 100, 1) if total > 0 else 0,
            'risky_percent': round((risky / total) * 100, 1) if total > 0 else 0,
            'low_risk_percent': round((low_risk / total) * 100, 1) if total > 0 else 0,
        }
        if log_id:
            save_verification_results(log_id, df)
            update_operation_log(log_id, status="completed", progress=1.0)
            delete_verification_progress(log_id)
        return df
    except Exception as e:
        st.error(f"Failed to process email list: {str(e)}")
        if log_id:
            update_operation_log(log_id, status="failed")
        return pd.DataFrame()

def generate_report_file(df, report_type):
    """Generate different types of report files without 'nan' entries"""
    if df.empty:
        return ""

    lower_results = df['verification_result'].astype(str).str.lower()
    low_risk_statuses = ['catch_all', 'catchall', 'catch-all']

    if report_type == "good":
        valid_statuses = ['valid', 'ok', 'good']
        filtered_df = df[lower_results.isin(valid_statuses)]
    elif report_type == "bad":
        filtered_df = df[lower_results == 'invalid']
    elif report_type == "risky":
        risky_statuses = ['unknown', 'risky', 'accept_all']
        mask = lower_results.isin(risky_statuses) & ~lower_results.isin(low_risk_statuses)
        filtered_df = df[mask]
    elif report_type == "low_risk":
        filtered_df = df[lower_results.isin(low_risk_statuses)]
    else:
        filtered_df = df

    entries = []
    for _, row in filtered_df.iterrows():
        lines = []
        name = row.get('name', '')
        if pd.notna(name) and str(name).strip() != "":
            lines.append(str(name))
        address = row.get('address_lines', '')
        if pd.notna(address) and str(address).strip() != "":
            lines.extend(str(address).split('\n'))
        email = row.get('email', '')
        if pd.notna(email) and str(email).strip() != "":
            lines.append(str(email))
        if lines:
            entries.append("\n".join(lines))

    return "\n\n".join(entries).strip()

def generate_email_report_filename(original_name, count, suffix):
    """Return a cleaned filename for email reports with an updated count."""
    name_no_ext = os.path.splitext(original_name)[0]
    pattern = r"\(\d+\s*entries\)\s*-\s*.+$"
    if re.search(pattern, name_no_ext):
        name_no_ext = re.sub(pattern, f"({count} entries) - {suffix}", name_no_ext)
    else:
        name_no_ext = f"{name_no_ext} ({count} entries) - {suffix}"
    return name_no_ext + ".txt"


def generate_good_emails_filename(original_name, count):
    """Return a cleaned filename for good emails with updated count."""
    return generate_email_report_filename(original_name, count, "CQ")


def generate_low_risk_emails_filename(original_name, count):
    """Return a cleaned filename for low-risk (catch-all) emails."""
    return generate_email_report_filename(original_name, count, "LOW_RISK_EMAIL")


def generate_high_risk_emails_filename(original_name, count):
    """Return a cleaned filename for high-risk emails."""
    return generate_email_report_filename(original_name, count, "HIGH_RISK")


def prepare_verification_downloads(result_df):
    """Populate session state with download content and filenames for reports."""
    good_count = st.session_state.verification_stats.get('good', 0)
    low_risk_count = st.session_state.verification_stats.get('low_risk', 0)
    high_risk_count = st.session_state.verification_stats.get('risky', 0)

    source_name = st.session_state.get('current_verification_file')
    good_source_name = source_name or "good_emails.txt"
    low_risk_source_name = source_name or "low_risk_emails.txt"
    high_risk_source_name = source_name or "high_risk_emails.txt"

    good_content = generate_report_file(result_df, "good")
    st.session_state.good_download_content = good_content
    st.session_state.good_download_file_name = generate_good_emails_filename(
        good_source_name, good_count
    )
    st.session_state.auto_download_good = True

    low_risk_content = generate_report_file(result_df, "low_risk")
    st.session_state.low_risk_download_content = low_risk_content
    st.session_state.low_risk_download_file_name = generate_low_risk_emails_filename(
        low_risk_source_name, low_risk_count
    )
    st.session_state.auto_download_low_risk = low_risk_count > 0

    high_risk_content = generate_report_file(result_df, "risky")
    st.session_state.high_risk_download_content = high_risk_content
    st.session_state.high_risk_download_file_name = generate_high_risk_emails_filename(
        high_risk_source_name, high_risk_count
    )
    st.session_state.auto_download_high_risk = high_risk_count > 0

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

def delete_firebase_file(filename):
    try:
        db = get_firestore_db()
        if not db:
            return False

        db.collection("email_files").document(filename).delete()
        return True
    except Exception as e:
        st.error(f"Failed to delete file: {str(e)}")
        return False

# ----- Verification Result Storage Functions -----
def save_verification_results(log_id, df):
    """Persist verification results to Firestore for later retrieval."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("verification_results").document(log_id)
        doc_ref.set({
            "user": st.session_state.get("username", "admin"),
            "file_name": st.session_state.get("current_verification_file", ""),
            "csv": df.to_csv(index=False),
            "stats": st.session_state.verification_stats,
            "saved_at": datetime.now(),
        })
        return True
    except Exception as e:
        st.error(f"Failed to save verification results: {str(e)}")
        return False


def load_verification_results(log_id):
    """Load previously saved verification results from Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc_ref = db.collection("verification_results").document(log_id)
        doc = doc_ref.get()
        if not doc.exists:
            return None

        data = doc.to_dict()
        csv_data = data.get("csv", "")
        df = pd.read_csv(StringIO(csv_data)) if csv_data else pd.DataFrame()
        stats = data.get("stats", {})
        file_name = data.get("file_name", "")
        return df, stats, file_name
    except Exception as e:
        st.error(f"Failed to load verification results: {str(e)}")
        return None


# ----- Verification Progress Persistence Functions -----
def save_verification_progress(log_id, file_content, results, current_index, total_emails):
    """Save intermediate verification progress for resuming later."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("verification_progress").document(log_id)
        doc_ref.set({
            "user": st.session_state.get("username", "admin"),
            "file_content": file_content,
            "results": results,
            "current_index": current_index,
            "total_emails": total_emails,
            "last_updated": datetime.now(),
        })
        return True
    except Exception as e:
        st.error(f"Failed to save verification progress: {str(e)}")
        return False


def load_verification_progress(log_id):
    """Load saved verification progress if available."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc = db.collection("verification_progress").document(log_id).get()
        if doc.exists:
            return doc.to_dict()
        return None
    except Exception as e:
        st.error(f"Failed to load verification progress: {str(e)}")
        return None


def delete_verification_progress(log_id):
    """Remove saved verification progress when done."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        db.collection("verification_progress").document(log_id).delete()
        return True
    except Exception as e:
        st.error(f"Failed to delete verification progress: {str(e)}")
        return False

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

# Sender name persistence functions
def save_sender_name(sender_name):
    """Persist the base sender name to Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("settings").document("sender_name")
        doc_ref.set({
            "value": sender_name,
            "last_updated": datetime.now(),
            "updated_by": "admin",
        })
        return True
    except Exception as e:
        st.error(f"Failed to save sender name: {str(e)}")
        return False


def load_sender_name():
    """Load the base sender name from Firestore if available."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc_ref = db.collection("settings").document("sender_name")
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict().get("value", None)
        return None
    except Exception as e:
        st.error(f"Failed to load sender name: {str(e)}")
        return None

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

def get_campaign_state(campaign_id):
    """Retrieve a single campaign document by ID."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc = db.collection("active_campaigns").document(str(campaign_id)).get()
        if doc.exists:
            return doc.to_dict()
        return None
    except Exception as e:
        st.error(f"Failed to get campaign: {str(e)}")
        return None

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


def execute_campaign(campaign_data):
    """Send emails for the provided campaign starting at the saved index."""
    df = pd.DataFrame(campaign_data.get("recipient_list", []))
    total_emails = campaign_data.get("total_emails", len(df))
    current_index = campaign_data.get("current_index", 0)
    success_count = campaign_data.get("emails_sent", 0)
    journal = campaign_data.get("journal_name", "")
    selected_subjects = campaign_data.get("email_subjects", [])
    email_body = campaign_data.get("email_body", "")
    email_subject = selected_subjects[0] if selected_subjects else ""
    unsubscribe_base_url = DEFAULT_UNSUBSCRIBE_BASE_URL
    campaign_id = campaign_data.get("campaign_id")
    log_id = campaign_data.get("log_id")

    progress_bar = st.progress(current_index / total_emails if total_emails else 0)
    status_text = st.empty()
    cancel_button = st.button("Cancel Campaign")

    reply_to = st.session_state.journal_reply_addresses.get(journal, None)
    email_ids = []

    # Always refresh the unsubscribe cache at the start of a campaign run to
    # ensure newly unsubscribed users are respected immediately.
    load_unsubscribed_users(force_refresh=True)

    for i in range(current_index, total_emails):
        if st.session_state.campaign_cancelled:
            break

        row = df.iloc[i]
        recipient_email = row.get('email', '')
        if is_email_unsubscribed(recipient_email):
            progress = (i + 1) / total_emails
            progress_bar.progress(progress)
            status_text.text(
                f"Skipping {i+1} of {total_emails}: {recipient_email} (unsubscribed)"
            )
            update_campaign_progress(campaign_id, i + 1, success_count)
            if log_id:
                update_operation_log(log_id, progress=progress)
            continue
        if is_email_blocked(recipient_email):
            progress = (i + 1) / total_emails
            progress_bar.progress(progress)
            status_text.text(f"Skipping {i+1} of {total_emails}: {recipient_email} (blocked)")
            update_campaign_progress(campaign_id, i + 1, success_count)
            if log_id:
                update_operation_log(log_id, progress=progress)
            continue

        address_lines = row.get('address_lines', '')
        if pd.notna(address_lines) and str(address_lines).strip() != "":
            author_address = "".join(f"{ln}<br>" for ln in str(address_lines).split('\n'))
        else:
            author_address = ""

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
        email_content = email_content.replace("$$Journal_Name$$", journal)

        recipient_email_for_unsubscribe = str(row.get('email', '')).strip()
        unsubscribe_link = f"{unsubscribe_base_url}{recipient_email_for_unsubscribe}"
        unsubscribe_placeholders = ("$$Unsubscribe_Link$$", "%unsubscribe_url%")
        unsubscribe_placeholder_found = False
        for unsubscribe_placeholder in unsubscribe_placeholders:
            if unsubscribe_placeholder in email_content:
                unsubscribe_placeholder_found = True
                email_content = email_content.replace(
                    unsubscribe_placeholder,
                    unsubscribe_link,
                )

        if unsubscribe_link not in email_content:
            if not unsubscribe_placeholder_found:
                email_content += (
                    "<br><br>"
                    "<p>If you no longer wish to receive these emails, please "
                    f"<a href=\"{unsubscribe_link}\">unsubscribe here</a>." "</p>"
                )

        plain_text = (
            email_content.replace("<br>", "\n")
            .replace("</p>", "\n\n")
            .replace("<p>", "")
        )
        for unsubscribe_placeholder in unsubscribe_placeholders:
            if unsubscribe_placeholder in plain_text:
                plain_text = plain_text.replace(
                    unsubscribe_placeholder,
                    unsubscribe_link,
                )
        if unsubscribe_link not in plain_text:
            plain_text += (
                "\n\nIf you no longer wish to receive these emails, please unsubscribe here: "
                f"{unsubscribe_link}"
            )

        subject_cycle = selected_subjects if selected_subjects else [email_subject]
        subject = subject_cycle[i % len(subject_cycle)]
        subject = subject.replace("$$AuthorLastname$$", last_name)

        service = (st.session_state.email_service or "MAILGUN").upper()
        if service == "SMTP2GO":
            success, email_id = send_email_via_smtp2go(
                recipient_email,
                subject,
                email_content,
                plain_text,
                unsubscribe_link,
                reply_to,
            )
        else:
            success, email_id = send_email_via_mailgun(
                recipient_email,
                subject,
                email_content,
                plain_text,
                unsubscribe_link,
                reply_to,
            )

        if success:
            success_count += 1
            if email_id:
                email_ids.append(email_id)

        progress = (i + 1) / total_emails
        progress_bar.progress(progress)
        status_text.text(f"Processing {i+1} of {total_emails}: {recipient_email}")
        update_campaign_progress(campaign_id, i + 1, success_count)
        if log_id:
            update_operation_log(log_id, progress=progress)

        st.session_state.active_campaign['current_index'] = i + 1
        st.session_state.active_campaign['emails_sent'] = success_count

        if cancel_button:
            st.session_state.campaign_cancelled = True
            st.warning("Campaign cancellation requested...")
            break

        time.sleep(0.1)

    if not st.session_state.campaign_cancelled:
        campaign_data = {
            'status': 'completed',
            'completed_at': datetime.now(),
            'emails_sent': success_count,
        }
        db = get_firestore_db()
        if db:
            doc_ref = db.collection("active_campaigns").document(str(campaign_id))
            doc_ref.update(campaign_data)

        record = {
            'timestamp': datetime.now(),
            'journal': journal,
            'emails_sent': success_count,
            'total_emails': total_emails,
            'subject': ','.join(selected_subjects) if selected_subjects else email_subject,
            'email_ids': ','.join(email_ids),
            'service': st.session_state.email_service,
        }
        st.session_state.campaign_history.append(record)
        save_campaign_history(record)

        progress_bar.progress(1.0)
        status_text.text("Campaign completed")
        st.success(f"Campaign completed! {success_count} of {total_emails} emails sent successfully.")
        if log_id:
            update_operation_log(log_id, status="completed", progress=1.0)
        delete_campaign(campaign_id)
    else:
        st.warning(f"Campaign cancelled. {success_count} of {total_emails} emails were sent.")
        if log_id:
            update_operation_log(log_id, status="failed", progress=(success_count / total_emails if total_emails else 0))

# ----- Operation Logging Functions -----
def start_operation_log(operation_type, meta=None):
    """Create a log entry in Firestore and return the log ID."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        log_ref = db.collection("operation_logs").document()
        log_data = {
            "user": st.session_state.get("username", "admin"),
            "operation_type": operation_type,
            "status": "in_progress",
            "timestamp": datetime.now(),
            "last_updated": datetime.now(),
            "meta": meta or {},
            "progress": 0,
        }
        log_ref.set(log_data)
        return log_ref.id
    except Exception as e:
        st.error(f"Failed to log operation: {str(e)}")
        return None


def update_operation_log(log_id, status=None, progress=None, meta=None):
    """Update status/progress for a given log entry."""
    try:
        if not log_id:
            return
        db = get_firestore_db()
        if not db:
            return

        update_data = {"last_updated": datetime.now()}
        if status:
            update_data["status"] = status
        if progress is not None:
            update_data["progress"] = progress
        if meta is not None:
            update_data["meta"] = meta

        db.collection("operation_logs").document(log_id).update(update_data)
    except Exception as e:
        st.error(f"Failed to update log: {str(e)}")


def get_incomplete_logs():
    """Return any logs for current user that are not completed."""
    try:
        db = get_firestore_db()
        if not db:
            return []

        logs_ref = db.collection("operation_logs")\
            .where("user", "==", st.session_state.get("username", "admin"))\
            .where("status", "in", ["in_progress", "failed"])

        return [doc.to_dict() | {"id": doc.id} for doc in logs_ref.stream()]
    except Exception as e:
        st.error(f"Failed to fetch logs: {str(e)}")
        return []


def check_incomplete_operations():
    """Show any incomplete operations in the sidebar."""
    logs = get_incomplete_logs()
    if not logs:
        return

    st.sidebar.markdown("### Pending Operations")
    for log in logs:
        log_id = log.get("id")
        op_type = log.get("operation_type")
        progress = log.get("progress", 0)
        status = log.get("status")
        meta = log.get("meta", {})
        if op_type == "campaign" and meta.get("campaign_id"):
            cid = meta.get("campaign_id")
            label = f"Resume Campaign {cid} ({int(progress*100)}%)"
            if st.sidebar.button(label, key=f"resume_{cid}"):
                campaign = get_campaign_state(cid)
                if campaign:
                    st.session_state.active_campaign = campaign
                    st.experimental_rerun()
            if st.sidebar.button("Mark as Complete", key=f"complete_sb_{log_id}"):
                update_operation_log(log_id, status="completed", progress=1.0)
                delete_campaign(cid)
                st.experimental_rerun()
            if st.sidebar.button("Delete", key=f"delete_sb_{cid}"):
                delete_campaign(cid)
                update_operation_log(log_id, status="completed", progress=1.0)
                st.experimental_rerun()
        elif op_type == "verification":
            file_name = meta.get("file_name", meta.get("source", ""))
            st.sidebar.write(f"{file_name} - {status} ({int(progress*100)}%)")
            if progress < 1.0:
                st.sidebar.markdown("<div class='resume-btn'>", unsafe_allow_html=True)
                if st.sidebar.button("Resume", key=f"resume_sb_{log_id}"):
                    pdata = load_verification_progress(log_id)
                    if pdata:
                        st.session_state.verification_resume_data = pdata
                        st.session_state.verification_resume_log_id = log_id
                        st.session_state.current_verification_file = file_name
                        st.experimental_rerun()
                st.sidebar.markdown("</div>", unsafe_allow_html=True)
            if st.sidebar.button("View Results", key=f"view_sb_{log_id}"):
                result = load_verification_results(log_id)
                if result:
                    df, stats, fname = result
                    st.session_state.verified_emails = df
                    st.session_state.verification_stats = stats
                    st.session_state.current_verification_file = fname
                    st.experimental_rerun()
                else:
                    st.sidebar.write("Results not available")
            if st.sidebar.button("Mark as Complete", key=f"complete_sb_{log_id}"):
                update_operation_log(log_id, status="completed", progress=1.0)
                st.experimental_rerun()

def display_pending_operations(operation_type):
    """Display incomplete operations of a specific type on the main page."""
    logs = [l for l in get_incomplete_logs() if l.get("operation_type") == operation_type]
    if not logs:
        return

    st.warning("Pending Operations")
    for log in logs:
        log_id = log.get("id")
        meta = log.get("meta", {})
        progress = log.get("progress", 0)
        status = log.get("status")
        if operation_type == "campaign" and meta.get("campaign_id"):
            cid = meta.get("campaign_id")
            journal = meta.get("journal", "")
            file_name = meta.get("file_name", "")
            label = f"Resume Campaign {cid} ({int(progress*100)}%)"
            st.write(f"**{journal}** - {file_name} - {status}")
            cols = st.columns(3)
            if cols[0].button(label, key=f"resume_main_{cid}"):
                campaign = get_campaign_state(cid)
                if campaign:
                    st.session_state.active_campaign = campaign
                    st.experimental_rerun()
            if cols[1].button("Mark as Complete", key=f"complete_{log_id}"):
                update_operation_log(log_id, status="completed", progress=1.0)
                delete_campaign(cid)
                st.experimental_rerun()
            if cols[2].button("Delete", key=f"delete_{cid}"):
                delete_campaign(cid)
                update_operation_log(log_id, status="completed", progress=1.0)
                st.experimental_rerun()
        elif operation_type == "verification":
            file_name = meta.get("file_name", meta.get("source", ""))
            st.write(f"{file_name} - {status} ({int(progress*100)}%)")
            col_count = 3 if progress < 1.0 else 2
            cols = st.columns(col_count)
            idx = 0
            if progress < 1.0:
                cols[idx].markdown("<div class='resume-btn'>", unsafe_allow_html=True)
                if cols[idx].button("Resume", key=f"resume_verify_{log_id}"):
                    pdata = load_verification_progress(log_id)
                    if pdata:
                        st.session_state.verification_resume_data = pdata
                        st.session_state.verification_resume_log_id = log_id
                        st.session_state.current_verification_file = file_name
                        st.experimental_rerun()
                cols[idx].markdown("</div>", unsafe_allow_html=True)
                idx += 1
            if cols[idx].button("View Results", key=f"view_{log_id}"):
                result = load_verification_results(log_id)
                if result:
                    df, stats, fname = result
                    st.session_state.verified_emails = df
                    st.session_state.verification_stats = stats
                    st.session_state.current_verification_file = fname
                    st.experimental_rerun()
                else:
                    st.warning("Results not available")
            if cols[idx+1].button("Mark as Complete", key=f"complete_{log_id}"):
                update_operation_log(log_id, status="completed", progress=1.0)
                st.experimental_rerun()

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
    display_pending_operations("campaign")

    if st.session_state.active_campaign and not st.session_state.campaign_paused:
        ac = st.session_state.active_campaign
        if ac.get('current_index', 0) < ac.get('total_emails', 0):
            st.info(f"Resuming campaign {ac.get('campaign_id')} - {ac.get('current_index')}/{ac.get('total_emails')}")
            if 'current_recipient_list' not in st.session_state or st.session_state.current_recipient_list is None:
                st.session_state.current_recipient_list = pd.DataFrame(ac.get('recipient_list', []))
            execute_campaign(ac)
            return

    if not st.session_state.block_settings_loaded:
        load_block_settings()
        st.session_state.block_settings_loaded = True

    if not st.session_state.sender_name_loaded:
        stored_name = load_sender_name()
        if stored_name:
            st.session_state.sender_base_name = stored_name
        update_sender_name()
        st.session_state.sender_name_loaded = True

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
        service_options = ["SMTP2GO", "MAILGUN"]
        current_service = (st.session_state.email_service or "MAILGUN").upper()
        default_index = service_options.index("MAILGUN")
        if current_service in service_options:
            default_index = service_options.index(current_service)
        st.session_state.email_service = st.selectbox(
            "Select Email Service",
            service_options,
            index=default_index,
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
            value=st.session_state.sender_base_name,
            key="sender_base_name"
        )
        if st.button("Save Sender Name", key="save_sender_name"):
            if save_sender_name(st.session_state.sender_base_name):
                update_sender_name()
                st.success("Sender name saved!")

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
            - $$Unsubscribe_Link$$: Unsubscribe link
            - %unsubscribe_url%: Mailgun unsubscribe link""")

        with preview_col:
            st.markdown("**Preview**")
            preview_html = email_body.replace("$$Author_Name$$", "Professor John Doe")
            preview_html = preview_html.replace(
                "$$Author_Address$$",
                "Department of Computer Science<br>Harvard University<br>Cambridge, MA<br>United States"
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
            for unsubscribe_placeholder in ("$$Unsubscribe_Link$$", "%unsubscribe_url%"):
                preview_html = preview_html.replace(
                    unsubscribe_placeholder,
                    "https://pphmjopenaccess.com/unsubscribe?email=john.doe@harvard.edu"
                )


            components.html(preview_html, height=400, scrolling=True)

    # Recipient management tabs
    st.subheader("Recipient Management")
    uploaded_file = None
    file_content = None
    df = None

    recipients_tab, unsubscribed_tab = st.tabs(["Recipient List", "Unsubscribed Users"])

    with recipients_tab:
        col_src, col_clock = st.columns([1, 2])
        with col_src:
            file_source = st.radio(
                "Select file source",
                ["Local Upload", "Cloud Storage"],
                key="recipient_file_source",
            )
        with col_clock:
            display_world_clocks()

        unsubscribed_lookup = st.session_state.get("unsubscribed_email_lookup", set())

        if file_source == "Local Upload":
            uploaded_file = st.file_uploader(
                "Upload recipient list (CSV or TXT)",
                type=["csv", "txt"],
                key="recipient_file_uploader",
            )
            if uploaded_file:
                st.session_state.current_recipient_file = uploaded_file.name
                if uploaded_file.name.endswith('.txt'):
                    file_content = read_uploaded_text(uploaded_file)
                    df = parse_email_entries(file_content)
                else:
                    df = pd.read_csv(uploaded_file)

                st.session_state.current_recipient_list = df
                st.dataframe(df.head())
                st.info(f"Total emails loaded: {len(df)}")

                if unsubscribed_lookup and 'email' in df.columns:
                    unsubscribed_matches = (
                        df['email']
                        .fillna("")
                        .astype(str)
                        .str.lower()
                        .isin(unsubscribed_lookup)
                    )
                    unsubscribed_count = int(unsubscribed_matches.sum())
                    if unsubscribed_count:
                        st.warning(
                            f"{unsubscribed_count} recipient(s) have unsubscribed and will be skipped automatically."
                        )

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
            if 'firebase_files' not in st.session_state or not st.session_state.firebase_files:
                st.session_state.firebase_files = list_firebase_files()

            if st.session_state.firebase_files:
                sel_col, del_col = st.columns([8, 1])
                selected_file = sel_col.selectbox(
                    "Select file from Firebase",
                    st.session_state.firebase_files,
                    key="select_file_campaign",
                )
                if del_col.button("🗑️", key="del_selected_campaign"):
                    if delete_firebase_file(selected_file):
                        st.session_state.firebase_files.remove(selected_file)
                        st.success(f"{selected_file} deleted!")
                        st.experimental_rerun()

                col_load, _ = st.columns([1, 1])
                if col_load.button("Load File", key="load_file_campaign"):
                    file_content = download_from_firebase(selected_file)
                    if file_content:
                        st.session_state.current_recipient_file = selected_file
                        if selected_file.endswith('.txt'):
                            df = parse_email_entries(file_content)
                        else:
                            df = pd.read_csv(StringIO(file_content))

                        st.session_state.current_recipient_list = df
                        st.dataframe(df.head())
                        st.info(f"Total emails loaded: {len(df)}")

                        if unsubscribed_lookup and 'email' in df.columns:
                            unsubscribed_matches = (
                                df['email']
                                .fillna("")
                                .astype(str)
                                .str.lower()
                                .isin(unsubscribed_lookup)
                            )
                            unsubscribed_count = int(unsubscribed_matches.sum())
                            if unsubscribed_count:
                                st.warning(
                                    f"{unsubscribed_count} recipient(s) have unsubscribed and will be skipped automatically."
                                )

                        refresh_journal_data()
            else:
                st.info("No files found in Cloud Storage")

    with unsubscribed_tab:
        st.caption(
            "These contacts opted out via the Mailgun webhook and are automatically excluded from future campaigns."
        )
        if st.button("Refresh unsubscribe list"):
            unsubscribe_records = load_unsubscribed_users(force_refresh=True)
        else:
            unsubscribe_records = st.session_state.get("unsubscribed_users", [])

        unsubscribed_lookup = st.session_state.get("unsubscribed_email_lookup", set())

        st.markdown("### Add Suppression Emails")
        st.write(
            "Use the options below to add unsubscribe/suppression emails manually or from a CSV file."
        )

        with st.form("manual_unsubscribe_form"):
            manual_emails_input = st.text_area(
                "Enter email addresses (one per line or separated by commas)",
                height=120,
                key="manual_unsubscribe_input",
            )
            manual_submit = st.form_submit_button("Add Email(s)")

        if manual_submit:
            raw_candidates = re.split(r"[\s,;]+", manual_emails_input or "")
            raw_candidates = [item for item in raw_candidates if item]
            valid_emails = []
            invalid_entries = []
            for candidate in raw_candidates:
                normalized = candidate.strip().lower()
                if not normalized:
                    continue
                if EMAIL_VALIDATION_REGEX.match(normalized):
                    valid_emails.append(normalized)
                else:
                    invalid_entries.append(candidate)

            valid_unique_emails = []
            seen = set()
            for email in valid_emails:
                if email in seen:
                    continue
                seen.add(email)
                valid_unique_emails.append(email)

            existing_emails = []
            added_count = 0
            failed_emails = []
            for email in valid_unique_emails:
                if email in unsubscribed_lookup:
                    existing_emails.append(email)
                    continue
                if set_email_unsubscribed(email):
                    added_count += 1
                else:
                    failed_emails.append(email)

            if added_count:
                unsubscribe_records = load_unsubscribed_users(force_refresh=True)
                unsubscribed_lookup = st.session_state.get("unsubscribed_email_lookup", set())
                st.success(f"Added {added_count} email(s) to the unsubscribe list.")
            if existing_emails:
                st.info(
                    f"{len(existing_emails)} email(s) were already unsubscribed and were skipped."
                )
            if invalid_entries:
                st.warning(
                    f"{len(invalid_entries)} entry(ies) were not valid email addresses and were skipped."
                )
            if failed_emails:
                st.error(
                    f"Unable to save {len(failed_emails)} email(s). Please try again."
                )
            if not any([added_count, existing_emails, invalid_entries, failed_emails]):
                st.info("No emails were provided to add.")

        suppression_csv = st.file_uploader(
            "Upload suppression CSV (email addresses in column C)",
            type=["csv"],
            key="suppression_csv_uploader",
            help="Only the third column (column C) will be processed for email addresses.",
        )

        process_csv_clicked = False
        if suppression_csv is not None:
            process_csv_clicked = st.button(
                "Add Emails from CSV",
                key="process_suppression_csv",
            )

        if suppression_csv is not None and process_csv_clicked:
            try:
                suppression_csv.seek(0)
                csv_df = pd.read_csv(
                    suppression_csv,
                    header=None,
                    dtype=str,
                    on_bad_lines="skip",
                )
            except Exception as exc:
                st.error(f"Failed to read CSV file: {exc}")
                csv_df = None

            if csv_df is not None:
                if csv_df.shape[1] < 3:
                    st.error("The uploaded CSV must contain at least three columns.")
                else:
                    column_c = (
                        csv_df.iloc[:, 2]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .str.lower()
                    )
                    valid_emails = []
                    invalid_entries = []
                    for value in column_c.unique():
                        if not value:
                            continue
                        if EMAIL_VALIDATION_REGEX.match(value):
                            valid_emails.append(value)
                        else:
                            invalid_entries.append(value)

                    if valid_emails:
                        added_count = 0
                        already_present = []
                        failed_emails = []
                        for email in valid_emails:
                            if email in unsubscribed_lookup:
                                already_present.append(email)
                                continue
                            if set_email_unsubscribed(email):
                                added_count += 1
                            else:
                                failed_emails.append(email)

                        if added_count:
                            unsubscribe_records = load_unsubscribed_users(force_refresh=True)
                            unsubscribed_lookup = st.session_state.get(
                                "unsubscribed_email_lookup", set()
                            )
                            st.success(
                                f"Added {added_count} email(s) from the CSV to the unsubscribe list."
                            )
                        if already_present:
                            st.info(
                                f"{len(already_present)} email(s) were already unsubscribed and were skipped."
                            )
                        if failed_emails:
                            st.error(
                                f"Failed to add {len(failed_emails)} email(s) from the CSV."
                            )
                    else:
                        st.info("No valid email addresses were found in column C of the CSV file.")

                    if invalid_entries:
                        st.warning(
                            f"{len(invalid_entries)} entries in column C were not valid email addresses."
                        )

        signing_key = config.get("mailgun", {}).get("signing_key")
        if not signing_key and config.get("mailgun", {}).get("api_key"):
            st.info(
                "Using the Mailgun API key for webhook signature verification. "
                "Set MAILGUN_SIGNING_KEY for enhanced security."
            )

        webhook_base_url = config.get("webhook", {}).get("url")
        if webhook_base_url:
            webhook_endpoint = webhook_base_url.rstrip('/') + "/unsubscribe"
            st.markdown(f"**Webhook Endpoint:** `{webhook_endpoint}`")
        else:
            port = os.getenv("WEBHOOK_PORT", "8000")
            st.markdown(
                f"Webhook server listening on `/unsubscribe` (local port {port}). "
                "Expose this URL publicly and set WEBHOOK_URL for Mailgun notifications."
            )

        if unsubscribe_records:
            display_rows = []
            for record in unsubscribe_records:
                unsubscribed_at = record.get("unsubscribed_at")
                unsubscribed_at_str = ""
                if isinstance(unsubscribed_at, datetime):
                    unsubscribed_at_str = unsubscribed_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                display_rows.append(
                    {
                        "Email": record.get("email"),
                        "Unsubscribed At": unsubscribed_at_str,
                        "Event": record.get("event"),
                        "Mailing List": record.get("mailing_list"),
                        "Reason": record.get("reason"),
                        "Tags": ", ".join(record.get("tags", [])) if record.get("tags") else "",
                    }
                )

            unsubscribed_df = pd.DataFrame(display_rows)
            st.metric("Total unsubscribed", len(unsubscribed_df))
            st.dataframe(unsubscribed_df)

            csv_buffer = StringIO()
            unsubscribed_df.to_csv(csv_buffer, index=False)
            st.download_button(
                "Download Unsubscribed CSV",
                csv_buffer.getvalue(),
                "unsubscribed_users.csv",
                "text/csv",
                key="download_unsubscribed_csv",
            )
        else:
            st.info("No unsubscribed users recorded yet.")


    file_source = st.session_state.get("recipient_file_source", "Local Upload")


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
            if file_source == "Local Upload" and uploaded_file:
                if uploaded_file.name.endswith('.txt'):
                    upload_to_firebase(file_content, uploaded_file.name)
                else:
                    csv_content = df.to_csv(index=False)
                    upload_to_firebase(csv_content, uploaded_file.name)
            service = (st.session_state.email_service or "MAILGUN").upper()
            if service == "SMTP2GO":
                if not config['smtp2go']['api_key']:
                    st.error("SMTP2GO API key not configured")
                    return
            else:
                mailgun_config = config.get('mailgun', {})
                if not (
                    mailgun_config.get('api_key')
                    and mailgun_config.get('domain')
                    and (mailgun_config.get('sender') or st.session_state.sender_email)
                ):
                    st.error("Mailgun configuration not complete")
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
                'file_name': st.session_state.get('current_recipient_file'),
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

            log_id = start_operation_log(
                "campaign",
                {
                    "campaign_id": campaign_id,
                    "journal": selected_journal,
                    "file_name": st.session_state.get("current_recipient_file"),
                },
            )
            if log_id:
                campaign_data['log_id'] = log_id

            if not save_campaign_state(campaign_data):
                st.error("Failed to save campaign state")
                return

            st.session_state.active_campaign = campaign_data
            st.session_state.campaign_paused = False
            st.session_state.campaign_cancelled = False

            execute_campaign(campaign_data)


def editor_invitation_section():
    st.header("Editor Invitation")
    display_pending_operations("campaign")

    if st.session_state.active_campaign and not st.session_state.campaign_paused:
        ac = st.session_state.active_campaign
        if ac.get('current_index', 0) < ac.get('total_emails', 0):
            st.info(f"Resuming campaign {ac.get('campaign_id')} - {ac.get('current_index')}/{ac.get('total_emails')}")

    if not st.session_state.block_settings_loaded:
        load_block_settings()
        st.session_state.block_settings_loaded = True

    if not st.session_state.sender_name_loaded:
        stored_name = load_sender_name()
        if stored_name:
            st.session_state.sender_base_name = stored_name
        update_editor_sender_name()
        st.session_state.sender_name_loaded = True

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
        service_options = ["SMTP2GO", "MAILGUN"]
        current_service = (st.session_state.email_service or "MAILGUN").upper()
        default_index = service_options.index("MAILGUN")
        if current_service in service_options:
            default_index = service_options.index(current_service)
        st.session_state.email_service = st.selectbox(
            "Select Email Service",
            service_options,
            index=default_index,
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
            value=st.session_state.sender_base_name,
            key="sender_base_name"
        )
        if st.button("Save Sender Name", key="save_sender_name_editor"):
            if save_sender_name(st.session_state.sender_base_name):
                update_editor_sender_name()
                st.success("Sender name saved!")

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
            - $$Unsubscribe_Link$$: Unsubscribe link
            - %unsubscribe_url%: Mailgun unsubscribe link""")

        with preview_col:
            st.markdown("**Preview**")
            preview_html = email_body.replace("$$Author_Name$$", "Professor John Doe")
            preview_html = preview_html.replace(
                "$$Author_Address$$",
                "Department of Computer Science<br>Harvard University<br>Cambridge, MA<br>United States"
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
            for unsubscribe_placeholder in ("$$Unsubscribe_Link$$", "%unsubscribe_url%"):
                preview_html = preview_html.replace(
                    unsubscribe_placeholder,
                    "https://pphmjopenaccess.com/unsubscribe?email=john.doe@harvard.edu"
                )

            components.html(preview_html, height=400, scrolling=True)

    st.subheader("Recipient List")
    col_src, col_clock = st.columns([1, 2])
    with col_src:
        file_source = st.radio("Select file source", ["Local Upload", "Cloud Storage"], key="recipient_source_editor")
    with col_clock:
        display_world_clocks()

    if file_source == "Local Upload":
        uploaded_file = st.file_uploader("Upload recipient list (CSV or TXT)", type=["csv", "txt"], key="recipient_upload_editor")
        if uploaded_file:
            st.session_state.current_recipient_file = uploaded_file.name
            if uploaded_file.name.endswith('.txt'):
                file_content = read_uploaded_text(uploaded_file)
                df = parse_email_entries(file_content)
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
        if 'firebase_files' not in st.session_state or not st.session_state.firebase_files:
            st.session_state.firebase_files = list_firebase_files()

        if 'firebase_files' in st.session_state and st.session_state.firebase_files:
            sel_col, del_col = st.columns([8,1])
            selected_file = sel_col.selectbox(
                "Select file from Firebase",
                st.session_state.firebase_files,
                key="select_file_editor",
            )
            if del_col.button("🗑️", key="del_selected_editor"):
                if delete_firebase_file(selected_file):
                    st.session_state.firebase_files.remove(selected_file)
                    st.success(f"{selected_file} deleted!")
                    st.experimental_rerun()

            col_load, _ = st.columns([1, 1])
            if col_load.button("Load File", key="load_file_editor"):
                file_content = download_from_firebase(selected_file)
                if file_content:
                    st.session_state.current_recipient_file = selected_file
                    if selected_file.endswith('.txt'):
                        df = parse_email_entries(file_content)
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
            if file_source == "Local Upload" and uploaded_file:
                if uploaded_file.name.endswith('.txt'):
                    upload_to_firebase(file_content, uploaded_file.name)
                else:
                    csv_content = df.to_csv(index=False)
                    upload_to_firebase(csv_content, uploaded_file.name)
            service = (st.session_state.email_service or "MAILGUN").upper()
            if service == "SMTP2GO":
                if not config['smtp2go']['api_key']:
                    st.error("SMTP2GO API key not configured")
                    return
            else:
                mailgun_config = config.get('mailgun', {})
                if not (
                    mailgun_config.get('api_key')
                    and mailgun_config.get('domain')
                    and (mailgun_config.get('sender') or st.session_state.sender_email)
                ):
                    st.error("Mailgun configuration not complete")
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
                'file_name': st.session_state.get('current_recipient_file'),
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

            log_id = start_operation_log(
                "campaign",
                {
                    "campaign_id": campaign_id,
                    "journal": selected_editor_journal,
                    "file_name": st.session_state.get("current_recipient_file"),
                },
            )
            if log_id:
                campaign_data['log_id'] = log_id

            if not save_campaign_state(campaign_data):
                st.error("Failed to save campaign state")
                return

            st.session_state.active_campaign = campaign_data
            st.session_state.campaign_paused = False
            st.session_state.campaign_cancelled = False

            execute_campaign(campaign_data)

# Email Verification Section
def email_verification_section():
    st.header("Email Verification")
    display_pending_operations("verification")

    # If a resume action was triggered, continue the verification
    if st.session_state.get("verification_resume_data"):
        pdata = st.session_state.verification_resume_data
        log_id = st.session_state.verification_resume_log_id
        with st.spinner("Resuming verification..."):
            result_df = process_email_list(
                pdata.get("file_content", ""),
                config['millionverifier']['api_key'],
                log_id,
                resume_data=pdata,
            )
            if not result_df.empty:
                st.session_state.verified_emails = result_df
                prepare_verification_downloads(result_df)
        st.session_state.verification_resume_data = None
        st.session_state.verification_resume_log_id = None
    
    # Check verification quota using correct endpoint
    if config['millionverifier']['api_key']:
        with st.spinner("Checking verification quota..."):
            remaining_quota = check_millionverifier_quota(
                config['millionverifier']['api_key']
            )
        st.metric("Remaining Verification Credits", remaining_quota)
    else:
        st.warning("MillionVerifier API key not configured")
    
    # File Upload for Verification
    st.subheader("Email List Verification")
    file_source = st.radio("Select file source for verification", ["Local Upload", "Cloud Storage"])
    
    if file_source == "Local Upload":
        uploaded_file = st.file_uploader("Upload email list for verification (TXT format)", type=["txt"])
        if uploaded_file:
            st.session_state.current_verification_file = uploaded_file.name
            file_content = read_uploaded_text(uploaded_file)
            st.text_area("File Content Preview", file_content, height=150)
            
            if st.button("Verify Emails"):
                if not config['millionverifier']['api_key']:
                    st.error("Please configure MillionVerifier API Key first")
                    return
                
                with st.spinner("Verifying emails..."):
                    log_id = start_operation_log(
                        "verification", {"file_name": uploaded_file.name})
                    result_df = process_email_list(file_content, config['millionverifier']['api_key'], log_id)
                    if not result_df.empty:
                        st.session_state.verified_emails = result_df
                        prepare_verification_downloads(result_df)
                        st.dataframe(result_df)
                    else:
                        st.error("No valid emails found in the file")
    else:
        if 'firebase_files_verification' not in st.session_state or not st.session_state.firebase_files_verification:
            st.session_state.firebase_files_verification = list_firebase_files()

        if 'firebase_files_verification' in st.session_state and st.session_state.firebase_files_verification:
            sel_col, del_col = st.columns([8,1])
            selected_file = sel_col.selectbox(
                "Select file to verify from Firebase",
                st.session_state.firebase_files_verification,
            )
            if del_col.button("🗑️", key="del_selected_verify"):
                if delete_firebase_file(selected_file):
                    st.session_state.firebase_files_verification.remove(selected_file)
                    st.success(f"{selected_file} deleted!")
                    st.experimental_rerun()

            col_load, _ = st.columns([1, 1])
            if col_load.button("Load File for Verification"):
                file_content = download_from_firebase(selected_file)
                if file_content:
                    st.session_state.current_verification_file = selected_file
                    st.text_area("File Content Preview", file_content, height=150)
                    st.session_state.current_verification_list = file_content

            if 'current_verification_list' in st.session_state and st.button("Start Verification"):
                if not config['millionverifier']['api_key']:
                    st.error("Please configure MillionVerifier API Key first")
                    return

                with st.spinner("Verifying emails..."):
                    log_id = start_operation_log(
                        "verification", {"file_name": selected_file})
                    result_df = process_email_list(
                        st.session_state.current_verification_list,
                        config['millionverifier']['api_key'],
                        log_id,
                    )
                    if not result_df.empty:
                        st.session_state.verified_emails = result_df
                        prepare_verification_downloads(result_df)
                        st.dataframe(result_df)
                    else:
                        st.error("No valid emails found in the file")

        else:
            st.info("No files found in Cloud Storage")
    
    # Verification Results and Reports
    if not st.session_state.verified_emails.empty:
        st.subheader("Verification Results")

        if st.session_state.get("auto_download_good"):
            good_content = st.session_state.get("good_download_content", "")
            good_file_name = st.session_state.get("good_download_file_name", "good_emails.txt")
            b64 = base64.b64encode(good_content.encode()).decode()
            download_html = f'<a id="auto_good_download" href="data:text/plain;base64,{b64}" download="{good_file_name}"></a>'
            download_html += "<script>document.getElementById('auto_good_download').click();</script>"
            components.html(download_html)
            st.session_state.auto_download_good = False

        if st.session_state.get("auto_download_low_risk"):
            low_risk_content = st.session_state.get("low_risk_download_content", "")
            low_risk_file_name = st.session_state.get("low_risk_download_file_name", "low_risk_emails.txt")
            b64 = base64.b64encode(low_risk_content.encode()).decode()
            download_html = (
                f'<a id="auto_low_risk_download" href="data:text/plain;base64,{b64}" '
                f'download="{low_risk_file_name}"></a>'
            )
            download_html += "<script>document.getElementById('auto_low_risk_download').click();</script>"
            components.html(download_html)
            st.session_state.auto_download_low_risk = False

        if st.session_state.get("auto_download_high_risk"):
            high_risk_content = st.session_state.get("high_risk_download_content", "")
            high_risk_file_name = st.session_state.get("high_risk_download_file_name", "high_risk_emails.txt")
            b64 = base64.b64encode(high_risk_content.encode()).decode()
            download_html = (
                f'<a id="auto_high_risk_download" href="data:text/plain;base64,{b64}" '
                f'download="{high_risk_file_name}"></a>'
            )
            download_html += "<script>document.getElementById('auto_high_risk_download').click();</script>"
            components.html(download_html)
            st.session_state.auto_download_high_risk = False

        # Display stats
        stats_defaults = {
            'good': 0,
            'good_percent': 0,
            'bad': 0,
            'bad_percent': 0,
            'risky': 0,
            'risky_percent': 0,
            'low_risk': 0,
            'low_risk_percent': 0,
        }
        stats = {**stats_defaults, **st.session_state.verification_stats}

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Good Emails", f"{stats['good']} ({stats['good_percent']}%)",
                     help="Good emails are valid, existing emails. It is safe to send emails to them.")
        with col2:
            st.metric("Bad Emails", f"{stats['bad']} ({stats['bad_percent']}%)",
                     help="Bad emails don't exist, don't email them!")
        with col3:
            st.metric("Risky Emails", f"{stats['risky']} ({stats['risky_percent']}%)",
                     help="Risky emails may exist or not. Use with caution.")
        with col4:
            st.metric(
                "Low Risk Emails",
                f"{stats['low_risk']} ({stats['low_risk_percent']}%)",
                help="Catch-all addresses verified as low risk. Engage with caution and monitor delivery.",
            )

        # Bar chart instead of pie chart
        plt.style.use("seaborn-v0_8-whitegrid")
        fig, ax = plt.subplots(figsize=(6, 3))
        categories = ['Good', 'Bad', 'Risky', 'Low Risk']
        counts = [stats['good'], stats['bad'], stats['risky'], stats['low_risk']]
        colors = ['#4CAF50', '#F44336', '#9C27B0', '#2196F3']
        
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
        col1, col2, col3, col4 = st.columns(4)
        good_count = st.session_state.verification_stats.get('good', 0)
        low_risk_count = st.session_state.verification_stats.get('low_risk', 0)
        high_risk_count = st.session_state.verification_stats.get('risky', 0)
        source_name = st.session_state.get('current_verification_file')

        with col1:
            good_content = generate_report_file(st.session_state.verified_emails, "good")
            original_name = source_name or "good_emails.txt"
            good_file_name = generate_good_emails_filename(original_name, good_count)
            st.download_button(
                label="Good Emails",
                data=good_content,
                file_name=good_file_name,
                mime="text/plain",
                help="Download emails verified as good",
                key="good_emails_btn",
                use_container_width=True
            )

        with col2:
            low_risk_content = generate_report_file(st.session_state.verified_emails, "low_risk")
            low_risk_name = source_name or "low_risk_emails.txt"
            low_risk_file_name = generate_low_risk_emails_filename(low_risk_name, low_risk_count)
            st.download_button(
                label="Low Risk Emails",
                data=low_risk_content,
                file_name=low_risk_file_name,
                mime="text/plain",
                help="Download catch-all addresses classified as low risk",
                key="low_risk_emails_btn",
                use_container_width=True,
            )

        with col3:
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

        with col4:
            risky_content = generate_report_file(st.session_state.verified_emails, "risky")
            high_risk_name = source_name or "high_risk_emails.txt"
            high_risk_file_name = generate_high_risk_emails_filename(high_risk_name, high_risk_count)
            st.download_button(
                label="High Risk Emails",
                data=risky_content,
                file_name=high_risk_file_name,
                mime="text/plain",
                help="Download emails flagged as high risk",
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

    service = (st.session_state.email_service or "MAILGUN").upper()

    if service == "SMTP2GO":
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
        st.info("Mailgun analytics are not available in this dashboard yet.")

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

    service = (st.session_state.email_service or "MAILGUN").upper()

    if service == "SMTP2GO":
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
        st.info("Mailgun analytics are not available in this dashboard yet.")

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
        check_incomplete_operations()
    
    # Initialize services
    if not st.session_state.firebase_initialized:
        initialize_firebase()

    ensure_webhook_server()

    if not st.session_state.unsubscribed_users_loaded:
        load_unsubscribed_users()

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
