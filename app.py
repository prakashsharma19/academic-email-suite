import streamlit as st
import streamlit.components.v1 as components
import boto3
import pandas as pd
import datetime
import time
import requests
import os
import pytz
import re
import hashlib
import logging
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid
from io import StringIO
import base64
import html
from pathlib import Path
from google.oauth2 import service_account
from streamlit_ace import st_ace
import firebase_admin
from firebase_admin import credentials, firestore
import threading
import copy
import gc
from urllib.parse import urlencode
import textwrap
import math


logger = logging.getLogger("academic_email_suite")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

os.environ.setdefault("STREAMLIT_SERVER_FILE_WATCHER_TYPE", "none")

UNSUBSCRIBED_CACHE = {"records": [], "emails": set(), "loaded": False}
UNSUBSCRIBED_CACHE_LOCK = threading.Lock()

EMAIL_VALIDATION_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

KVN_MAX_EMAILS_PER_BATCH = 700
KVN_SCHEDULE_COLLECTION = "kvn_settings"
KVN_SCHEDULE_DOCUMENT = "send_schedule"
KVN_SLOT_BUFFER = timedelta(hours=1)
DEFAULT_DISPLAY_TIMEZONE = "Asia/Kolkata"
try:
    KVN_DISPLAY_TIMEZONE = pytz.timezone(
        os.getenv("KVN_DISPLAY_TIMEZONE", DEFAULT_DISPLAY_TIMEZONE)
    )
except Exception:
    KVN_DISPLAY_TIMEZONE = pytz.timezone(DEFAULT_DISPLAY_TIMEZONE)

st.set_page_config(
    page_title="PPH Email Verifier",
    layout="wide",
    page_icon="📧",
    initial_sidebar_state="collapsed",
    menu_items={
        'About': "### Academic Email Management Suite\n\nFor assistance please contact publication@pphmj.com"
    }
)


@st.cache_data(show_spinner=False)
def load_theme_css():
    css_path = Path(__file__).parent / "templates" / "light_theme.css"
    try:
        return css_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Light theme CSS not found at %s", css_path)
        return ""


# Light Theme with Footer
def set_light_theme():
    css = load_theme_css()
    if css:
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


set_light_theme()

def format_duration(seconds):
    """Format a duration in seconds into a short human readable string."""
    if seconds is None:
        return ""
    if seconds <= 0:
        return "Less than a second"

    seconds = int(seconds)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def render_progress_indicator(placeholder, label, progress, eta_seconds=None):
    """Render a circular progress indicator alongside descriptive text."""
    if placeholder is None:
        return

    safe_label = html.escape(label or "")
    bounded_progress = min(max(progress or 0, 0.0), 1.0)
    percent_complete = int(round(bounded_progress * 100))
    sweep_angle = bounded_progress * 360

    if eta_seconds is None and bounded_progress > 0:
        eta_text = "Estimating remaining time..."
    elif eta_seconds is None or eta_seconds <= 0:
        eta_text = ""
    else:
        eta_text = f"Estimated time remaining: {format_duration(eta_seconds)}"

    eta_html = (
        f"<span class='progress-eta'>{html.escape(eta_text)}</span>" if eta_text else ""
    )

    html_markup = textwrap.dedent(
        f"""
        <div class="progress-wrapper">
            <div class="progress-circle" style="background: conic-gradient(var(--primary-color) {sweep_angle:.2f}deg, rgba(226, 232, 240, 0.6) {sweep_angle:.2f}deg);">
                <div class="progress-value">{percent_complete}%</div>
            </div>
            <div class="progress-details">
                <div class="progress-label">{safe_label}</div>
                {eta_html}
            </div>
        </div>
        """
    ).strip()

    placeholder.markdown(html_markup, unsafe_allow_html=True)

# Authentication System
def check_auth():
    session_state = _get_session_state()
    if session_state is None:
        st.error("Session unavailable. Please refresh the page to start a new session.")
        st.stop()

    if 'authenticated' not in session_state:
        session_state.authenticated = False

    if not session_state.authenticated:
        st.markdown(
            """
            <style>
            [data-testid="stAppViewContainer"] > .main {
                display: flex;
                align-items: center;
                justify-content: center;
                padding-top: 6vh;
                padding-bottom: 6vh;
            }
            form[data-testid="stForm"] {
                width: 100%;
                max-width: 420px;
                background: #ffffff;
                padding: 2.75rem 2.5rem 2.4rem;
                border-radius: 20px;
                box-shadow: var(--shadow-sm);
            }
            form[data-testid="stForm"] img {
                display: block;
                margin: 0 auto 1.5rem;
                width: 64px !important;
            }
            form[data-testid="stForm"] .app-login-heading {
                text-align: center;
                font-size: 1.85rem;
                margin-bottom: 0.4rem;
                color: var(--text-color);
                font-weight: 700;
            }
            form[data-testid="stForm"] .app-login-subtitle {
                text-align: center;
                color: var(--muted-text);
                margin-bottom: 1.75rem;
                font-size: 1rem;
            }
            form[data-testid="stForm"] .app-login-notice {
                text-align: center;
                color: #c62828;
                font-weight: 700;
                margin-bottom: 1.5rem;
                text-transform: uppercase;
                animation: app-login-blink 1s ease-in-out infinite;
            }
            @keyframes app-login-blink {
                0%, 100% {
                    color: #c62828;
                    text-shadow: 0 0 6px rgba(198, 40, 40, 0.75);
                }
                50% {
                    color: rgba(198, 40, 40, 0.15);
                    text-shadow: none;
                }
            }
            form[data-testid="stForm"] .stTextInput input {
                border-radius: 12px;
                padding: 0.75rem 1rem;
            }
            form[data-testid="stForm"] .stButton > button {
                width: 100%;
                padding: 0.75rem 0;
                font-size: 1.05rem;
                border-radius: 12px;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        with st.form("login_form"):
            st.image("PPHLogo_en.png", width=64)
            st.markdown(
                "<h1 class='app-login-heading'>PPH Email Verifier</h1>",
                unsafe_allow_html=True,
            )
            st.markdown(
                "<p class='app-login-subtitle'>Secure access to manage academic verification workflows</p>",
                unsafe_allow_html=True,
            )

            username = st.text_input("Username")
            password = st.text_input("Password", type="password")

            if st.form_submit_button("Login", use_container_width=True):
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
def _get_session_state():
    """Return the active Streamlit session state if available."""

    try:
        return st.session_state
    except RuntimeError:
        # When running outside the Streamlit runtime (e.g. background threads)
        # accessing session state raises a RuntimeError. In those scenarios we
        # simply fall back to returning ``None`` so the caller can avoid
        # interacting with session state.
        return None


def init_session_state():
    defaults = {
        'authenticated': False,
        'email_service': 'MAILGUN',
        'sender_name': config['sender_name'],
        'sender_email': (
            config.get('mailgun', {}).get('sender')
            or config['smtp2go']['sender']
        ),
        'journal_reply_addresses': {},
        'default_reply_to': "",
    }

    session_state = _get_session_state()
    if session_state is None:
        return

    for key, value in defaults.items():
        if key not in session_state:
            session_state[key] = value


def ensure_session_defaults(defaults):
    """Populate session state lazily using the provided defaults."""

    session_state = _get_session_state()
    if session_state is None:
        return

    for key, default in defaults.items():
        if key not in session_state:
            session_state[key] = default() if callable(default) else copy.deepcopy(default)


def invalidate_unsubscribed_cache():
    global UNSUBSCRIBED_CACHE
    with UNSUBSCRIBED_CACHE_LOCK:
        UNSUBSCRIBED_CACHE = {"records": [], "emails": set(), "loaded": False}
    try:
        st.session_state.unsubscribed_users_loaded = False
    except RuntimeError:
        # Accessing session state outside the Streamlit context will raise a RuntimeError.
        pass


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


def load_unsubscribed_users(force_refresh=False):
    if force_refresh:
        invalidate_unsubscribed_cache()

    with UNSUBSCRIBED_CACHE_LOCK:
        cache_loaded = UNSUBSCRIBED_CACHE["loaded"]
        cached_records = copy.deepcopy(UNSUBSCRIBED_CACHE["records"]) if cache_loaded else None
        cached_emails = set(UNSUBSCRIBED_CACHE["emails"]) if cache_loaded else set()

    session_state = _get_session_state()

    if cache_loaded:
        if session_state is not None:
            session_state.unsubscribed_users = cached_records or []
            session_state.unsubscribed_email_lookup = cached_emails
            session_state.unsubscribed_users_loaded = True
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

        if session_state is not None:
            session_state.unsubscribed_users = records
            session_state.unsubscribed_email_lookup = email_lookup
            session_state.unsubscribed_users_loaded = True
        return records
    except Exception as exc:
        logger.exception("Failed to load unsubscribed users: %s", exc)
        return []


def is_email_unsubscribed(email):
    if not email:
        return False
    session_state = _get_session_state()
    if session_state is not None:
        lookup = session_state.get("unsubscribed_email_lookup", set())
    else:
        with UNSUBSCRIBED_CACHE_LOCK:
            cache_loaded = UNSUBSCRIBED_CACHE.get("loaded", False)
            cached_emails = set(UNSUBSCRIBED_CACHE.get("emails", set()))

        if not cache_loaded:
            load_unsubscribed_users()
            with UNSUBSCRIBED_CACHE_LOCK:
                cached_emails = set(UNSUBSCRIBED_CACHE.get("emails", set()))

        lookup = cached_emails

    return email.lower() in lookup


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


@st.cache_data(show_spinner=False)
def load_default_journals():
    return (
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
        "OA - Universal Journal of Mathematics and Mathematical Sciences",
    )


@st.cache_data(show_spinner=False)
def load_default_editor_journals():
    return (
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
    )


JOURNALS = list(load_default_journals())
EDITOR_JOURNALS = list(load_default_editor_journals())

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
def _get_env_value(primary_key, *fallback_keys, default=None):
    """Return the first non-empty environment variable among the provided keys."""
    for key in (primary_key, *fallback_keys):
        if not key:
            continue
        value = os.getenv(key)
        if value is not None:
            value = value.strip()
            if value:
                return value
    return default


def _get_env_bool(primary_key, *fallback_keys, default=True):
    """Parse environment variables that represent boolean values."""
    raw_value = _get_env_value(primary_key, *fallback_keys, default=None)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"false", "0", "no", "off"}:
        return False
    if normalized in {"true", "1", "yes", "on"}:
        return True

    return default


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
        'kvn_smtp': {
            'host': _get_env_value("KVN_SMTP_HOST", "SMTP_HOST", default=""),
            'port': int(_get_env_value("KVN_SMTP_PORT", "SMTP_PORT", default="587") or 587),
            'username': _get_env_value("KVN_SMTP_USERNAME", "SMTP_USER", default=""),
            'password': _get_env_value("KVN_SMTP_PASSWORD", "SMTP_PASS", default=""),
            'sender': _get_env_value("KVN_SMTP_SENDER_EMAIL", "SMTP_FROM", default=""),
            'use_tls': _get_env_bool("KVN_SMTP_USE_TLS", "USE_TLS", default=True),
        },
        'sender_name': os.getenv("SENDER_NAME", "Pushpa Publishing House"),
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
        UNSUBSCRIBE_PAGE_URL = "https://hooks.pphmjopenaccess.com/unsubscribe"

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

# Ensure Firebase is ready before rendering any UI lazily


def get_firestore_db():
    if not firebase_admin._apps:
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


def send_email_via_kvn(recipient, subject, body_html, body_text, unsubscribe_link, reply_to=None):
    settings = get_effective_kvn_settings()
    host = settings.get('host')
    port = settings.get('port', 587)
    username = settings.get('username')
    password = settings.get('password')
    sender_email = settings.get('sender') or st.session_state.sender_email
    use_tls = settings.get('use_tls', True)

    missing = [key for key, value in (
        ('host', host),
        ('port', port),
        ('username', username),
        ('password', password),
        ('sender', sender_email),
    ) if not value and value != 0]

    if missing:
        st.error(
            "KVN SMTP configuration incomplete. Please update the settings for: "
            + ", ".join(missing)
        )
        return False, None

    message = EmailMessage()
    message['Subject'] = subject
    message['From'] = formataddr((st.session_state.sender_name, sender_email))
    message['To'] = recipient
    message['Date'] = formatdate(localtime=True)
    message['Message-ID'] = make_msgid()
    message['List-Unsubscribe'] = f"<{unsubscribe_link}>"
    message['List-Unsubscribe-Post'] = "List-Unsubscribe=One-Click"
    if reply_to:
        message['Reply-To'] = reply_to

    message.set_content(body_text)
    message.add_alternative(body_html, subtype="html")

    target_port = 587
    last_error = None

    if port != target_port:
        logger.info(
            "KVN SMTP forcing configured port %s to %s for delivery.",
            port,
            target_port,
        )

    def _attempt_send(force_tls: bool) -> str:
        """Attempt a single SMTP delivery, optionally forcing STARTTLS."""

        logger.info(
            "Attempting SMTP connection to %s:%s (TLS=%s)",
            host,
            target_port,
            force_tls,
        )

        server = smtplib.SMTP(host, target_port, timeout=30)
        with server:
            server.ehlo()
            supports_starttls = server.has_extn("starttls")
            if force_tls:
                if supports_starttls:
                    server.starttls()
                    server.ehlo()
                else:
                    logger.warning(
                        "SMTP server does not advertise STARTTLS support; continuing without TLS despite request."
                    )

            auth_supported = server.has_extn("auth")
            if auth_supported:
                try:
                    server.login(username, password)
                except smtplib.SMTPNotSupportedError as auth_exc:
                    logger.warning(
                        "SMTP AUTH not supported despite advertisement; proceeding without auth: %s",
                        auth_exc,
                    )
                except smtplib.SMTPResponseException as auth_exc:
                    if (
                        not force_tls
                        and auth_exc.smtp_code == 530
                        and supports_starttls
                    ):
                        logger.warning(
                            "SMTP server demanded STARTTLS before authentication; will retry with TLS."
                        )
                        raise
                    raise
                except Exception:
                    raise
            else:
                if username or password:
                    logger.warning(
                        "SMTP AUTH extension not supported by server; attempting delivery without authentication."
                    )

            try:
                server.send_message(message)
            except smtplib.SMTPResponseException as send_exc:
                if (
                    not force_tls
                    and send_exc.smtp_code == 530
                    and supports_starttls
                ):
                    logger.warning(
                        "SMTP server demanded STARTTLS before sending; will retry with TLS."
                    )
                raise

        logger.info(
            "Successfully sent email via KVN SMTP on %s:%s",
            host,
            target_port,
        )
        return message['Message-ID']

    tls_attempts = [use_tls] if use_tls else [False, True]

    for force_tls in tls_attempts:
        try:
            message_id = _attempt_send(force_tls)
            return True, message_id
        except smtplib.SMTPResponseException as exc:
            last_error = exc
            if (
                exc.smtp_code == 530
                and not force_tls
                and not use_tls
            ):
                # Retry loop will attempt again with TLS enabled.
                continue
            logger.error(
                "Failed to send email via KVN SMTP on %s:%s: %s",
                host,
                target_port,
                exc,
            )
            break
        except Exception as exc:
            last_error = exc
            logger.error(
                "Failed to send email via KVN SMTP on %s:%s: %s",
                host,
                target_port,
                exc,
            )
            break

    error_message = (
        "Failed to send email via KVN SMTP. "
        + (str(last_error) if last_error else "Unknown error occurred.")
    )
    st.error(error_message)
    logger.error("KVN SMTP send failure: %s", error_message)
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
    session_state = _get_session_state()
    username = getattr(session_state, "username", None) if session_state else None
    if username is None and session_state and hasattr(session_state, "get"):
        username = session_state.get("username", None)
    if username is None:
        username = "admin"

    def normalize_results(results_list):
        normalized = []
        for item in results_list:
            if isinstance(item, dict):
                normalized.append(item.get('result', 'error'))
            elif item is None:
                normalized.append('error')
            else:
                normalized.append(item)
        return normalized

    results = []
    total_emails = 0
    current_index = 0
    try:
        df = parse_email_entries(file_content)
        
        if df.empty:
            return pd.DataFrame(
                columns=['name', 'department', 'university', 'country', 'address_lines', 'email', 'verification_result']
            )
        
        # Verify emails
        total_emails = len(df)
        st.session_state.verification_start_time = time.time()

        progress_container = st.container()
        progress_indicator = progress_container.empty()
        progress_bar = progress_container.progress(0 if total_emails else 0)

        start_index = 0
        if resume_data:
            start_index = resume_data.get('current_index', 0)
            prev_results = resume_data.get('results', [])
            for idx, res in enumerate(prev_results):
                if idx < len(df):
                    df.loc[idx, 'verification_result'] = res
                    results.append({'result': res})
            initial_progress = start_index / total_emails if total_emails else 0
            progress_bar.progress(initial_progress)
            st.session_state.verification_progress = initial_progress
        else:
            initial_progress = 0
        current_index = start_index

        render_progress_indicator(
            progress_indicator,
            "Preparing verification...",
            initial_progress,
        )

        batch_size = 200
        for batch_start in range(start_index, total_emails, batch_size):
            batch_end = min(batch_start + batch_size, total_emails)
            for i in range(batch_start, batch_end):
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
                current_index = i + 1
                elapsed_time = time.time() - st.session_state.verification_start_time
                remaining_time = None
                if progress > 0:
                    estimated_total_time = elapsed_time / progress
                    remaining_time = max(0, estimated_total_time - elapsed_time)
                render_progress_indicator(
                    progress_indicator,
                    f"Verifying {i+1} of {total_emails} emails",
                    progress,
                    remaining_time,
                )
                if log_id:
                    update_operation_log(log_id, progress=progress)
                    save_verification_progress(
                        log_id,
                        file_content,
                        normalize_results(results),
                        i + 1,
                        total_emails,
                        username=username,
                    )

                time.sleep(0.1)  # Rate limiting

            gc.collect()

        normalized_results = normalize_results(results)
        df['verification_result'] = normalized_results

        render_progress_indicator(
            progress_indicator,
            "Verification complete",
            1.0 if total_emails else 0,
            0,
        )
        
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
            save_verification_progress(
                log_id,
                file_content,
                normalize_results(results),
                current_index,
                total_emails,
                username=username,
            )
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


def _normalize_to_utc(dt):
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return pytz.utc.localize(dt)
        return dt.astimezone(pytz.utc)
    return None


def _localize_for_display(dt):
    utc_dt = _normalize_to_utc(dt)
    if not utc_dt:
        return None
    try:
        return utc_dt.astimezone(KVN_DISPLAY_TIMEZONE)
    except Exception:
        return utc_dt


def _seconds_until(dt):
    utc_dt = _normalize_to_utc(dt)
    if not utc_dt:
        return None
    remaining = (utc_dt - datetime.now(pytz.utc)).total_seconds()
    return max(0, remaining)


def format_kvn_display_time(dt):
    localized = _localize_for_display(dt)
    if not localized:
        return ""
    return localized.strftime("%d %b %Y %I:%M %p %Z")


def format_display_datetime(dt, fmt="%Y-%m-%d %H:%M:%S %Z"):
    localized = _localize_for_display(dt)
    if not localized:
        return ""
    return localized.strftime(fmt)


def format_kvn_remaining_time(dt):
    remaining_seconds = _seconds_until(dt)
    if remaining_seconds is None:
        return ""
    if remaining_seconds <= 0:
        return "Less than a second left"
    remaining = format_duration(remaining_seconds)
    if remaining.endswith(" 0s"):
        remaining = remaining[:-3]
    return f"{remaining} left"


def get_kvn_schedule_doc():
    db = get_firestore_db()
    if not db:
        return None
    return db.collection(KVN_SCHEDULE_COLLECTION).document(KVN_SCHEDULE_DOCUMENT)


def get_kvn_last_send_time(force_refresh=False):
    session_state = _get_session_state()
    if (
        not force_refresh
        and session_state is not None
        and 'kvn_last_send_time' in session_state
        and session_state.kvn_last_send_time is not None
    ):
        return session_state.kvn_last_send_time

    try:
        doc_ref = get_kvn_schedule_doc()
        if not doc_ref:
            return None
        snapshot = doc_ref.get()
        if snapshot.exists:
            data = snapshot.to_dict()
            last_send = data.get('last_send_time') if data else None
            normalized = _normalize_to_utc(last_send)
            if session_state is not None:
                session_state.kvn_last_send_time = normalized
            return normalized
    except Exception as exc:
        st.error(f"Failed to load KVN send schedule: {exc}")
    return None


def record_kvn_send_completion(timestamp=None):
    ts = timestamp or datetime.now(pytz.utc)
    ts_utc = _normalize_to_utc(ts)
    if not ts_utc:
        ts_utc = datetime.now(pytz.utc)
    try:
        doc_ref = get_kvn_schedule_doc()
        if not doc_ref:
            return
        stored_ts = ts_utc.replace(tzinfo=None)
        updated_at = datetime.utcnow()
        doc_ref.set(
            {
                'last_send_time': stored_ts,
                'updated_at': updated_at,
            },
            merge=True,
        )
        session_state = _get_session_state()
        if session_state is not None:
            session_state.kvn_last_send_time = ts_utc
    except Exception as exc:
        st.error(f"Failed to update KVN send schedule: {exc}")


def get_kvn_send_availability():
    last_send = get_kvn_last_send_time()
    if not last_send:
        return True, None, None
    next_slot = last_send + KVN_SLOT_BUFFER
    now_utc = datetime.now(pytz.utc)
    if now_utc >= next_slot:
        return True, last_send, next_slot
    return False, last_send, next_slot


def prepare_kvn_recipient_batches(df, source_filename):
    if df is None:
        return df, source_filename, [], 0

    total = len(df)
    registry = st.session_state.setdefault('kvn_split_registry', {})

    if total == 0:
        summary = {
            'source_file': source_filename,
            'metadata': [],
            'original_total': 0,
            'hash': None,
        }
        st.session_state.kvn_split_summary = summary
        st.session_state.kvn_active_chunk_index = 0
        st.session_state.kvn_active_chunk_name = source_filename
        return df, source_filename, [], 0

    csv_content = df.to_csv(index=False)
    file_hash = hashlib.md5(csv_content.encode('utf-8')).hexdigest()
    cached = registry.get(file_hash)

    if cached:
        metadata = copy.deepcopy(cached['metadata'])
        first_chunk_df = cached['first_chunk'].copy()
        effective_name = metadata[0]['name'] if metadata else source_filename
    else:
        chunk_size = KVN_MAX_EMAILS_PER_BATCH
        chunk_count = max(1, math.ceil(total / chunk_size))
        base_name = Path(source_filename or f"recipients_{int(time.time())}").stem
        metadata = []
        chunk_dfs = []

        if chunk_count == 1:
            effective_name = source_filename or f"{base_name}.csv"
            chunk_df = df.copy().reset_index(drop=True)
            upload_to_firebase(chunk_df.to_csv(index=False), effective_name)
            metadata.append({'name': effective_name, 'count': total})
            chunk_dfs.append(chunk_df)
            existing_files = set(st.session_state.get('firebase_files', []))
            existing_files.add(effective_name)
            st.session_state.firebase_files = sorted(existing_files)
        else:
            for idx, start in enumerate(range(0, total, chunk_size), start=1):
                chunk_df = df.iloc[start:start + chunk_size].copy().reset_index(drop=True)
                chunk_name = f"{base_name}_kvn_part{idx:02d}_of_{chunk_count}.csv"
                upload_to_firebase(chunk_df.to_csv(index=False), chunk_name)
                metadata.append({'name': chunk_name, 'count': len(chunk_df)})
                chunk_dfs.append(chunk_df)
            effective_name = metadata[0]['name']
            existing_files = set(st.session_state.get('firebase_files', []))
            existing_files.update(item['name'] for item in metadata)
            st.session_state.firebase_files = sorted(existing_files)

        first_chunk_df = chunk_dfs[0]
        registry[file_hash] = {
            'metadata': metadata,
            'first_chunk': first_chunk_df,
        }
        st.session_state.kvn_split_registry = registry

    summary = {
        'source_file': source_filename,
        'metadata': metadata,
        'original_total': total,
        'hash': file_hash,
    }
    st.session_state.kvn_split_summary = summary
    st.session_state.kvn_active_chunk_index = 0
    active_name = metadata[0]['name'] if metadata else source_filename
    st.session_state.kvn_active_chunk_name = active_name

    return first_chunk_df.copy(), active_name, metadata, total


def render_kvn_batch_summary(metadata, original_total):
    if not metadata:
        return
    if len(metadata) == 1:
        st.info(
            f"KVN SMTP batch prepared with {metadata[0]['count']} recipient(s). "
            f"Quota allows up to {KVN_MAX_EMAILS_PER_BATCH} emails per hour.")
        return

    lines = []
    for idx, item in enumerate(metadata, start=1):
        lines.append(f"Batch {idx}: {item['count']} recipients ({item['name']})")
    details = "\n".join(lines)
    st.info(
        f"KVN SMTP quota split {original_total} recipients into {len(metadata)} batches of up to "
        f"{KVN_MAX_EMAILS_PER_BATCH} emails. Files saved to Cloud Storage:\n{details}")

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
def save_verification_progress(
    log_id,
    file_content,
    results,
    current_index,
    total_emails,
    username="admin",
):
    """Save intermediate verification progress for resuming later."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        safe_username = username or "admin"
        doc_ref = db.collection("verification_progress").document(log_id)
        doc_ref.set({
            "user": safe_username,
            "file_content": file_content,
            "results": results,
            "current_index": current_index,
            "total_emails": total_emails,
            "last_updated": datetime.now(),
        })
        return True
    except Exception as e:
        try:
            st.error(f"Failed to save verification progress: {str(e)}")
        except RuntimeError:
            logger.error("Failed to save verification progress: %s", e)
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


def save_sender_email(sender_email):
    """Persist the default sender email address to Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("settings").document("sender_email")
        doc_ref.set({
            "value": sender_email,
            "last_updated": datetime.now(),
            "updated_by": st.session_state.get("username", "admin"),
        })
        return True
    except Exception as e:
        st.error(f"Failed to save sender email: {str(e)}")
        return False


def load_sender_email():
    """Retrieve the default sender email address from Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc_ref = db.collection("settings").document("sender_email")
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict().get("value", None)
        return None
    except Exception as e:
        st.error(f"Failed to load sender email: {str(e)}")
        return None


def save_reply_address(journal_name, address):
    """Save reply-to address for a journal to Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("reply_addresses").document(journal_name)
        doc_ref.set({
            "email": address,
            "updated_at": datetime.now(),
            "updated_by": st.session_state.get("username", "admin"),
        })
        st.session_state.journal_reply_addresses[journal_name] = address
        return True
    except Exception as e:
        st.error(f"Failed to save reply-to address: {str(e)}")
        return False


def save_default_reply_address(address):
    """Persist a global reply-to email used when a journal-specific one is absent."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        doc_ref = db.collection("settings").document("default_reply_to")
        doc_ref.set({
            "value": address,
            "updated_at": datetime.now(),
            "updated_by": st.session_state.get("username", "admin"),
        })
        st.session_state.default_reply_to = address or ""
        return True
    except Exception as e:
        st.error(f"Failed to save default reply-to address: {str(e)}")
        return False


def load_reply_addresses():
    """Load all reply-to addresses from Firestore into session state."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        docs = db.collection("reply_addresses").stream()
        for doc in docs:
            data = doc.to_dict() or {}
            st.session_state.journal_reply_addresses[doc.id] = data.get("email", "")
        load_default_reply_address()
        return True
    except Exception as e:
        st.error(f"Failed to load reply-to addresses: {str(e)}")
        return False


def load_default_reply_address():
    """Fetch the default reply-to email from Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc = db.collection("settings").document("default_reply_to").get()
        if doc.exists:
            value = (doc.to_dict() or {}).get("value", "")
            st.session_state.default_reply_to = value or ""
            return value
        return None
    except Exception as e:
        st.error(f"Failed to load default reply-to address: {str(e)}")
        return None


def get_reply_to_for_journal(journal_name):
    """Return the reply-to email for a journal or fall back to the default."""
    if journal_name:
        reply = st.session_state.journal_reply_addresses.get(journal_name)
        if reply:
            return reply
    default_reply = st.session_state.get("default_reply_to", "")
    return default_reply or None


def save_default_email_service(service_name):
    """Persist the preferred email service so the choice sticks across sessions."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        normalized = (service_name or "MAILGUN").upper()
        doc_ref = db.collection("settings").document("default_email_service")
        doc_ref.set({
            "value": normalized,
            "last_updated": datetime.now(),
            "updated_by": st.session_state.get("username", "admin"),
        })
        return True
    except Exception as e:
        st.error(f"Failed to save default email service: {str(e)}")
        return False


def load_default_email_service():
    """Return the persisted default email service if available."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc_ref = db.collection("settings").document("default_email_service")
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict().get("value", None)
        return None
    except Exception as e:
        st.error(f"Failed to load default email service: {str(e)}")
        return None


def get_effective_kvn_settings():
    """Merge environment defaults with any saved KVN SMTP overrides."""
    base = copy.deepcopy(config.get('kvn_smtp', {}))
    overrides = st.session_state.get('kvn_smtp_settings', {}) or {}
    for key, value in overrides.items():
        if value not in (None, ""):
            base[key] = value
    # Ensure port is always an integer
    if 'port' in base:
        try:
            base['port'] = int(base['port'])
        except (TypeError, ValueError):
            base['port'] = 587
    else:
        base['port'] = 587
    base['use_tls'] = bool(base.get('use_tls', True))
    return base


def save_kvn_smtp_settings(settings):
    """Persist KVN SMTP credentials/settings to Firestore."""
    try:
        db = get_firestore_db()
        if not db:
            return False

        sanitized = {
            'host': settings.get('host', ''),
            'port': int(settings.get('port', 587) or 587),
            'username': settings.get('username', ''),
            'password': settings.get('password', ''),
            'sender': settings.get('sender', ''),
            'use_tls': bool(settings.get('use_tls', True)),
            'updated_at': datetime.now(),
            'updated_by': st.session_state.get('username', 'admin'),
        }

        db.collection('settings').document('kvn_smtp').set(sanitized)
        st.session_state.kvn_smtp_settings = sanitized
        config['kvn_smtp'].update({
            'host': sanitized['host'],
            'port': sanitized['port'],
            'username': sanitized['username'],
            'password': sanitized['password'],
            'sender': sanitized['sender'],
            'use_tls': sanitized['use_tls'],
        })
        return True
    except Exception as e:
        st.error(f"Failed to save KVN SMTP settings: {str(e)}")
        return False


def load_kvn_smtp_settings():
    """Load KVN SMTP settings from Firestore into session state."""
    try:
        db = get_firestore_db()
        if not db:
            return None

        doc_ref = db.collection('settings').document('kvn_smtp')
        doc = doc_ref.get()
        if doc.exists:
            data = doc.to_dict() or {}
            st.session_state.kvn_smtp_settings = data
            config['kvn_smtp'].update({
                'host': data.get('host', ''),
                'port': int(data.get('port', config['kvn_smtp'].get('port', 587) or 587)),
                'username': data.get('username', ''),
                'password': data.get('password', ''),
                'sender': data.get('sender', ''),
                'use_tls': bool(data.get('use_tls', True)),
            })
            return data
        return None
    except Exception as e:
        st.error(f"Failed to load KVN SMTP settings: {str(e)}")
        return None


def is_kvn_smtp_configured():
    """Quick helper to verify KVN SMTP settings are complete."""
    settings = get_effective_kvn_settings()
    required_fields = ['host', 'port', 'username', 'password', 'sender']
    return all(settings.get(field) for field in required_fields)


def get_service_display_name(service_key):
    mapping = {
        'SMTP2GO': 'SMTP2GO',
        'MAILGUN': 'Mailgun',
        'KVN SMTP': 'KVN SMTP',
        'KVN': 'KVN SMTP',
    }
    normalized = (service_key or 'MAILGUN').upper()
    return mapping.get(normalized, normalized.title())


def normalize_service_key(service_key):
    normalized = (service_key or 'MAILGUN').upper()
    if normalized == 'KVN':
        return 'KVN SMTP'
    return normalized

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
        update_payload = {
            "current_index": current_index,
            "emails_sent": emails_sent,
            "last_updated": datetime.now(),
        }
        doc_ref.set(update_payload, merge=True)
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

    service_key = (
        campaign_data.get("email_service")
        or st.session_state.email_service
        or "MAILGUN"
    )
    service = normalize_service_key(service_key)
    st.session_state.email_service = service
    service_display = get_service_display_name(service)

    progress_container = st.container()
    progress_indicator = progress_container.empty()
    initial_progress = current_index / total_emails if total_emails else 0
    progress_bar = progress_container.progress(initial_progress)
    render_progress_indicator(
        progress_indicator,
        f"{service_display} · Preparing campaign",
        initial_progress,
    )
    start_time = time.time()
    cancel_button = st.button("Cancel Campaign")

    reply_to = get_reply_to_for_journal(journal)
    email_ids = []
    last_email_sent_at = None

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
            elapsed = time.time() - start_time
            remaining = None
            if progress > 0:
                remaining = max(0, (elapsed / progress) - elapsed)
            render_progress_indicator(
                progress_indicator,
                f"{service_display} · Skipping {i+1} of {total_emails}: {recipient_email} (unsubscribed)",
                progress,
                remaining,
            )
            update_campaign_progress(campaign_id, i + 1, success_count)
            if log_id:
                update_operation_log(log_id, progress=progress)
            continue
        if is_email_blocked(recipient_email):
            progress = (i + 1) / total_emails
            progress_bar.progress(progress)
            elapsed = time.time() - start_time
            remaining = None
            if progress > 0:
                remaining = max(0, (elapsed / progress) - elapsed)
            render_progress_indicator(
                progress_indicator,
                f"{service_display} · Skipping {i+1} of {total_emails}: {recipient_email} (blocked)",
                progress,
                remaining,
            )
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

        if service == "SMTP2GO":
            success, email_id = send_email_via_smtp2go(
                recipient_email,
                subject,
                email_content,
                plain_text,
                unsubscribe_link,
                reply_to,
            )
        elif service == "KVN SMTP":
            success, email_id = send_email_via_kvn(
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
            last_email_sent_at = datetime.now(pytz.utc)

        progress = (i + 1) / total_emails
        progress_bar.progress(progress)
        elapsed = time.time() - start_time
        remaining = None
        if progress > 0:
            remaining = max(0, (elapsed / progress) - elapsed)
        render_progress_indicator(
            progress_indicator,
            f"{service_display} · Sending {i+1} of {total_emails}: {recipient_email}",
            progress,
            remaining,
        )
        update_campaign_progress(campaign_id, i + 1, success_count)
        if log_id:
            update_operation_log(log_id, progress=progress)

        st.session_state.active_campaign['current_index'] = i + 1
        st.session_state.active_campaign['emails_sent'] = success_count

        if cancel_button:
            st.session_state.campaign_cancelled = True
            st.warning("Campaign cancellation requested...")
            break

    if not st.session_state.campaign_cancelled:
        campaign_data = {
            'status': 'completed',
            'completed_at': datetime.now(),
            'emails_sent': success_count,
        }
        db = get_firestore_db()
        if db:
            doc_ref = db.collection("active_campaigns").document(str(campaign_id))
            doc_ref.set(campaign_data, merge=True)

        record = {
            'timestamp': datetime.now(),
            'journal': journal,
            'emails_sent': success_count,
            'total_emails': total_emails,
            'subject': ','.join(selected_subjects) if selected_subjects else email_subject,
            'email_ids': ','.join(email_ids),
            'service': service,
        }
        if service == "KVN SMTP" and last_email_sent_at:
            normalized_last = _normalize_to_utc(last_email_sent_at)
            next_slot = normalized_last + KVN_SLOT_BUFFER
            record['kvn_last_email_sent_at'] = normalized_last.replace(tzinfo=None)
            record['kvn_next_available_slot'] = next_slot.replace(tzinfo=None)
            record_kvn_send_completion(last_email_sent_at)
        elif service == "KVN SMTP":
            record_kvn_send_completion()
        st.session_state.campaign_history.append(record)
        save_campaign_history(record)

        progress_bar.progress(1.0)
        render_progress_indicator(
            progress_indicator,
            f"{service_display} · Campaign completed",
            1.0,
            0,
        )
        st.success(
            f"Campaign completed via {service_display}! {success_count} of {total_emails} emails sent successfully."
        )
        if log_id:
            update_operation_log(log_id, status="completed", progress=1.0)
        delete_campaign(campaign_id)
    else:
        st.warning(
            f"Campaign cancelled while using {service_display}. {success_count} of {total_emails} emails were sent."
        )
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
                    stored_service = campaign.get('email_service')
                    if stored_service:
                        st.session_state.email_service = normalize_service_key(stored_service)
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
                st.sidebar.markdown("<span></span></div>", unsafe_allow_html=True)
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
                    stored_service = campaign.get('email_service')
                    if stored_service:
                        st.session_state.email_service = normalize_service_key(stored_service)
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
                cols[idx].markdown("<span></span></div>", unsafe_allow_html=True)
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
    ensure_session_defaults({
        'active_campaign': lambda: None,
        'campaign_paused': lambda: False,
        'campaign_cancelled': lambda: False,
        'current_recipient_list': lambda: None,
        'current_recipient_file': lambda: None,
        'block_settings_loaded': lambda: False,
        'sender_name_loaded': lambda: False,
        'journals_loaded': lambda: False,
        'editor_journals_loaded': lambda: False,
        'show_journal_details': lambda: False,
        'journal_subjects': dict,
        'template_content': dict,
        'spam_check_cache': dict,
        'template_spam_score': dict,
        'template_spam_report': dict,
        'template_spam_summary': dict,
        'journal_reply_addresses': dict,
        'default_reply_to': lambda: "",
        'firebase_files': list,
        'selected_journal': lambda: JOURNALS[0] if JOURNALS else None,
    })

    st.header("Email Campaign Management")
    display_pending_operations("campaign")

    if st.session_state.active_campaign and not st.session_state.campaign_paused:
        ac = st.session_state.active_campaign
        if ac.get('current_index', 0) < ac.get('total_emails', 0):
            st.info(f"Resuming campaign {ac.get('campaign_id')} - {ac.get('current_index')}/{ac.get('total_emails')}")
            if 'current_recipient_list' not in st.session_state or st.session_state.current_recipient_list is None:
                st.session_state.current_recipient_list = pd.DataFrame(ac.get('recipient_list', []))
            stored_service = ac.get('email_service')
            if stored_service:
                st.session_state.email_service = normalize_service_key(stored_service)
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

    selection_col, delivery_col = st.columns([3, 2], gap="large")

    with selection_col:
        st.markdown("<div class='modern-card journal-card'>", unsafe_allow_html=True)
        st.markdown("#### Journal Selection")
        selected_journal = st.selectbox(
            "Select Journal",
            JOURNALS,
            index=JOURNALS.index(st.session_state.selected_journal)
            if st.session_state.selected_journal in JOURNALS else 0,
        )
        if selected_journal != st.session_state.selected_journal:
            st.session_state.selected_journal = selected_journal
            update_sender_name()
        toggle_label = "Hide Subjects & Template" if st.session_state.show_journal_details else "Load Subjects & Template"
        if st.button(toggle_label, key="toggle_journal_details"):
            st.session_state.show_journal_details = not st.session_state.show_journal_details
            if st.session_state.show_journal_details:
                refresh_journal_data()
            st.experimental_rerun()
        st.markdown("<span></span></div>", unsafe_allow_html=True)

    with delivery_col:
        st.markdown("<div class='modern-card'>", unsafe_allow_html=True)
        st.markdown("#### Delivery Controls")
        col_service, col_status, col_settings = st.columns([2, 1.5, 1])

        service_options = ["SMTP2GO", "MAILGUN", "KVN SMTP"]
        current_service = (st.session_state.email_service or "MAILGUN").upper()
        if current_service not in service_options:
            current_service = "MAILGUN"

        with col_service:
            st.markdown("<div class='compact-select'>", unsafe_allow_html=True)
            selected_service = st.selectbox(
                "Active Email Service",
                service_options,
                index=service_options.index(current_service),
                key="campaign_service_select",
            )
            st.markdown("<span></span></div>", unsafe_allow_html=True)
            st.session_state.email_service = selected_service

        with col_status:
            display_name = get_service_display_name(selected_service)
            st.markdown(
                f"<span class='status-badge'>Active: {display_name}</span>",
                unsafe_allow_html=True,
            )
            if selected_service == "KVN SMTP" and not is_kvn_smtp_configured():
                st.warning("KVN SMTP needs configuration in Settings.", icon="⚠️")

        with col_settings:
            if st.button("Open Settings", key="open_settings_from_campaign"):
                st.session_state.requested_mode = "Settings"
                st.experimental_rerun()

        reply_to_value = get_reply_to_for_journal(selected_journal)
        default_reply = st.session_state.get("default_reply_to", "")
        if reply_to_value:
            escaped_reply = html.escape(reply_to_value)
            st.caption(f"Reply-to for {selected_journal}: {escaped_reply}")
        elif default_reply:
            escaped_default = html.escape(default_reply)
            st.caption(f"Reply-to for {selected_journal}: inherits default {escaped_default}")
        else:
            st.caption(f"Reply-to for {selected_journal}: Not configured")
        st.caption("Manage sender identity, reply-to addresses, and suppression lists from the Settings tab.")
        st.markdown("<span></span></div>", unsafe_allow_html=True)

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

    recipients_container = st.container()

    with recipients_container:
        col_src, col_clock = st.columns([1, 2])
        with col_src:
            file_source = st.radio(
                "Select file source",
                ["Local Upload", "Cloud Storage"],
                key="recipient_file_source",
            )
        with col_clock:
            st.markdown(
                f"<div class='highlight-panel'><strong>Active Service:</strong> {get_service_display_name(st.session_state.email_service)}</div>",
                unsafe_allow_html=True,
            )
        unsubscribed_lookup = st.session_state.get("unsubscribed_email_lookup", set())

        if file_source == "Local Upload":
            uploaded_file = st.file_uploader(
                "Upload recipient list (CSV or TXT)",
                type=["csv", "txt"],
                key="recipient_file_uploader",
            )
            if uploaded_file:
                original_filename = uploaded_file.name
                st.session_state.current_recipient_file = original_filename
                if uploaded_file.name.endswith('.txt'):
                    file_content = read_uploaded_text(uploaded_file)
                    df = parse_email_entries(file_content)
                else:
                    df = pd.read_csv(uploaded_file)

                service_key = normalize_service_key(st.session_state.email_service)
                kvn_metadata = []
                original_total = len(df)
                if service_key == "KVN SMTP":
                    df, effective_name, kvn_metadata, original_total = prepare_kvn_recipient_batches(df, original_filename)
                    st.session_state.current_recipient_file = effective_name

                st.session_state.current_recipient_list = df
                st.dataframe(df.head())
                if service_key == "KVN SMTP":
                    st.info(f"KVN SMTP batch ready with {len(df)} recipient(s).")
                    render_kvn_batch_summary(kvn_metadata, original_total)
                else:
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

                        service_key = normalize_service_key(st.session_state.email_service)
                        kvn_metadata = []
                        original_total = len(df)
                        if service_key == "KVN SMTP":
                            df, effective_name, kvn_metadata, original_total = prepare_kvn_recipient_batches(df, selected_file)
                            st.session_state.current_recipient_file = effective_name

                        st.session_state.current_recipient_list = df
                        st.dataframe(df.head())
                        if service_key == "KVN SMTP":
                            st.info(f"KVN SMTP batch ready with {len(df)} recipient(s).")
                            render_kvn_batch_summary(kvn_metadata, original_total)
                        else:
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
        service = normalize_service_key(st.session_state.email_service)
        kvn_button_disabled = False
        if service == "KVN SMTP":
            can_send_now, kvn_last_sent, kvn_next_slot = get_kvn_send_availability()
            if not can_send_now:
                kvn_button_disabled = True
                if kvn_next_slot:
                    remaining_text = format_kvn_remaining_time(kvn_next_slot)
                    kvn_message = (
                        f"KVN SMTP quota reached. Next batch available at {format_kvn_display_time(kvn_next_slot)}."
                    )
                    if remaining_text:
                        kvn_message += f" ({remaining_text})"
                    st.warning(kvn_message)
            elif kvn_last_sent:
                st.info(
                    f"Last KVN SMTP batch completed at {format_kvn_display_time(kvn_last_sent)}. Quota reset available.")

        send_ads_clicked = st.button("Send Ads", key="send_ads", disabled=kvn_button_disabled)
        st.markdown("<span></span></div>", unsafe_allow_html=True)
        if send_ads_clicked:
            if service == "KVN SMTP":
                can_send_now, _, _ = get_kvn_send_availability()
                if not can_send_now:
                    st.warning("KVN SMTP quota cooldown in effect. Please wait for the next available slot.")
                    return
            if file_source == "Local Upload" and uploaded_file:
                if uploaded_file.name.endswith('.txt'):
                    upload_to_firebase(file_content, uploaded_file.name)
                else:
                    csv_content = df.to_csv(index=False)
                    upload_to_firebase(csv_content, uploaded_file.name)
            if service == "SMTP2GO":
                if not config['smtp2go']['api_key']:
                    st.error("SMTP2GO API key not configured")
                    return
            elif service == "KVN SMTP":
                if not is_kvn_smtp_configured():
                    st.error("Please configure KVN SMTP credentials in Settings before sending.")
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
            if df is None:
                st.error("No recipient list loaded. Please upload or select recipients before sending.")
                return
            if not isinstance(df, pd.DataFrame):
                try:
                    df = pd.DataFrame(df)
                except Exception:
                    st.error("Unable to process the recipient list. Please reload your recipients and try again.")
                    return
                st.session_state.current_recipient_list = df

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
                'reply_to': get_reply_to_for_journal(selected_journal),
                'recipient_list': df.to_dict('records'),
                'total_emails': total_emails,
                'current_index': 0,
                'emails_sent': 0,
                'status': 'active',
                'created_at': datetime.now(),
                'last_updated': datetime.now()
            }

            if service == "KVN SMTP":
                campaign_data['kvn_split_summary'] = st.session_state.get('kvn_split_summary')
                campaign_data['kvn_batch_index'] = st.session_state.get('kvn_active_chunk_index', 0)

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
    ensure_session_defaults({
        'active_campaign': lambda: None,
        'campaign_paused': lambda: False,
        'campaign_cancelled': lambda: False,
        'block_settings_loaded': lambda: False,
        'reply_addresses_loaded': lambda: False,
        'sender_name_loaded': lambda: False,
        'sender_base_name': lambda: st.session_state.sender_name,
        'editor_journals_loaded': lambda: False,
        'selected_editor_journal': lambda: EDITOR_JOURNALS[0] if EDITOR_JOURNALS else None,
        'editor_show_journal_details': lambda: False,
        'journal_subjects': dict,
    })

    st.header("Editor Invitation")
    display_pending_operations("campaign")

    if st.session_state.active_campaign and not st.session_state.campaign_paused:
        ac = st.session_state.active_campaign
        if ac.get('current_index', 0) < ac.get('total_emails', 0):
            st.info(f"Resuming campaign {ac.get('campaign_id')} - {ac.get('current_index')}/{ac.get('total_emails')}")

    if not st.session_state.block_settings_loaded:
        load_block_settings()
        st.session_state.block_settings_loaded = True

    if not st.session_state.reply_addresses_loaded:
        load_reply_addresses()
        st.session_state.reply_addresses_loaded = True

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

    selection_col, delivery_col = st.columns([3, 2], gap="large")

    with selection_col:
        st.markdown("<div class='modern-card journal-card'>", unsafe_allow_html=True)
        st.markdown("#### Journal Selection")
        selected_editor_journal = st.selectbox(
            "Select Journal",
            EDITOR_JOURNALS,
            index=EDITOR_JOURNALS.index(st.session_state.selected_editor_journal)
            if st.session_state.selected_editor_journal in EDITOR_JOURNALS else 0,
            on_change=update_editor_sender_name,
            key="selected_editor_journal",
        )
        toggle_label = "Hide Subjects & Template" if st.session_state.editor_show_journal_details else "Load Subjects & Template"
        if st.button(toggle_label, key="toggle_editor_journal_details"):
            st.session_state.editor_show_journal_details = not st.session_state.editor_show_journal_details
            if st.session_state.editor_show_journal_details:
                refresh_editor_journal_data()
            st.experimental_rerun()
        st.markdown("<span></span></div>", unsafe_allow_html=True)

    with delivery_col:
        st.markdown("<div class='modern-card'>", unsafe_allow_html=True)
        st.markdown("#### Delivery Controls")
        col_service, col_status, col_settings = st.columns([2, 1.5, 1])

        service_options = ["SMTP2GO", "MAILGUN", "KVN SMTP"]
        current_service = (st.session_state.email_service or "MAILGUN").upper()
        if current_service not in service_options:
            current_service = "MAILGUN"

        with col_service:
            st.markdown("<div class='compact-select'>", unsafe_allow_html=True)
            selected_service = st.selectbox(
                "Active Email Service",
                service_options,
                index=service_options.index(current_service),
                key="editor_service_select",
            )
            st.markdown("<span></span></div>", unsafe_allow_html=True)
            st.session_state.email_service = selected_service

        with col_status:
            display_name = get_service_display_name(selected_service)
            st.markdown(
                f"<span class='status-badge'>Active: {display_name}</span>",
                unsafe_allow_html=True,
            )
            if selected_service == "KVN SMTP" and not is_kvn_smtp_configured():
                st.warning("KVN SMTP needs configuration in Settings.", icon="⚠️")

        with col_settings:
            if st.button("Open Settings", key="open_settings_from_editor"):
                st.session_state.requested_mode = "Settings"
                st.experimental_rerun()

        reply_to_value = get_reply_to_for_journal(selected_editor_journal)
        default_reply = st.session_state.get("default_reply_to", "")
        if reply_to_value:
            escaped_reply = html.escape(reply_to_value)
            st.caption(f"Reply-to for {selected_editor_journal}: {escaped_reply}")
        elif default_reply:
            escaped_default = html.escape(default_reply)
            st.caption(f"Reply-to for {selected_editor_journal}: inherits default {escaped_default}")
        else:
            st.caption(f"Reply-to for {selected_editor_journal}: Not configured")
        st.caption("Manage global sender settings from the Settings tab.")
        st.markdown("<span></span></div>", unsafe_allow_html=True)


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
        st.markdown(
            f"<div class='highlight-panel'><strong>Active Service:</strong> {get_service_display_name(st.session_state.email_service)}</div>",
            unsafe_allow_html=True,
        )

    if file_source == "Local Upload":
        uploaded_file = st.file_uploader("Upload recipient list (CSV or TXT)", type=["csv", "txt"], key="recipient_upload_editor")
        if uploaded_file:
            original_filename = uploaded_file.name
            st.session_state.current_recipient_file = original_filename
            if uploaded_file.name.endswith('.txt'):
                file_content = read_uploaded_text(uploaded_file)
                df = parse_email_entries(file_content)
            else:
                df = pd.read_csv(uploaded_file)

            service_key = normalize_service_key(st.session_state.email_service)
            kvn_metadata = []
            original_total = len(df)
            if service_key == "KVN SMTP":
                df, effective_name, kvn_metadata, original_total = prepare_kvn_recipient_batches(df, original_filename)
                st.session_state.current_recipient_file = effective_name

            st.session_state.current_recipient_list = df
            st.dataframe(df.head())
            if service_key == "KVN SMTP":
                st.info(f"KVN SMTP batch ready with {len(df)} recipient(s).")
                render_kvn_batch_summary(kvn_metadata, original_total)
            else:
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

                    service_key = normalize_service_key(st.session_state.email_service)
                    kvn_metadata = []
                    original_total = len(df)
                    if service_key == "KVN SMTP":
                        df, effective_name, kvn_metadata, original_total = prepare_kvn_recipient_batches(df, selected_file)
                        st.session_state.current_recipient_file = effective_name

                    st.session_state.current_recipient_list = df
                    st.dataframe(df.head())
                    if service_key == "KVN SMTP":
                        st.info(f"KVN SMTP batch ready with {len(df)} recipient(s).")
                        render_kvn_batch_summary(kvn_metadata, original_total)
                    else:
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
        service = normalize_service_key(st.session_state.email_service)
        kvn_button_disabled = False
        if service == "KVN SMTP":
            can_send_now, kvn_last_sent, kvn_next_slot = get_kvn_send_availability()
            if not can_send_now:
                kvn_button_disabled = True
                if kvn_next_slot:
                    remaining_text = format_kvn_remaining_time(kvn_next_slot)
                    kvn_message = (
                        f"KVN SMTP quota reached. Next batch available at {format_kvn_display_time(kvn_next_slot)}."
                    )
                    if remaining_text:
                        kvn_message += f" ({remaining_text})"
                    st.warning(kvn_message)
            elif kvn_last_sent:
                st.info(
                    f"Last KVN SMTP batch completed at {format_kvn_display_time(kvn_last_sent)}. Quota reset available.")

        send_invitation_clicked = st.button("Send Invitation", key="send_invitation", disabled=kvn_button_disabled)
        st.markdown("<span></span></div>", unsafe_allow_html=True)
        if send_invitation_clicked:
            if service == "KVN SMTP":
                can_send_now, _, _ = get_kvn_send_availability()
                if not can_send_now:
                    st.warning("KVN SMTP quota cooldown in effect. Please wait for the next available slot.")
                    return
            if file_source == "Local Upload" and uploaded_file:
                if uploaded_file.name.endswith('.txt'):
                    upload_to_firebase(file_content, uploaded_file.name)
                else:
                    csv_content = df.to_csv(index=False)
                    upload_to_firebase(csv_content, uploaded_file.name)
            if service == "SMTP2GO":
                if not config['smtp2go']['api_key']:
                    st.error("SMTP2GO API key not configured")
                    return
            elif service == "KVN SMTP":
                if not is_kvn_smtp_configured():
                    st.error("Please configure KVN SMTP credentials in Settings before sending.")
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
            if df is None:
                st.error("No recipient list loaded. Please upload or select recipients before sending.")
                return
            if not isinstance(df, pd.DataFrame):
                try:
                    df = pd.DataFrame(df)
                except Exception:
                    st.error("Unable to process the recipient list. Please reload your recipients and try again.")
                    return
                st.session_state.current_recipient_list = df

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
                'reply_to': get_reply_to_for_journal(selected_editor_journal),
                'recipient_list': df.to_dict('records'),
                'total_emails': total_emails,
                'current_index': 0,
                'emails_sent': 0,
                'status': 'active',
                'created_at': datetime.now(),
                'last_updated': datetime.now()
            }

            if service == "KVN SMTP":
                campaign_data['kvn_split_summary'] = st.session_state.get('kvn_split_summary')
                campaign_data['kvn_batch_index'] = st.session_state.get('kvn_active_chunk_index', 0)

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
    ensure_session_defaults({
        'verification_resume_data': lambda: None,
        'verification_resume_log_id': lambda: None,
        'verified_emails': lambda: pd.DataFrame(),
        'verification_stats': dict,
        'verification_start_time': lambda: None,
        'verification_progress': lambda: 0,
        'auto_download_good': lambda: False,
        'auto_download_low_risk': lambda: False,
        'auto_download_high_risk': lambda: False,
        'good_download_content': lambda: "",
        'good_download_file_name': lambda: None,
        'low_risk_download_content': lambda: "",
        'low_risk_download_file_name': lambda: None,
        'high_risk_download_content': lambda: "",
        'high_risk_download_file_name': lambda: None,
        'current_verification_file': lambda: None,
        'current_verification_list': lambda: None,
        'firebase_files_verification': list,
    })

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

        # Lightweight bar chart for verification summary
        categories = ['Good', 'Bad', 'Risky', 'Low Risk']
        counts = [stats['good'], stats['bad'], stats['risky'], stats['low_risk']]
        chart_df = pd.DataFrame({'Category': categories, 'Count': counts}).set_index('Category')
        st.bar_chart(chart_df)
        
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

def settings_section():
    ensure_session_defaults({
        'block_settings_loaded': lambda: False,
        'journals_loaded': lambda: False,
        'editor_journals_loaded': lambda: False,
        'reply_addresses_loaded': lambda: False,
        'sender_base_name': lambda: st.session_state.sender_name,
        'journal_reply_addresses': dict,
        'default_reply_to': lambda: "",
        'blocked_domains': list,
        'blocked_emails': list,
    })

    st.header("Settings & Preferences")
    st.caption("All changes are securely saved to Firestore so they persist across sessions.")

    if not st.session_state.block_settings_loaded:
        load_block_settings()
        st.session_state.block_settings_loaded = True
    if not st.session_state.journals_loaded:
        load_journals_from_firebase()
        st.session_state.journals_loaded = True
    if not st.session_state.editor_journals_loaded:
        load_editor_journals_from_firebase()
        st.session_state.editor_journals_loaded = True
    if not st.session_state.reply_addresses_loaded:
        load_reply_addresses()
        st.session_state.reply_addresses_loaded = True

    general_tab, services_tab, compliance_tab = st.tabs([
        "General",
        "Email Services",
        "Compliance & Safety",
    ])

    with general_tab:
        top_left, top_right = st.columns([1, 1], gap="large")

        with top_left:
            st.markdown("<div class='modern-card journal-card'>", unsafe_allow_html=True)
            st.subheader("Sender Identity")
            with st.form("sender_identity_form"):
                col1, col2 = st.columns(2)
                with col1:
                    sender_name_input = st.text_input(
                        "Sender display name",
                        st.session_state.sender_base_name,
                        key="settings_sender_name_input",
                    )
                with col2:
                    sender_email_input = st.text_input(
                        "Default sender email",
                        st.session_state.sender_email,
                        key="settings_sender_email_input",
                    )
                submitted_identity = st.form_submit_button("Save Sender Identity")

            if submitted_identity:
                updates = []
                if sender_name_input and sender_name_input != st.session_state.sender_base_name:
                    st.session_state.sender_base_name = sender_name_input
                    st.session_state.sender_name = sender_name_input
                    if save_sender_name(sender_name_input):
                        update_sender_name()
                        updates.append("display name")
                    else:
                        st.error("Failed to update sender name.")
                if sender_email_input and sender_email_input != st.session_state.sender_email:
                    st.session_state.sender_email = sender_email_input
                    if save_sender_email(sender_email_input):
                        config['mailgun']['sender'] = sender_email_input
                        config['kvn_smtp']['sender'] = sender_email_input
                        updates.append("sender email")
                    else:
                        st.error("Failed to update sender email.")
                if updates:
                    st.success(f"Updated {', '.join(updates)} successfully.")
                else:
                    st.info("No changes detected.")
            st.markdown("<span></span></div>", unsafe_allow_html=True)

        with top_right:
            st.markdown("<div class='modern-card'>", unsafe_allow_html=True)
            st.subheader("Global Reply-to")
            st.caption("Used whenever a journal specific reply-to is not configured.")
            with st.form("default_reply_form"):
                default_reply_to_input = st.text_input(
                    "Default reply-to email",
                    st.session_state.get("default_reply_to", ""),
                    key="settings_default_reply_input",
                )
                submitted_default_reply = st.form_submit_button("Save Default Reply-to")

            if submitted_default_reply:
                if save_default_reply_address(default_reply_to_input):
                    st.success("Default reply-to email saved.")
                else:
                    st.error("Unable to store default reply-to email.")
            st.markdown("<span></span></div>", unsafe_allow_html=True)

        st.markdown("<div class='modern-card'>", unsafe_allow_html=True)
        st.subheader("Reply-to Addresses")
        reply_targets = sorted(set(JOURNALS + EDITOR_JOURNALS))
        if reply_targets:
            default_journal = (
                st.session_state.selected_journal
                if st.session_state.selected_journal in reply_targets
                else reply_targets[0]
            )
            with st.form("reply_to_form"):
                reply_choice = st.selectbox(
                    "Select journal",
                    reply_targets,
                    index=reply_targets.index(default_journal),
                    key="settings_reply_choice",
                )
                reply_value = st.text_input(
                    "Reply-to email",
                    st.session_state.journal_reply_addresses.get(reply_choice, "") or st.session_state.default_reply_to,
                    key="settings_reply_value",
                )
                submitted_reply = st.form_submit_button("Save Reply-to")

            if submitted_reply:
                if save_reply_address(reply_choice, reply_value):
                    st.success(f"Reply-to address for {reply_choice} saved.")
                else:
                    st.error("Failed to save reply-to address.")
        else:
            st.info("No journals available yet. Add journals to configure reply-to addresses.")

        if st.session_state.journal_reply_addresses:
            display_rows = [
                {
                    "Journal": name,
                    "Reply-To": addr or st.session_state.default_reply_to or "Not set",
                }
                for name, addr in sorted(st.session_state.journal_reply_addresses.items())
            ]
            st.dataframe(pd.DataFrame(display_rows))
        else:
            st.info("No reply-to addresses saved yet.")
        st.markdown("<span></span></div>", unsafe_allow_html=True)

    with services_tab:
        st.subheader("Email Service Preferences")
        service_options = ["SMTP2GO", "MAILGUN", "KVN SMTP"]
        current_service = (st.session_state.email_service or "MAILGUN").upper()
        if current_service not in service_options:
            current_service = "MAILGUN"

        with st.form("default_service_form"):
            default_service = st.selectbox(
                "Default email service",
                service_options,
                index=service_options.index(current_service),
                key="settings_default_service",
            )
            submitted_service = st.form_submit_button("Save Default Service")

        if submitted_service:
            st.session_state.email_service = default_service
            if save_default_email_service(default_service):
                st.success(f"Default service set to {get_service_display_name(default_service)}.")
            else:
                st.error("Failed to update default service.")

        st.markdown("---")
        st.subheader("KVN SMTP Configuration")
        kvn_settings = get_effective_kvn_settings()
        with st.form("kvn_smtp_form"):
            host_input = st.text_input("SMTP host", kvn_settings.get('host', ''), key="kvn_host")
            port_input = st.number_input("Port", min_value=1, max_value=65535, value=int(kvn_settings.get('port', 587)), key="kvn_port")
            username_input = st.text_input("Username", kvn_settings.get('username', ''), key="kvn_username")
            password_input = st.text_input("Password", kvn_settings.get('password', ''), type="password", key="kvn_password")
            sender_input = st.text_input("Sender email", kvn_settings.get('sender', st.session_state.sender_email), key="kvn_sender")
            use_tls_input = st.checkbox("Use TLS", value=bool(kvn_settings.get('use_tls', True)), key="kvn_use_tls")
            submitted_kvn = st.form_submit_button("Save KVN SMTP Settings")

        if submitted_kvn:
            settings_payload = {
                'host': host_input,
                'port': port_input,
                'username': username_input,
                'password': password_input,
                'sender': sender_input,
                'use_tls': use_tls_input,
            }
            if save_kvn_smtp_settings(settings_payload):
                st.success("KVN SMTP settings updated successfully.")
            else:
                st.error("Failed to update KVN SMTP settings.")

        if is_kvn_smtp_configured():
            st.caption("KVN SMTP is ready to send campaigns.")
        else:
            st.warning("KVN SMTP settings are incomplete. Fill in all fields to enable this service.")

    with compliance_tab:
        st.subheader("Suppression & Safety")
        with st.form("compliance_form"):
            blocked_domains_text = st.text_area(
                "Blocked domains",
                "\n".join(st.session_state.blocked_domains),
                key="settings_blocked_domains",
            )
            blocked_emails_text = st.text_area(
                "Blocked email addresses",
                "\n".join(st.session_state.blocked_emails),
                key="settings_blocked_emails",
            )
            submitted_compliance = st.form_submit_button("Save Suppression Lists")

        if submitted_compliance:
            st.session_state.blocked_domains = [d.strip() for d in blocked_domains_text.splitlines() if d.strip()]
            st.session_state.blocked_emails = [e.strip() for e in blocked_emails_text.splitlines() if e.strip()]
            if save_block_settings():
                st.success("Suppression lists updated successfully.")
            else:
                st.error("Failed to update suppression lists.")

        st.caption("Contacts in these lists will never receive campaign emails.")


def main():
    # Ensure base session defaults are populated once the Streamlit runtime is available.
    init_session_state()

    # Check authentication
    check_auth()

    # Main app for authenticated users
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] > .main {
            display: block;
            padding-top: 0;
            padding-bottom: 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Ensure session defaults are available before the sidebar interacts with them.
    ensure_session_defaults({
        'requested_mode': lambda: None,
        'active_app_mode': lambda: "Verify Emails",
        'sender_base_name': lambda: st.session_state.sender_name,
        'sender_name_loaded': lambda: False,
        'sender_email_loaded': lambda: False,
        'default_email_service_loaded': lambda: False,
        'reply_addresses_loaded': lambda: False,
        'kvn_settings_loaded': lambda: False,
        'kvn_smtp_settings': dict,
        'block_settings_loaded': lambda: False,
        'blocked_domains': list,
        'blocked_emails': list,
        'journals_loaded': lambda: False,
        'editor_journals_loaded': lambda: False,
        'template_content': dict,
        'journal_subjects': dict,
        'campaign_history': list,
        'unsubscribed_users_loaded': lambda: False,
        'unsubscribed_users': list,
        'unsubscribed_email_lookup': set,
        'firebase_initialized': lambda: False,
    })

    # Navigation with additional links and heading in the sidebar
    with st.sidebar:
        st.markdown("## PPH Email Verifier")
        sidebar_modes = ["Verify Emails", "Email Campaign", "Editor Invitation", "Settings"]
        if st.session_state.requested_mode and st.session_state.requested_mode in sidebar_modes:
            st.session_state.active_app_mode = st.session_state.requested_mode
            st.session_state.requested_mode = None
        default_index = sidebar_modes.index(st.session_state.active_app_mode) if st.session_state.active_app_mode in sidebar_modes else 0
        app_mode = st.selectbox(
            "Select Mode",
            sidebar_modes,
            index=default_index,
            key="app_mode_select",
        )
        st.session_state.active_app_mode = app_mode

        st.markdown("---")
        st.markdown("### Quick Links")
        st.markdown("[📊 Email Reports](https://app.mailgun.com/mg/reporting/metrics/)", unsafe_allow_html=True)
        st.markdown("[📝 Entry Manager](https://pphentry.onrender.com)", unsafe_allow_html=True)
        st.markdown(
            f"<span class='status-badge'>Service: {get_service_display_name(st.session_state.email_service)}</span>",
            unsafe_allow_html=True,
        )
        check_incomplete_operations()
    
    # Initialize services lazily based on the active mode
    requires_marketing_data = app_mode in {"Email Campaign", "Editor Invitation", "Settings"}

    if not st.session_state.get('firebase_initialized'):
        initialize_firebase()

    if st.session_state.get('firebase_initialized') and requires_marketing_data:
        if not st.session_state.get('kvn_settings_loaded'):
            load_kvn_smtp_settings()
            st.session_state.kvn_settings_loaded = True
        if not st.session_state.get('reply_addresses_loaded'):
            load_reply_addresses()
            st.session_state.reply_addresses_loaded = True
        if not st.session_state.get('default_email_service_loaded'):
            stored_service = load_default_email_service()
            if stored_service:
                st.session_state.email_service = stored_service.upper()
            st.session_state.default_email_service_loaded = True
        if not st.session_state.get('sender_email_loaded'):
            stored_sender_email = load_sender_email()
            if stored_sender_email:
                st.session_state.sender_email = stored_sender_email
                config['mailgun']['sender'] = stored_sender_email
                config['kvn_smtp']['sender'] = stored_sender_email
            st.session_state.sender_email_loaded = True

    if requires_marketing_data and st.session_state.get('firebase_initialized'):
        if not st.session_state.get('unsubscribed_users_loaded'):
            load_unsubscribed_users()

    if app_mode == "Email Campaign":
        email_campaign_section()
    elif app_mode == "Editor Invitation":
        editor_invitation_section()
    elif app_mode == "Verify Emails":
        email_verification_section()
    else:
        settings_section()

if __name__ == "__main__":
    main()
