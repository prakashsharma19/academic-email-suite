"""Streamlit page for managing unsubscribe requests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
import base64

import sys

import streamlit as st
from streamlit.errors import StreamlitAPIException
from jinja2 import Environment, FileSystemLoader, select_autoescape

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from app import (
    EMAIL_VALIDATION_REGEX,
    initialize_firebase,
    is_email_unsubscribed,
    load_unsubscribed_users,
    set_email_unsubscribed,
)

PAGE_TITLE = "Unsubscribe Confirmation"


@st.cache_resource(show_spinner=False)
def _load_unsubscribe_template():
    templates_dir = Path(__file__).resolve().parents[1] / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    return env.get_template("unsubscribe.html")


@st.cache_data(show_spinner=False)
def _load_logo_data() -> str:
    logo_path = PROJECT_ROOT / "PPHLogo_en.png"
    if not logo_path.exists():
        return ""
    try:
        encoded = base64.b64encode(logo_path.read_bytes()).decode("utf-8")
    except Exception:
        return ""
    return f"data:image/png;base64,{encoded}"


def _get_query_params() -> Dict[str, str]:
    try:
        params = dict(st.query_params)
    except AttributeError:
        params = {
            key: (value[0] if isinstance(value, list) else value)
            for key, value in st.experimental_get_query_params().items()
        }
    return {key: value for key, value in params.items() if value is not None}


def _ensure_session_state_defaults():
    if "firebase_initialized" not in st.session_state:
        st.session_state.firebase_initialized = False
    if "unsubscribed_users_loaded" not in st.session_state:
        st.session_state.unsubscribed_users_loaded = False


def _render_template(**context):
    template = _load_unsubscribe_template()
    html = template.render(**context)
    st.components.v1.html(html, height=800, scrolling=True)


def _ensure_firebase_loaded():
    _ensure_session_state_defaults()
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    if not st.session_state.unsubscribed_users_loaded:
        load_unsubscribed_users()


def main():
    try:
        st.set_page_config(page_title=PAGE_TITLE, layout="centered", initial_sidebar_state="collapsed")
    except StreamlitAPIException:
        # The host app (app.py) sets page config already when running in multipage mode.
        pass

    st.markdown(
        """
        <style>
        header, footer, #MainMenu {visibility: hidden !important;}
        [data-testid="stSidebar"] {display: none !important;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    params = _get_query_params()
    email = (params.get("email") or "").strip()
    heading = "Manage Your Subscription"
    message = None
    status = "info"
    unsubscribed = False

    if not email:
        heading = "Email Address Required"
        message = "No email address was provided for unsubscribe."
        status = "error"
    elif not EMAIL_VALIDATION_REGEX.match(email):
        heading = "Invalid Email"
        message = "The email address provided appears to be invalid."
        status = "error"
    else:
        _ensure_firebase_loaded()
        already_unsubscribed = is_email_unsubscribed(email)
        success = set_email_unsubscribed(email)
        if success:
            load_unsubscribed_users(force_refresh=True)
            unsubscribed = True
            heading = "You're Unsubscribed"
            if already_unsubscribed:
                message = "This email address was already unsubscribed. We'll continue to exclude it from future communications."
                status = "info"
            else:
                message = "You have been unsubscribed. We will exclude this address from future communications."
                status = "success"
        else:
            heading = "We're Sorry"
            message = "We were unable to process your unsubscribe request at this time. Please try again later."
            status = "error"

    context = {
        "email": email,
        "message": message,
        "status": status,
        "unsubscribed": unsubscribed,
        "heading": heading,
        "current_year": datetime.now(timezone.utc).year,
        "logo_data": _load_logo_data(),
    }

    _render_template(**context)


if __name__ == "__main__":
    main()
