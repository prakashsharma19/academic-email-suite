"""Streamlit page for managing unsubscribe requests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlencode

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
    set_email_resubscribed,
    set_email_unsubscribed,
)

PAGE_TITLE = "Manage Email Preferences"


@st.cache_resource(show_spinner=False)
def _load_unsubscribe_template():
    templates_dir = Path(__file__).resolve().parents[1] / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    return env.get_template("unsubscribe.html")


def _get_query_params() -> Dict[str, str]:
    try:
        params = dict(st.query_params)
    except AttributeError:
        params = {
            key: (value[0] if isinstance(value, list) else value)
            for key, value in st.experimental_get_query_params().items()
        }
    return {key: value for key, value in params.items() if value is not None}


def _set_query_params(params: Dict[str, str]):
    params = {key: value for key, value in params.items() if value}
    try:
        st.query_params.clear()
        for key, value in params.items():
            st.query_params[key] = value
    except AttributeError:
        st.experimental_set_query_params(**params)


def _pop_feedback(email: str) -> Optional[Dict[str, str]]:
    feedback = st.session_state.get("unsubscribe_feedback")
    if feedback and feedback.get("email") == email:
        st.session_state.pop("unsubscribe_feedback", None)
        return feedback
    return None


def _store_feedback(email: str, message: str, status: str):
    st.session_state.unsubscribe_feedback = {
        "email": email,
        "message": message,
        "status": status,
    }


def _render_template(**context):
    template = _load_unsubscribe_template()
    html = template.render(**context)
    st.components.v1.html(html, height=800, scrolling=True)


def _ensure_firebase_loaded():
    if not st.session_state.firebase_initialized:
        initialize_firebase()
    if not st.session_state.unsubscribed_users_loaded:
        load_unsubscribed_users()


def _process_action(action: str, email: str) -> Optional[Dict[str, str]]:
    if action not in {"unsubscribe", "resubscribe"}:
        return None

    if action == "unsubscribe":
        success = set_email_unsubscribed(email)
        if success:
            load_unsubscribed_users(force_refresh=True)
            return {
                "message": "You have been unsubscribed successfully and will be excluded from future campaigns.",
                "status": "success",
            }
    else:
        success = set_email_resubscribed(email)
        if success:
            load_unsubscribed_users(force_refresh=True)
            return {
                "message": "You have been re-subscribed successfully and will receive future campaigns.",
                "status": "success",
            }

    return {
        "message": "We were unable to process your request at this time. Please try again later.",
        "status": "error",
    }


def main():
    try:
        st.set_page_config(page_title=PAGE_TITLE, layout="centered", initial_sidebar_state="collapsed")
    except StreamlitAPIException:
        # The host app (app.py) sets page config already when running in multipage mode.
        pass

    params = _get_query_params()
    email = (params.get("email") or "").strip()
    action = (params.get("action") or "").strip().lower()

    message = None
    status = "info"
    show_actions = False
    unsubscribed = False
    excluded_from_future_sends = False

    if not email:
        message = "No email address was provided for unsubscribe."
        status = "error"
    elif not EMAIL_VALIDATION_REGEX.match(email):
        message = "The email address provided appears to be invalid."
        status = "error"
    else:
        _ensure_firebase_loaded()
        unsubscribed = is_email_unsubscribed(email)
        excluded_from_future_sends = unsubscribed
        show_actions = True

        if action:
            feedback = _process_action(action, email)
            if feedback:
                _store_feedback(email, feedback["message"], feedback["status"])
            _set_query_params({"email": email})
            st.experimental_rerun()

        feedback = _pop_feedback(email)
        if feedback:
            message = feedback["message"]
            status = feedback["status"]
        elif unsubscribed:
            message = "This email address is currently unsubscribed."
            status = "info"

    context = {
        "email": email,
        "message": message,
        "status": status,
        "unsubscribed": unsubscribed,
        "show_actions": show_actions,
        "excluded_from_future_sends": excluded_from_future_sends,
        "current_year": datetime.now(timezone.utc).year,
        "use_links_for_actions": show_actions,
        "link_target": "_top",
        "unsubscribe_action_url": f"?{urlencode({'email': email, 'action': 'unsubscribe'})}" if show_actions else "",
        "resubscribe_action_url": f"?{urlencode({'email': email, 'action': 'resubscribe'})}" if show_actions else "",
    }

    _render_template(**context)


if __name__ == "__main__":
    main()
