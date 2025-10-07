import os
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore
import streamlit as st


def _get_firebase_config():
    return {
        "type": os.getenv("FIREBASE_TYPE", ""),
        "project_id": os.getenv("FIREBASE_PROJECT_ID", ""),
        "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID", ""),
        "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.getenv("FIREBASE_CLIENT_EMAIL", ""),
        "client_id": os.getenv("FIREBASE_CLIENT_ID", ""),
        "auth_uri": os.getenv("FIREBASE_AUTH_URI", ""),
        "token_uri": os.getenv("FIREBASE_TOKEN_URI", ""),
        "auth_provider_x509_cert_url": os.getenv("FIREBASE_AUTH_PROVIDER_CERT_URL", ""),
        "client_x509_cert_url": os.getenv("FIREBASE_CLIENT_CERT_URL", ""),
        "universe_domain": os.getenv("FIREBASE_UNIVERSE_DOMAIN", "googleapis.com"),
    }


def _get_firebase_app():
    if firebase_admin._apps:
        return firebase_admin.get_app()

    config = _get_firebase_config()
    private_key = config["private_key"]
    if private_key and not private_key.startswith("-----BEGIN PRIVATE KEY-----"):
        private_key = "-----BEGIN PRIVATE KEY-----\n" + private_key + "\n-----END PRIVATE KEY-----"

    cred = credentials.Certificate(
        {
            "type": config["type"],
            "project_id": config["project_id"],
            "private_key_id": config["private_key_id"],
            "private_key": private_key,
            "client_email": config["client_email"],
            "client_id": config["client_id"],
            "auth_uri": config["auth_uri"],
            "token_uri": config["token_uri"],
            "auth_provider_x509_cert_url": config["auth_provider_x509_cert_url"],
            "client_x509_cert_url": config["client_x509_cert_url"],
            "universe_domain": config["universe_domain"],
        }
    )

    return firebase_admin.initialize_app(cred)


def _get_firestore_client():
    app = _get_firebase_app()
    return firestore.client(app=app)


def _mark_email_unsubscribed(email: str) -> bool:
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        return False

    try:
        db = _get_firestore_client()
        doc_ref = db.collection("unsubscribed_users").document(normalized_email)
        timestamp = datetime.utcnow()
        doc_ref.set(
            {
                "email": normalized_email,
                "unsubscribed": True,
                "unsubscribed_at": timestamp,
                "updated_at": timestamp,
            },
            merge=True,
        )
        return True
    except Exception as exc:  # pylint: disable=broad-except
        st.error(f"Unable to unsubscribe {normalized_email}: {exc}")
        return False


def main():
    st.set_page_config(page_title="Unsubscribe", page_icon="📭", layout="centered")
    st.title("Unsubscribe")

    params = st.experimental_get_query_params()
    email_param = params.get("email")
    email = email_param[0] if email_param else ""

    if not email:
        st.warning("No email address was provided.")
        return

    with st.spinner("Updating your subscription preferences..."):
        if _mark_email_unsubscribed(email):
            st.success("You have been unsubscribed successfully.")
        else:
            st.error("We couldn't process your unsubscribe request. Please try again later.")


if __name__ == "__main__":
    main()
