import json
import time
from pathlib import Path
from typing import Dict, List

import requests
import streamlit as st


PROGRESS_FILE = Path("verification_progress.json")
REQUEST_TIMEOUT = 20


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: #f7f9fc;
        }
        .main .block-container {
            max-width: 850px;
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        .app-card {
            background: #ffffff;
            border: 1px solid #e9eef5;
            border-radius: 14px;
            padding: 1.25rem;
            margin-bottom: 1rem;
            box-shadow: 0 4px 16px rgba(17, 24, 39, 0.04);
        }
        .app-title {
            text-align: center;
            margin-bottom: 0.25rem;
        }
        .app-subtitle {
            text-align: center;
            color: #5f6b7a;
            margin-bottom: 1rem;
        }
        .stButton > button, .stDownloadButton > button {
            border-radius: 10px !important;
            padding: 0.55rem 1rem !important;
            font-weight: 600 !important;
            border: 1px solid #d8e2f1 !important;
        }
        .status-pill {
            background: #eef4ff;
            border: 1px solid #d8e2f1;
            border-radius: 10px;
            padding: 0.6rem 0.8rem;
            color: #324056;
            font-size: 0.95rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def parse_uploaded_emails(file_bytes: bytes) -> List[str]:
    text = file_bytes.decode("utf-8", errors="ignore")
    emails: List[str] = []
    seen = set()
    for line in text.splitlines():
        email = line.strip()
        if not email:
            continue
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        emails.append(email)
    return emails


def verify_email(email: str, api_key: str) -> str:
    url = "https://api.millionverifier.com/api/v3/"
    try:
        response = requests.get(
            url,
            params={"api": api_key, "email": email},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        return f"error:{exc}"
    except ValueError:
        return "error:invalid_json"

    result = str(data.get("result", "unknown")).strip().lower()
    return result or "unknown"


def load_progress() -> Dict:
    if not PROGRESS_FILE.exists():
        return {}

    try:
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_progress(progress: Dict) -> None:
    PROGRESS_FILE.write_text(
        json.dumps(progress, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def clear_progress() -> None:
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


def build_result_text(results: List[Dict[str, str]]) -> str:
    lines = [f"{item['email']}|{item['status']}" for item in results]
    return "\n".join(lines)


def progress_matches_current_file(progress_data: Dict, emails: List[str], file_name: str) -> bool:
    return (
        bool(progress_data)
        and progress_data.get("file_name") == file_name
        and progress_data.get("emails") == emails
    )


def initialize_state() -> None:
    defaults = {
        "results": [],
        "processed_count": 0,
        "remaining_count": 0,
        "active_file_name": "",
        "active_emails": [],
        "is_running": False,
        "result_filename": "verified_results.txt",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def run_verification(api_key: str, emails: List[str], file_name: str, start_index: int, prior_results: List[Dict[str, str]]) -> None:
    st.session_state.is_running = True
    st.session_state.active_file_name = file_name
    st.session_state.active_emails = emails
    st.session_state.results = prior_results.copy()

    total = len(emails)
    progress_bar = st.progress(start_index / total if total else 0.0)
    status_placeholder = st.empty()

    processed = start_index
    status_placeholder.info(f"Resuming from {start_index}/{total}" if start_index else "Starting verification...")

    for idx in range(start_index, total):
        email = emails[idx]
        status = verify_email(email, api_key)
        st.session_state.results.append({"email": email, "status": status})
        processed = idx + 1

        st.session_state.processed_count = processed
        st.session_state.remaining_count = total - processed

        save_progress(
            {
                "file_name": file_name,
                "emails": emails,
                "results": st.session_state.results,
                "last_processed_index": processed,
                "updated_at": int(time.time()),
            }
        )

        progress_bar.progress(processed / total)
        status_placeholder.info(f"Processed {processed}/{total} | Remaining {total - processed}")

    st.session_state.is_running = False
    clear_progress()
    status_placeholder.success("Verification completed. Progress file cleared.")


def render_header() -> None:
    st.markdown("<h1 class='app-title'>Email Verification Tool</h1>", unsafe_allow_html=True)
    st.markdown(
        "<p class='app-subtitle'>Upload a TXT list, verify with MillionVerifier, and download results.</p>",
        unsafe_allow_html=True,
    )


def render_status(processed: int, remaining: int, total: int) -> None:
    col1, col2, col3 = st.columns(3)
    col1.markdown(f"<div class='status-pill'><b>Total:</b> {total}</div>", unsafe_allow_html=True)
    col2.markdown(f"<div class='status-pill'><b>Processed:</b> {processed}</div>", unsafe_allow_html=True)
    col3.markdown(f"<div class='status-pill'><b>Remaining:</b> {remaining}</div>", unsafe_allow_html=True)


def main() -> None:
    st.set_page_config(page_title="Email Verification Tool", page_icon="✅", layout="centered")
    initialize_state()
    inject_styles()

    with st.container():
        st.markdown("<div class='app-card'>", unsafe_allow_html=True)
        render_header()

        api_key = st.text_input("MillionVerifier API Key", type="password", placeholder="Enter API key")
        uploaded = st.file_uploader("Upload TXT file (one email per line)", type=["txt"])

        if not api_key:
            st.warning("Enter your MillionVerifier API key to continue.")

        emails: List[str] = []
        file_name = ""
        if uploaded is not None:
            file_name = uploaded.name
            emails = parse_uploaded_emails(uploaded.getvalue())
            if not emails:
                st.error("No valid emails found in uploaded file.")
            else:
                st.success(f"Loaded {len(emails)} unique email(s) from {file_name}")

        progress_data = load_progress()
        can_resume = bool(uploaded and progress_matches_current_file(progress_data, emails, file_name))

        start_index = 0
        prior_results: List[Dict[str, str]] = []
        if can_resume:
            start_index = int(progress_data.get("last_processed_index", 0))
            prior_results = list(progress_data.get("results", []))
            st.info(f"Resume available: {start_index}/{len(emails)} already processed.")

        total = len(emails)
        processed = start_index if can_resume else 0
        remaining = max(total - processed, 0)
        render_status(processed, remaining, total)

        left, right = st.columns([1, 1])
        start_label = "Resume Verification" if can_resume and start_index > 0 else "Start Verification"
        start_clicked = left.button(start_label, type="primary", use_container_width=True, disabled=not (api_key and emails))
        reset_clicked = right.button("Reset Saved Progress", use_container_width=True)

        if reset_clicked:
            clear_progress()
            st.session_state.results = []
            st.session_state.processed_count = 0
            st.session_state.remaining_count = total
            st.success("Saved progress removed.")

        if start_clicked:
            run_verification(api_key, emails, file_name, start_index, prior_results)
            st.session_state.result_filename = f"{Path(file_name).stem}_verified.txt" if file_name else "verified_results.txt"

        if st.session_state.results:
            result_text = build_result_text(st.session_state.results)
            st.markdown("---")
            st.subheader("Download Results")
            renamed = st.text_input(
                "Result filename",
                value=st.session_state.result_filename,
                help="Name of the downloaded TXT file",
            )
            if renamed:
                st.session_state.result_filename = renamed if renamed.endswith(".txt") else f"{renamed}.txt"

            st.download_button(
                label="Download Verified File",
                data=result_text.encode("utf-8"),
                file_name=st.session_state.result_filename,
                mime="text/plain",
                use_container_width=True,
            )

        st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
