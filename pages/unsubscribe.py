"""Disabled unsubscribe page placeholder."""

from streamlit.errors import StreamlitAPIException
import streamlit as st

PAGE_TITLE = "Unsubscribe"


def main():
    try:
        st.set_page_config(
            page_title=PAGE_TITLE,
            layout="centered",
            initial_sidebar_state="collapsed",
        )
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

    st.title("Service Temporarily Unavailable")
    st.info(
        "The unsubscribe service has been disabled on this platform. "
        "Please contact the administrator for assistance."
    )


if __name__ == "__main__":
    main()
