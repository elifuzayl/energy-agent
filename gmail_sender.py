"""
Gmail sender — works both locally and on GitHub Actions.
On GitHub Actions, token is loaded from GMAIL_TOKEN_JSON environment variable.
"""

import os
import json
import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

log = logging.getLogger(__name__)

SCOPES     = ["https://www.googleapis.com/auth/gmail.send"]
CREDS_FILE = os.path.join(os.path.dirname(__file__), "credentials.json")
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token.json")


def get_gmail_service():
    creds = None

    # GitHub Actions: load token from environment variable
    token_json = os.environ.get("GMAIL_TOKEN_JSON")
    if token_json:
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)

    # Local: load from token.json file
    elif os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    # Refresh if expired
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        # Save refreshed token back to file (local only)
        if not os.environ.get("GMAIL_TOKEN_JSON"):
            with open(TOKEN_FILE, "w") as f:
                f.write(creds.to_json())

    # First-time local auth (opens browser)
    if not creds or not creds.valid:
        if not os.environ.get("GMAIL_TOKEN_JSON"):
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(TOKEN_FILE, "w") as f:
                f.write(creds.to_json())
        else:
            raise RuntimeError("GMAIL_TOKEN_JSON is set but token is invalid — re-authenticate locally.")

    return build("gmail", "v1", credentials=creds)


def send_email_gmail(subject: str, html_body: str, recipients: list[str]) -> None:
    if not recipients:
        log.warning("No recipients — skipping")
        return
    service = get_gmail_service()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    log.info(f"Gmail sent to {len(recipients)} recipients")
