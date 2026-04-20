from __future__ import annotations

import os
import smtplib
import ssl
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

from utils.security import resolve_secret


class EmailSendError(RuntimeError):
    pass


def _safe_header(s: str) -> str:
    try:
        return str(Header(s, "utf-8"))
    except Exception:
        return s


def send_smtp_email(
    smtp_cfg: Dict[str, Any],
    to_addrs: List[str],
    subject: str,
    body_html: str,
    attachments: Optional[List[str]] = None,
) -> None:
    """Send one email via SMTP with HTML body + optional file attachments."""

    attachments = attachments or []

    server = str(smtp_cfg.get("server", "")).strip()
    port = int(smtp_cfg.get("port", 587))
    security = str(smtp_cfg.get("security", "TLS")).upper()
    username = str(smtp_cfg.get("username", "")).strip()
    password = resolve_secret(
        str(smtp_cfg.get("password", "")).strip(),
        env_var=str(smtp_cfg.get("password_env", "") or ""),
        default_env_var="SCADA_SMTP_PASSWORD",
        keyring_service=str(smtp_cfg.get("password_keyring_service", "") or ""),
        keyring_username=str(smtp_cfg.get("password_keyring_username", "") or ""),
    )
    from_addr = str(smtp_cfg.get("from_addr", "")).strip() or username
    display_name = str(smtp_cfg.get("display_name", "")).strip()
    timeout = float(smtp_cfg.get("timeout_sec", 12))

    if not server:
        raise EmailSendError("SMTP server is empty")
    if not from_addr:
        raise EmailSendError("From address is empty")
    if not to_addrs:
        raise EmailSendError("No recipients")

    msg = MIMEMultipart()
    msg["Subject"] = _safe_header(subject)
    msg["From"] = _safe_header(f"{display_name} <{from_addr}>" if display_name else from_addr)
    msg["To"] = ", ".join(to_addrs)

    msg.attach(MIMEText(body_html or "", "html", "utf-8"))

    # Attach files
    for p in attachments:
        p = str(p or "").strip()
        if not p or not os.path.exists(p):
            continue
        try:
            with open(p, "rb") as f:
                part = MIMEApplication(f.read())
            part.add_header("Content-Disposition", "attachment", filename=os.path.basename(p))
            msg.attach(part)
        except Exception:
            continue

    try:
        if security == "SSL":
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(server, port, timeout=timeout, context=context) as s:
                if username:
                    s.login(username, password)
                s.sendmail(from_addr, to_addrs, msg.as_string())
        else:
            with smtplib.SMTP(server, port, timeout=timeout) as s:
                s.ehlo()
                if security == "TLS":
                    s.starttls(context=ssl.create_default_context())
                    s.ehlo()
                if username:
                    s.login(username, password)
                s.sendmail(from_addr, to_addrs, msg.as_string())
    except Exception as e:
        raise EmailSendError(str(e))
