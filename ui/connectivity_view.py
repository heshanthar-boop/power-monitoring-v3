"""
Connectivity Settings Tab
=========================
Unified panel for all outbound/inbound connectivity:

  • Modbus TCP Gateway  — switch from RTU/serial to a TCP gateway
  • MQTT Publisher      — live readings to MQTT broker
  • REST API            — HTTP/JSON pull endpoint for dashboards

Design principles
-----------------
• Engineering-grade: all fields validated before saving.
• Non-destructive: saving connectivity settings does NOT restart the
  Modbus worker unless the user explicitly clicks "Apply & Reconnect".
• Status indicators for MQTT and REST API are updated on every UI tick.
"""
from __future__ import annotations

import time
import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Dict, Optional

from ui.styles import get_theme
from utils.logger import setup_logger

logger = setup_logger("connectivity_view")


# ── helpers ───────────────────────────────────────────────────────────────────

def _section(parent, title: str) -> ttk.LabelFrame:
    f = ttk.LabelFrame(parent, text=f"  {title}  ",
                       style="Form.TLabelframe", padding=(16, 12))
    f.pack(fill="x", padx=12, pady=(12, 0))
    return f


def _row(parent, label: str, row: int, entry_width: int = 22) -> tk.StringVar:
    ttk.Label(parent, text=label, style="FieldLabel.TLabel").grid(
        row=row, column=0, sticky="w", padx=(0, 12), pady=4)
    var = tk.StringVar()
    ttk.Entry(parent, textvariable=var, width=entry_width).grid(
        row=row, column=1, sticky="w")
    return var


def _bool_row(parent, label: str, row: int) -> tk.BooleanVar:
    var = tk.BooleanVar()
    ttk.Checkbutton(parent, text=label, variable=var).grid(
        row=row, column=0, columnspan=2, sticky="w", pady=4)
    return var


# ── main tab ──────────────────────────────────────────────────────────────────

class ConnectivityTab(ttk.Frame):
    """
    Full connectivity settings page.

    Parameters
    ----------
    parent        : Tk parent widget
    cfg           : live config dict (shared reference)
    on_save_config: callable(cfg) — persists config to disk
    mqtt_publisher: MQTTPublisher instance (may be None until wired)
    rest_api      : RESTApiServer instance (may be None until wired)
    on_apply_comm : callable() — restart Modbus worker (from CommTab pattern)
    """

    def __init__(
        self,
        parent,
        cfg: dict,
        on_save_config: Optional[Callable] = None,
        mqtt_publisher=None,
        rest_api=None,
        remote_sync=None,
        on_apply_comm: Optional[Callable] = None,
        sms_service=None,
    ) -> None:
        super().__init__(parent)
        self.cfg           = cfg
        self._save_cb      = on_save_config
        self._mqtt         = mqtt_publisher
        self._rest         = rest_api
        self._remote_sync  = remote_sync
        self._apply_comm   = on_apply_comm
        self._sms_service  = sms_service

        self._build_ui()
        self._load_from_cfg()

    # ── build ─────────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        # scrollable inner frame
        canvas = tk.Canvas(self, borderwidth=0, highlightthickness=0)
        vsb    = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)

        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = ttk.Frame(canvas)
        win   = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_inner_cfg(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
            canvas.itemconfig(win, width=canvas.winfo_width())
        inner.bind("<Configure>", _on_inner_cfg)
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(win, width=e.width))

        self._build_tcp_section(inner)
        self._build_mqtt_section(inner)
        self._build_rest_section(inner)
        self._build_remote_sync_section(inner)
        self._build_firebase_section(inner)
        self._build_sms_section(inner)

        # bottom action bar
        bar = ttk.Frame(inner)
        bar.pack(fill="x", padx=12, pady=14)
        ttk.Button(bar, text="💾  Save All",
                   command=self._save_all).pack(side="right", padx=(6, 0))
        ttk.Button(bar, text="↩  Reset to Saved",
                   command=self._load_from_cfg).pack(side="right")

    # ── Modbus TCP section ────────────────────────────────────────────────────

    def _build_tcp_section(self, parent) -> None:
        f = _section(parent, "Modbus TCP Gateway  (IEC 61158 / IEC 61784)")

        note = ttk.Label(
            f,
            text=(
                "Switch transport from RS-485/RTU to a Modbus TCP gateway (Ethernet).\n"
                "Set TCP Host + Port and click Apply & Reconnect. Leave blank for RTU mode."
            ),
            wraplength=640, justify="left",
        )
        note.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

        self._tcp_enabled  = _bool_row(f, "Enable Modbus TCP transport", 1)
        self._tcp_host     = _row(f, "TCP Host (IP or hostname):", 2, 26)
        self._tcp_port_var = _row(f, "TCP Port (default 502):",   3, 8)

        ttk.Button(
            f, text="⚡  Apply & Reconnect",
            command=self._apply_tcp,
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 0))

        self._lbl_tcp_status = ttk.Label(f, text="")
        self._lbl_tcp_status.grid(row=4, column=2, sticky="e", padx=(20, 0))

    # ── MQTT section ──────────────────────────────────────────────────────────

    def _build_mqtt_section(self, parent) -> None:
        f = _section(parent, "MQTT Publisher  (OASIS MQTT v3.1.1 / v5.0)")

        note = ttk.Label(
            f,
            text=(
                "Publish live meter readings to an MQTT broker.\n"
                "Topic: {prefix}/live/{source}/{key}  •  Payload: {\"v\":125.5,\"ts\":...,\"q\":\"GOOD\"}\n"
                "Requires paho-mqtt:  pip install paho-mqtt"
            ),
            wraplength=640, justify="left",
        )
        note.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

        self._mqtt_enabled  = _bool_row(f, "Enable MQTT publisher", 1)
        self._mqtt_broker   = _row(f, "Broker host:",         2, 26)
        self._mqtt_port_var = _row(f, "Broker port (1883):",  3, 8)
        self._mqtt_prefix   = _row(f, "Topic prefix:",        4, 20)
        self._mqtt_user     = _row(f, "Username (optional):", 5, 20)
        self._mqtt_pass     = _row(f, "Password (optional):", 6, 20)
        self._mqtt_qos_var  = _row(f, "QoS (0/1/2):",         7, 4)
        self._mqtt_retain   = _bool_row(f, "Retain last message on broker", 8)
        self._mqtt_interval = _row(f, "Publish interval (sec):", 9, 6)
        self._mqtt_tls      = _bool_row(f, "Enable TLS", 10)

        # ── Bandwidth / GPRS-4G mode ──────────────────────────────────────────
        ttk.Separator(f, orient="horizontal").grid(
            row=11, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        ttk.Label(f, text="Bandwidth / GPRS-4G mode",
                  font=("Segoe UI", 9, "bold")).grid(
            row=12, column=0, columnspan=3, sticky="w", pady=(0, 4))

        ttk.Label(f, text="Mode:", style="FieldLabel.TLabel").grid(
            row=13, column=0, sticky="w", padx=(0, 12), pady=4)
        self._mqtt_bw_mode = tk.StringVar(value="normal")
        bw_frame = ttk.Frame(f)
        bw_frame.grid(row=13, column=1, sticky="w")
        ttk.Radiobutton(bw_frame, text="Normal", variable=self._mqtt_bw_mode,
                        value="normal").pack(side="left", padx=(0, 12))
        ttk.Radiobutton(bw_frame, text="GPRS / 4G (metered)",
                        variable=self._mqtt_bw_mode,
                        value="gprs").pack(side="left")

        self._mqtt_min_interval = _row(f, "Min interval in GPRS mode (sec):", 14, 6)
        self._mqtt_compress     = _bool_row(
            f, "Compress summary payload (zlib)  — broker & subscribers must handle binary", 15)
        self._mqtt_summary_only = _bool_row(
            f, "Summary-only  — skip per-parameter /live/ topics (maximum bandwidth saving)", 16)

        ttk.Label(
            f,
            text=(
                "GPRS mode enforces a minimum publish interval regardless of the interval setting above.\n"
                "Summary-only reduces publish count from (N_params × N_meters + 1) to 1 per cycle."
            ),
            foreground="#888888",
            font=("Segoe UI", 8),
            wraplength=560, justify="left",
        ).grid(row=17, column=0, columnspan=3, sticky="w", pady=(0, 4))

        ttk.Button(
            f, text="✔  Apply MQTT",
            command=self._apply_mqtt,
        ).grid(row=18, column=0, sticky="w", pady=(8, 0))

        # Status badge (updated from update_view)
        status_row = ttk.Frame(f)
        status_row.grid(row=18, column=1, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Label(status_row, text="Status:").pack(side="left", padx=(0, 6))
        self._lbl_mqtt_state = ttk.Label(status_row, text="DISABLED", width=14,
                                          relief="ridge", anchor="center")
        self._lbl_mqtt_state.pack(side="left")
        self._lbl_mqtt_err = ttk.Label(status_row, text="", foreground="red")
        self._lbl_mqtt_err.pack(side="left", padx=(8, 0))

    # ── REST API section ──────────────────────────────────────────────────────

    def _build_rest_section(self, parent) -> None:
        f = _section(parent, "REST API  (HTTP/JSON pull endpoint)")

        note = ttk.Label(
            f,
            text=(
                "Expose live readings over HTTP so Grafana, Node-RED, and custom\n"
                "dashboards can pull data without accessing Modbus directly.\n"
                "Endpoints: GET /api/v1/readings  •  /api/v1/meters  •  /api/v1/status  •  /health"
            ),
            wraplength=640, justify="left",
        )
        note.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

        self._rest_enabled  = _bool_row(f, "Enable REST API server", 1)
        self._rest_host     = _row(f, "Bind host (127.0.0.1 = local only):", 2, 20)
        self._rest_port_var = _row(f, "Bind port (default 8080):",           3, 8)
        self._rest_api_key  = _row(f, "API key (blank = no auth):",          4, 28)

        ttk.Button(
            f, text="✔  Apply REST API",
            command=self._apply_rest,
        ).grid(row=5, column=0, sticky="w", pady=(8, 0))

        status_row = ttk.Frame(f)
        status_row.grid(row=5, column=1, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Label(status_row, text="Status:").pack(side="left", padx=(0, 6))
        self._lbl_rest_state = ttk.Label(status_row, text="STOPPED", width=14,
                                          relief="ridge", anchor="center")
        self._lbl_rest_state.pack(side="left")

    def _build_remote_sync_section(self, parent) -> None:
        f = _section(parent, "Remote Sync Health  (offline-first queue)")

        note = ttk.Label(
            f,
            text=(
                "Keeps remote monitoring usable during poor internet. REST always shows the latest local snapshot; "
                "MQTT snapshots are queued on disk when the broker is offline and replayed when it reconnects."
            ),
            wraplength=760, justify="left",
        )
        note.grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 8))

        self._sync_enabled = _bool_row(f, "Enable offline queue for MQTT remote monitoring", 1)
        self._sync_max_queue = _row(f, "Max queued snapshots:", 2, 8)
        self._sync_replay_batch = _row(f, "Replay batch size:", 3, 8)
        self._sync_replay_interval = _row(f, "Replay spacing (sec):", 4, 8)

        btn_row = ttk.Frame(f)
        btn_row.grid(row=5, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Button(btn_row, text="Apply Sync Settings", command=self._apply_remote_sync).pack(side="left")
        ttk.Button(btn_row, text="Clear Queue", style="Ghost.TButton", command=self._clear_remote_sync_queue).pack(side="left", padx=(8, 0))

        status = ttk.Frame(f)
        status.grid(row=1, column=2, rowspan=5, sticky="nsew", padx=(24, 0))
        status.columnconfigure(1, weight=1)

        self._lbl_sync_state = ttk.Label(status, text="UNCONFIGURED", width=16, relief="ridge", anchor="center")
        self._lbl_sync_state.grid(row=0, column=0, sticky="w", pady=(0, 4))
        self._lbl_sync_queue = ttk.Label(status, text="Queue: -")
        self._lbl_sync_queue.grid(row=1, column=0, columnspan=2, sticky="w", pady=2)
        self._lbl_sync_counts = ttk.Label(status, text="Sent: -  Failed: -  Dropped: -")
        self._lbl_sync_counts.grid(row=2, column=0, columnspan=2, sticky="w", pady=2)
        self._lbl_sync_last = ttk.Label(status, text="Last sync: -")
        self._lbl_sync_last.grid(row=3, column=0, columnspan=2, sticky="w", pady=2)
        self._lbl_sync_err = ttk.Label(status, text="", foreground="red", wraplength=360)
        self._lbl_sync_err.grid(row=4, column=0, columnspan=2, sticky="w", pady=(2, 0))

    # ── Firebase section ──────────────────────────────────────────────────────

    def _build_firebase_section(self, parent) -> None:
        f = _section(parent, "Firebase Cloud Push  (Google Firestore — free tier)")

        note = ttk.Label(
            f,
            text=(
                "Push live meter readings to Google Firestore so the web dashboard\n"
                "and mobile browsers can monitor from anywhere — no port forwarding needed.\n"
                "Requires:  pip install firebase-admin  +  service account key JSON file.\n"
                "Free quota: 20,000 writes/day  (6 meters @ 30 s interval = ~17,000/day)."
            ),
            wraplength=640, justify="left",
        )
        note.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

        self._fb_enabled      = _bool_row(f, "Enable Firebase push", 1)
        self._fb_key_path     = _row(f, "Service account key file (.json):", 2, 36)
        self._fb_site_id      = _row(f, "Site ID  (e.g. site_01):",          3, 20)
        self._fb_interval     = _row(f, "Push interval (seconds):",          4, 8)
        self._fb_history      = _bool_row(f, "Write history sub-collection (uses more quota)", 5)

        # Browse button next to key path
        ttk.Button(
            f, text="Browse…",
            command=self._browse_key_file,
        ).grid(row=2, column=2, sticky="w", padx=(6, 0))

        btn_row = ttk.Frame(f)
        btn_row.grid(row=6, column=0, columnspan=3, sticky="w", pady=(10, 0))

        ttk.Button(btn_row, text="✔  Apply Firebase",
                   command=self._apply_firebase).pack(side="left")

        ttk.Button(btn_row, text="Test Connection",
                   style="Ghost.TButton",
                   command=self._test_firebase).pack(side="left", padx=(8, 0))

        status_row = ttk.Frame(f)
        status_row.grid(row=6, column=1, columnspan=2, sticky="e", pady=(10, 0))
        ttk.Label(status_row, text="Status:").pack(side="left", padx=(0, 6))
        self._lbl_fb_state = ttk.Label(status_row, text="DISABLED", width=14,
                                        relief="ridge", anchor="center")
        self._lbl_fb_state.pack(side="left")
        self._lbl_fb_err = ttk.Label(status_row, text="", foreground="red", wraplength=300)
        self._lbl_fb_err.pack(side="left", padx=(8, 0))

    # ── SMS / WhatsApp Alert section ─────────────────────────────────────────

    def _build_sms_section(self, parent) -> None:
        f = _section(parent, "SMS / WhatsApp Alerts  (critical alarm notification)")

        note = ttk.Label(
            f,
            text=(
                "Send an SMS or WhatsApp message when a new ALARM-severity fault fires.\n"
                "Providers: Twilio (global, WhatsApp), Dialog Axiata (LK), Mobitel/SLT (LK), Generic HTTP.\n"
                "Rate-limited per alarm code — one SMS per cooldown window (default 60 min).\n"
                "Twilio requires:  pip install twilio  |  Dialog/Mobitel: no extra packages."
            ),
            wraplength=640, justify="left",
        )
        note.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

        self._sms_enabled     = _bool_row(f, "Enable SMS / WhatsApp alerts", 1)
        self._sms_notify_clear = _bool_row(f, "Also send SMS when alarm clears (CLEAR notification)", 2)
        self._sms_notify_warn  = _bool_row(f, "Also send SMS for WARN-severity alarms (chatty — use with care)", 3)
        self._sms_cooldown    = _row(f, "Cooldown between repeat SMS for same alarm (min):", 4, 6)

        # Provider selector
        ttk.Label(f, text="Provider:", style="FieldLabel.TLabel").grid(
            row=5, column=0, sticky="w", padx=(0, 12), pady=4)
        self._sms_provider = tk.StringVar(value="twilio")
        prov_frame = ttk.Frame(f)
        prov_frame.grid(row=5, column=1, sticky="w")
        for prov_val, prov_lbl in (
            ("twilio",      "Twilio"),
            ("dialog",      "Dialog (LK)"),
            ("mobitel",     "Mobitel (LK)"),
            ("generic_http","Generic HTTP"),
        ):
            ttk.Radiobutton(prov_frame, text=prov_lbl,
                            variable=self._sms_provider,
                            value=prov_val).pack(side="left", padx=(0, 10))

        # Recipients
        ttk.Label(f, text="Recipients (one per line, E.164 format\n+94771234567):",
                  style="FieldLabel.TLabel").grid(
            row=6, column=0, sticky="nw", padx=(0, 12), pady=4)
        self._sms_recipients_txt = tk.Text(f, width=32, height=4, wrap="none")
        self._sms_recipients_txt.grid(row=6, column=1, sticky="w", pady=4)
        ttk.Label(
            f,
            text="WhatsApp (Twilio): prefix number with\nwhatsapp:  e.g. whatsapp:+94771234567",
            foreground="#888888", font=("Segoe UI", 8),
        ).grid(row=6, column=2, sticky="nw", padx=(8, 0))

        # Twilio credentials
        ttk.Separator(f, orient="horizontal").grid(
            row=7, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        ttk.Label(f, text="Twilio / Provider credentials",
                  font=("Segoe UI", 9, "bold")).grid(
            row=8, column=0, columnspan=3, sticky="w", pady=(0, 4))

        self._sms_account_sid  = _row(f, "Account SID  (or env:VAR_NAME):", 9,  30)
        self._sms_auth_token   = _row(f, "Auth Token   (or env:VAR_NAME):", 10, 30)
        self._sms_from_number  = _row(f, "From number  (+E.164 or whatsapp:+…):", 11, 24)
        self._sms_username     = _row(f, "Username (Dialog / Mobitel):", 12, 22)
        self._sms_password     = _row(f, "Password (Dialog / Mobitel):", 13, 22)
        self._sms_sender_id    = _row(f, "Sender ID / From name:", 14, 14)
        self._sms_api_url      = _row(f, "API URL (Generic HTTP / override):", 15, 40)

        # Test send
        ttk.Separator(f, orient="horizontal").grid(
            row=16, column=0, columnspan=3, sticky="ew", pady=(8, 4))
        test_row = ttk.Frame(f)
        test_row.grid(row=17, column=0, columnspan=3, sticky="w", pady=(0, 4))
        self._sms_test_number = tk.StringVar()
        ttk.Label(test_row, text="Test number:").pack(side="left", padx=(0, 6))
        ttk.Entry(test_row, textvariable=self._sms_test_number, width=18).pack(side="left")
        ttk.Button(test_row, text="Send Test SMS",
                   style="Ghost.TButton",
                   command=self._test_sms).pack(side="left", padx=(8, 0))
        self._lbl_sms_test = ttk.Label(test_row, text="")
        self._lbl_sms_test.pack(side="left", padx=(8, 0))

        # Apply + status
        btn_row = ttk.Frame(f)
        btn_row.grid(row=18, column=0, columnspan=3, sticky="w", pady=(10, 0))
        ttk.Button(btn_row, text="✔  Apply SMS Settings",
                   command=self._apply_sms).pack(side="left")

        status_row = ttk.Frame(f)
        status_row.grid(row=18, column=2, sticky="e", pady=(10, 0))
        ttk.Label(status_row, text="Status:").pack(side="left", padx=(0, 6))
        self._lbl_sms_state = ttk.Label(status_row, text="DISABLED", width=14,
                                         relief="ridge", anchor="center")
        self._lbl_sms_state.pack(side="left")
        self._lbl_sms_stat_detail = ttk.Label(status_row, text="", foreground="#888888")
        self._lbl_sms_stat_detail.pack(side="left", padx=(8, 0))

    def _browse_key_file(self) -> None:
        from tkinter import filedialog
        path = filedialog.askopenfilename(
            title="Select Firebase service account key",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if path:
            self._fb_key_path.set(path)

    # ── Load / Save ───────────────────────────────────────────────────────────

    def _load_from_cfg(self) -> None:
        """Populate form fields from config dict."""
        serial   = self.cfg.get("serial")    or {}
        mqtt     = self.cfg.get("mqtt")      or {}
        rest     = self.cfg.get("rest_api")  or {}
        sync     = self.cfg.get("remote_sync") or {}
        firebase = self.cfg.get("firebase")  or {}
        sms      = self.cfg.get("sms_alert") or {}
        sms_pc   = sms.get("provider_config") or {}

        # TCP
        is_tcp = str(serial.get("transport", "rtu")).lower() == "tcp"
        self._tcp_enabled.set(is_tcp)
        self._tcp_host.set(str(serial.get("tcp_host", "") or ""))
        self._tcp_port_var.set(str(serial.get("tcp_port", 502) or 502))

        # MQTT
        self._mqtt_enabled.set(bool(mqtt.get("enabled", False)))
        self._mqtt_broker.set(str(mqtt.get("broker_host", "localhost") or ""))
        self._mqtt_port_var.set(str(mqtt.get("broker_port", 1883)))
        self._mqtt_prefix.set(str(mqtt.get("topic_prefix", "power_monitor") or ""))
        self._mqtt_user.set(str(mqtt.get("username", "") or ""))
        self._mqtt_pass.set(str(mqtt.get("password", "") or ""))
        self._mqtt_qos_var.set(str(mqtt.get("qos", 1)))
        self._mqtt_retain.set(bool(mqtt.get("retain", False)))
        self._mqtt_interval.set(str(mqtt.get("publish_interval_sec", 5)))
        self._mqtt_tls.set(bool(mqtt.get("tls_enabled", False)))
        # Bandwidth mode
        self._mqtt_bw_mode.set(str(mqtt.get("bandwidth_mode", "normal")))
        self._mqtt_min_interval.set(str(mqtt.get("min_publish_interval_sec", 60)))
        self._mqtt_compress.set(bool(mqtt.get("compress_payload", False)))
        self._mqtt_summary_only.set(bool(mqtt.get("publish_summary_only", False)))

        # REST
        self._rest_enabled.set(bool(rest.get("enabled", False)))
        self._rest_host.set(str(rest.get("host", "127.0.0.1") or ""))
        self._rest_port_var.set(str(rest.get("port", 8080)))
        self._rest_api_key.set(str(rest.get("api_key", "") or ""))

        # Remote sync
        self._sync_enabled.set(bool(sync.get("enabled", True)))
        self._sync_max_queue.set(str(sync.get("max_queue", 300)))
        self._sync_replay_batch.set(str(sync.get("replay_batch", 5)))
        self._sync_replay_interval.set(str(sync.get("min_replay_interval_sec", 1.0)))

        # Firebase
        self._fb_enabled.set(bool(firebase.get("enabled", False)))
        self._fb_key_path.set(str(firebase.get("key_path", "firebase_key.json") or ""))
        self._fb_site_id.set(str(firebase.get("site_id", "site_01") or ""))
        self._fb_interval.set(str(firebase.get("push_interval_sec", 30)))
        self._fb_history.set(bool(firebase.get("enable_history", False)))

        # SMS / WhatsApp
        self._sms_enabled.set(bool(sms.get("enabled", False)))
        self._sms_notify_clear.set(bool(sms.get("notify_clear", False)))
        self._sms_notify_warn.set(bool(sms.get("notify_warn", False)))
        self._sms_cooldown.set(str(sms.get("cooldown_min", 60)))
        self._sms_provider.set(str(sms.get("provider", "twilio")))
        # Recipients: one per line
        recipients = sms.get("recipients") or []
        self._sms_recipients_txt.delete("1.0", "end")
        self._sms_recipients_txt.insert("1.0", "\n".join(str(r) for r in recipients))
        # Provider credentials
        self._sms_account_sid.set(str(sms_pc.get("account_sid", "") or ""))
        self._sms_auth_token.set(str(sms_pc.get("auth_token", "") or ""))
        self._sms_from_number.set(str(sms_pc.get("from_number", "") or ""))
        self._sms_username.set(str(sms_pc.get("username", "") or ""))
        self._sms_password.set(str(sms_pc.get("password", "") or ""))
        self._sms_sender_id.set(str(sms_pc.get("sender_id", "SCADA") or "SCADA"))
        self._sms_api_url.set(str(sms_pc.get("api_url", "") or ""))

    def _save_all(self) -> None:
        """Persist all sections to config without restarting services."""
        self._write_tcp_to_cfg()
        self._write_mqtt_to_cfg()
        self._write_rest_to_cfg()
        self._write_remote_sync_to_cfg()
        self._write_firebase_to_cfg()
        self._write_sms_to_cfg()
        self._persist_cfg()

    def _persist_cfg(self) -> None:
        if callable(self._save_cb):
            try:
                self._save_cb(self.cfg)
            except Exception as e:
                logger.warning("Could not save config: %s", e)

    # ── TCP apply ─────────────────────────────────────────────────────────────

    def _write_tcp_to_cfg(self) -> bool:
        serial = self.cfg.setdefault("serial", {})
        is_tcp = self._tcp_enabled.get()
        serial["transport"] = "tcp" if is_tcp else "rtu"
        if is_tcp:
            host = self._tcp_host.get().strip()
            if not host:
                self._lbl_tcp_status.configure(text="⚠ Host required", foreground="red")
                return False
            try:
                port = int(self._tcp_port_var.get() or 502)
                if not (1 <= port <= 65535):
                    raise ValueError
            except ValueError:
                self._lbl_tcp_status.configure(text="⚠ Invalid port", foreground="red")
                return False
            serial["tcp_host"] = host
            serial["tcp_port"] = port
        self._lbl_tcp_status.configure(text="✔ Saved", foreground="green")
        return True

    def _apply_tcp(self) -> None:
        if not self._write_tcp_to_cfg():
            return
        self._persist_cfg()
        # Trigger Modbus worker restart via the comm apply callback
        if callable(self._apply_comm):
            try:
                self._apply_comm()
                self._lbl_tcp_status.configure(text="✔ Reconnecting…", foreground="green")
            except Exception as e:
                self._lbl_tcp_status.configure(text=f"⚠ {e}", foreground="red")

    # ── MQTT apply ────────────────────────────────────────────────────────────

    def _write_mqtt_to_cfg(self) -> None:
        mqtt = self.cfg.setdefault("mqtt", {})
        try:
            p = int(self._mqtt_port_var.get() or 1883)
        except ValueError:
            p = 1883
        try:
            interval = float(self._mqtt_interval.get() or 5)
        except ValueError:
            interval = 5.0
        try:
            qos = int(self._mqtt_qos_var.get() or 1)
            qos = max(0, min(2, qos))
        except ValueError:
            qos = 1
        try:
            min_interval = float(self._mqtt_min_interval.get() or 60)
            min_interval = max(1.0, min_interval)
        except ValueError:
            min_interval = 60.0

        mqtt.update({
            "enabled":                   self._mqtt_enabled.get(),
            "broker_host":               self._mqtt_broker.get().strip(),
            "broker_port":               p,
            "topic_prefix":              self._mqtt_prefix.get().strip() or "power_monitor",
            "username":                  self._mqtt_user.get().strip(),
            "password":                  self._mqtt_pass.get().strip(),
            "qos":                       qos,
            "retain":                    self._mqtt_retain.get(),
            "publish_interval_sec":      interval,
            "tls_enabled":               self._mqtt_tls.get(),
            # Bandwidth mode
            "bandwidth_mode":            self._mqtt_bw_mode.get(),
            "min_publish_interval_sec":  min_interval,
            "compress_payload":          self._mqtt_compress.get(),
            "publish_summary_only":      self._mqtt_summary_only.get(),
        })

    def _apply_mqtt(self) -> None:
        self._write_mqtt_to_cfg()
        self._persist_cfg()
        if self._mqtt is not None:
            try:
                self._mqtt.reconfigure(self.cfg)
            except Exception as e:
                logger.warning("MQTT reconfigure error: %s", e)
        if self._remote_sync is not None:
            try:
                self._remote_sync.reconfigure(self.cfg)
            except Exception as e:
                logger.warning("Remote sync reconfigure error: %s", e)

    # ── REST apply ────────────────────────────────────────────────────────────

    def _write_rest_to_cfg(self) -> None:
        rest = self.cfg.setdefault("rest_api", {})
        try:
            p = int(self._rest_port_var.get() or 8080)
        except ValueError:
            p = 8080
        rest.update({
            "enabled": self._rest_enabled.get(),
            "host":    self._rest_host.get().strip() or "127.0.0.1",
            "port":    p,
            "api_key": self._rest_api_key.get().strip(),
        })

    def _apply_rest(self) -> None:
        self._write_rest_to_cfg()
        self._persist_cfg()
        if self._rest is not None:
            try:
                self._rest.reconfigure(self.cfg)
            except Exception as e:
                logger.warning("REST API reconfigure error: %s", e)
        if self._remote_sync is not None:
            try:
                self._remote_sync.reconfigure(self.cfg)
            except Exception as e:
                logger.warning("Remote sync reconfigure error: %s", e)

    # Remote sync apply

    def _write_remote_sync_to_cfg(self) -> None:
        sync = self.cfg.setdefault("remote_sync", {})
        try:
            max_queue = max(10, int(self._sync_max_queue.get() or 300))
        except ValueError:
            max_queue = 300
        try:
            replay_batch = max(1, int(self._sync_replay_batch.get() or 5))
        except ValueError:
            replay_batch = 5
        try:
            replay_interval = max(0.2, float(self._sync_replay_interval.get() or 1.0))
        except ValueError:
            replay_interval = 1.0
        sync.update({
            "enabled": self._sync_enabled.get(),
            "max_queue": max_queue,
            "replay_batch": replay_batch,
            "min_replay_interval_sec": replay_interval,
        })

    def _apply_remote_sync(self) -> None:
        self._write_remote_sync_to_cfg()
        self._persist_cfg()
        if self._remote_sync is not None:
            try:
                self._remote_sync.reconfigure(self.cfg)
            except Exception as e:
                logger.warning("Remote sync reconfigure error: %s", e)
        self._update_remote_sync_status()

    def _clear_remote_sync_queue(self) -> None:
        if self._remote_sync is None:
            return
        try:
            self._remote_sync.clear_queue()
        except Exception as e:
            logger.warning("Remote sync queue clear error: %s", e)
        self._update_remote_sync_status()

    # ── Firebase apply ────────────────────────────────────────────────────────

    def _write_firebase_to_cfg(self) -> None:
        fb = self.cfg.setdefault("firebase", {})
        try:
            interval = float(self._fb_interval.get() or 30)
            interval = max(5.0, interval)   # minimum 5 s to protect quota
        except ValueError:
            interval = 30.0
        fb.update({
            "enabled":           self._fb_enabled.get(),
            "key_path":          self._fb_key_path.get().strip(),
            "site_id":           self._fb_site_id.get().strip() or "site_01",
            "push_interval_sec": interval,
            "enable_history":    self._fb_history.get(),
        })

    def _apply_firebase(self) -> None:
        self._write_firebase_to_cfg()
        self._persist_cfg()
        self._lbl_fb_state.configure(text="SAVED", background="#27ae60", foreground="white")
        self._lbl_fb_err.configure(
            text="Restart the app for Firebase changes to take effect.",
            foreground="orange",
        )

    def _test_firebase(self) -> None:
        """Quick connection test — tries to init firebase-admin and write a test doc."""
        import threading
        self._lbl_fb_state.configure(text="TESTING…", background="#e67e22", foreground="white")
        self._lbl_fb_err.configure(text="", foreground="red")

        key_path = self._fb_key_path.get().strip()
        site_id  = self._fb_site_id.get().strip() or "site_01"

        def _run():
            try:
                import os
                import json
                import firebase_admin
                from firebase_admin import credentials, firestore

                if not key_path:
                    raise ValueError("Key file path is empty.")
                if not os.path.isfile(key_path):
                    raise FileNotFoundError(f"Key file not found:\n{key_path}")
                with open(key_path, "r", encoding="utf-8") as fh:
                    key_data = json.load(fh)
                project = key_data.get("project_id", "unknown")

                # Use a separate app name to avoid conflicts with running publisher
                app_name = "_connectivity_test"
                try:
                    app = firebase_admin.get_app(app_name)
                except ValueError:
                    cred = credentials.Certificate(key_path)
                    app  = firebase_admin.initialize_app(cred, name=app_name)

                db = firestore.client(app=app)
                db.collection("sites").document(site_id).set(
                    {"_test": True, "ts": firestore.SERVER_TIMESTAMP},
                    merge=True,
                )
                msg = f"OK — project: {project}\nWrote test doc to sites/{site_id}"
                self.after(0, lambda: self._fb_test_result(True, msg))
            except ImportError:
                self.after(0, lambda: self._fb_test_result(
                    False, "firebase-admin not installed.\nRun:  pip install firebase-admin"))
            except Exception as e:
                self.after(0, lambda: self._fb_test_result(False, str(e)))

        threading.Thread(target=_run, daemon=True).start()

    def _fb_test_result(self, ok: bool, msg: str) -> None:
        if ok:
            self._lbl_fb_state.configure(text="OK", background="#27ae60", foreground="white")
            self._lbl_fb_err.configure(text=msg, foreground="#27ae60")
        else:
            self._lbl_fb_state.configure(text="FAILED", background="#c0392b", foreground="white")
            self._lbl_fb_err.configure(text=msg, foreground="red")

    # ── Status updates (called from UI tick) ──────────────────────────────────

    _MQTT_COLORS = {
        "CONNECTED":   "#27ae60",
        "CONNECTING":  "#e67e22",
        "ERROR":       "#c0392b",
        "DISABLED":    "#7f8c8d",
        "UNAVAILABLE": "#c0392b",
    }

    def update_view(self) -> None:
        """Refresh MQTT, REST, Firebase and SMS status indicators."""
        self._update_mqtt_status()
        self._update_rest_status()
        self._update_remote_sync_status()
        self._update_firebase_status()
        self._update_sms_status()

    def _update_mqtt_status(self) -> None:
        if self._mqtt is None:
            return
        try:
            state = self._mqtt.status
            color = self._MQTT_COLORS.get(state, "#7f8c8d")
            self._lbl_mqtt_state.configure(text=state)
            try:
                self._lbl_mqtt_state.configure(background=color, foreground="white")
            except Exception:
                pass
            err = self._mqtt.last_error or ""
            self._lbl_mqtt_err.configure(text=(err[:60] + "…") if len(err) > 60 else err)
        except Exception:
            pass

    def _update_rest_status(self) -> None:
        if self._rest is None:
            return
        try:
            if self._rest.is_running:
                state = f"RUNNING  :{(self._cfg_rest_port())}"
                color = "#27ae60"
            elif (self.cfg.get("rest_api") or {}).get("enabled", False):
                state = "FAILED"
                color = "#c0392b"
            else:
                state = "DISABLED"
                color = "#7f8c8d"
            self._lbl_rest_state.configure(text=state)
            try:
                self._lbl_rest_state.configure(background=color, foreground="white")
            except Exception:
                pass
        except Exception:
            pass

    def _update_remote_sync_status(self) -> None:
        sync = self._remote_sync
        try:
            if sync is None:
                self._lbl_sync_state.configure(text="UNCONFIGURED", background="#7f8c8d", foreground="white")
                return
            health = sync.health() if hasattr(sync, "health") else {}
            enabled = bool(health.get("enabled", False))
            queue_depth = int(health.get("queue_depth", 0) or 0)
            max_queue = int(health.get("max_queue", 0) or 0)
            mqtt_state = str(health.get("mqtt_state", "UNKNOWN") or "UNKNOWN").upper()
            if not enabled:
                state = "QUEUE OFF"
                color = "#7f8c8d"
            elif queue_depth > 0 and mqtt_state != "CONNECTED":
                state = "BUFFERING"
                color = "#e67e22"
            elif queue_depth > 0:
                state = "REPLAYING"
                color = "#3498db"
            else:
                state = "READY"
                color = "#27ae60"
            self._lbl_sync_state.configure(text=state, background=color, foreground="white")
            self._lbl_sync_queue.configure(
                text=f"Queue: {queue_depth}/{max_queue}   MQTT: {mqtt_state}   REST: {health.get('rest_state', 'UNKNOWN')}"
            )
            self._lbl_sync_counts.configure(
                text=(
                    f"Sent: {int(health.get('sent_count', 0) or 0)}   "
                    f"Replayed: {int(health.get('replayed_count', 0) or 0)}   "
                    f"Failed: {int(health.get('failed_count', 0) or 0)}   "
                    f"Dropped: {int(health.get('dropped_count', 0) or 0)}"
                )
            )
            last_success = float(health.get("last_success_ts", 0.0) or 0.0)
            last_rest = float(health.get("last_rest_update_ts", 0.0) or 0.0)
            last = max(last_success, last_rest)
            if last > 0:
                age = max(0, int(time.time() - last))
                self._lbl_sync_last.configure(text=f"Last sync/local update: {age}s ago")
            else:
                self._lbl_sync_last.configure(text="Last sync/local update: -")
            err = str(health.get("last_error", "") or "")
            self._lbl_sync_err.configure(text=(err[:90] + "...") if len(err) > 90 else err)
        except Exception:
            pass

    # ── SMS apply ─────────────────────────────────────────────────────────────

    def _write_sms_to_cfg(self) -> None:
        sms = self.cfg.setdefault("sms_alert", {})
        try:
            cooldown = max(1.0, float(self._sms_cooldown.get() or 60))
        except ValueError:
            cooldown = 60.0

        # Parse recipients from text widget (one per line, strip blanks)
        raw = self._sms_recipients_txt.get("1.0", "end")
        recipients = [r.strip() for r in raw.splitlines() if r.strip()]

        sms.update({
            "enabled":          self._sms_enabled.get(),
            "notify_clear":     self._sms_notify_clear.get(),
            "notify_warn":      self._sms_notify_warn.get(),
            "cooldown_min":     cooldown,
            "provider":         self._sms_provider.get(),
            "recipients":       recipients,
        })
        pc = sms.setdefault("provider_config", {})
        pc.update({
            "account_sid":  self._sms_account_sid.get().strip(),
            "auth_token":   self._sms_auth_token.get().strip(),
            "from_number":  self._sms_from_number.get().strip(),
            "username":     self._sms_username.get().strip(),
            "password":     self._sms_password.get().strip(),
            "sender_id":    self._sms_sender_id.get().strip() or "SCADA",
            "api_url":      self._sms_api_url.get().strip(),
        })

    def _apply_sms(self) -> None:
        self._write_sms_to_cfg()
        self._persist_cfg()
        if self._sms_service is not None:
            try:
                self._sms_service.reconfigure(self.cfg)
            except Exception as e:
                logger.warning("SMS service reconfigure error: %s", e)
        self._update_sms_status()

    def _test_sms(self) -> None:
        """Send a test SMS synchronously in a thread, update result label."""
        to = self._sms_test_number.get().strip()
        if not to:
            self._lbl_sms_test.configure(text="Enter a number first", foreground="red")
            return
        # Write current form fields to cfg first so test uses latest settings
        self._write_sms_to_cfg()

        if self._sms_service is None:
            self._lbl_sms_test.configure(text="SMS service not attached", foreground="red")
            return

        self._lbl_sms_test.configure(text="Sending…", foreground="#e67e22")
        self._sms_service.reconfigure(self.cfg)

        def _run():
            err = self._sms_service.test_send(to)
            def _done():
                if err:
                    self._lbl_sms_test.configure(text=f"Failed: {err[:60]}", foreground="red")
                else:
                    self._lbl_sms_test.configure(text="Sent OK ✔", foreground="#27ae60")
            try:
                self.after(0, _done)
            except Exception:
                pass
        import threading as _thr
        _thr.Thread(target=_run, daemon=True, name="sms-test").start()

    def _update_sms_status(self) -> None:
        svc = self._sms_service
        sms_cfg = self.cfg.get("sms_alert") or {}
        enabled = bool(sms_cfg.get("enabled", False))
        try:
            if not enabled:
                self._lbl_sms_state.configure(text="DISABLED")
                try:
                    self._lbl_sms_state.configure(background="#7f8c8d", foreground="white")
                except Exception:
                    pass
                self._lbl_sms_stat_detail.configure(text="")
                return

            if svc is None:
                self._lbl_sms_state.configure(text="NO SERVICE")
                try:
                    self._lbl_sms_state.configure(background="#c0392b", foreground="white")
                except Exception:
                    pass
                return

            provider = str(svc.provider).upper()
            sent     = int(svc.sent_count)
            fails    = int(svc.fail_count)
            last_err = str(svc.last_error or "")

            # Twilio availability check
            if svc.provider == "twilio" and not svc.is_twilio_available:
                self._lbl_sms_state.configure(text="NO TWILIO")
                try:
                    self._lbl_sms_state.configure(background="#c0392b", foreground="white")
                except Exception:
                    pass
                self._lbl_sms_stat_detail.configure(
                    text="pip install twilio", foreground="red")
                return

            if fails > 0 and sent == 0:
                state, color = "ERROR", "#c0392b"
            elif fails > 0:
                state, color = "PARTIAL", "#e67e22"
            else:
                state, color = "READY", "#27ae60"

            self._lbl_sms_state.configure(text=state)
            try:
                self._lbl_sms_state.configure(background=color, foreground="white")
            except Exception:
                pass

            detail = f"{provider}  Sent:{sent}  Fail:{fails}"
            if last_err:
                detail += f"  Err:{last_err[:40]}"
            self._lbl_sms_stat_detail.configure(
                text=detail, foreground="#888888" if fails == 0 else "red")
        except Exception:
            pass

    _FB_COLORS = {
        "RUNNING":   "#27ae60",
        "DISABLED":  "#7f8c8d",
        "ERROR":     "#c0392b",
        "SAVED":     "#27ae60",
        "TESTING…":  "#e67e22",
        "FAILED":    "#c0392b",
        "OK":        "#27ae60",
    }

    def _update_firebase_status(self) -> None:
        fb_cfg = (self.cfg.get("firebase") or {})
        enabled = bool(fb_cfg.get("enabled", False))
        if not enabled:
            try:
                self._lbl_fb_state.configure(text="DISABLED", background="#7f8c8d", foreground="white")
            except Exception:
                pass
            return
        # If publisher is accessible via main_window, reflect its state
        try:
            pub = getattr(self, "_fb_publisher", None)
            if pub is not None:
                state = str(getattr(pub, "status", "RUNNING")).upper()
                color = self._FB_COLORS.get(state, "#7f8c8d")
                self._lbl_fb_state.configure(text=state, background=color, foreground="white")
                err = getattr(pub, "last_error", "") or ""
                self._lbl_fb_err.configure(
                    text=(err[:80] + "…") if len(err) > 80 else err,
                    foreground="red",
                )
        except Exception:
            pass

    def set_firebase_publisher(self, pub) -> None:
        self._fb_publisher = pub

    def _cfg_rest_port(self) -> int:
        try:
            return int((self.cfg.get("rest_api") or {}).get("port", 8080))
        except Exception:
            return 8080

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def set_mqtt_publisher(self, pub) -> None:
        self._mqtt = pub

    def set_rest_api(self, api) -> None:
        self._rest = api

    def set_remote_sync(self, sync) -> None:
        self._remote_sync = sync

    def set_sms_service(self, svc) -> None:
        self._sms_service = svc
