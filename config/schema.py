# config/schema.py

CONFIG_SCHEMA_VERSION = 2


def default_config() -> dict:
    return {
        # Incremented whenever a breaking key is added or renamed.
        # Used by load_config() to trigger migrations.
        "schema_version": CONFIG_SCHEMA_VERSION,

        # Empty string = no password protection on setup tabs (default).
        # Set a non-empty value to require a password before unlocking setup.
        # NOTE: stored as plaintext in config.json (P1: add hashing).
        # NEVER ship with "1000" or any other hardcoded default.
        "setup_write_password": "",

        # Single storage root (optional): if set, all logs/reports/snapshots/email DB
        # will live under this folder. If blank, defaults to %APPDATA%\PowerMonitoringReporting.
        "paths": {
            "base_dir": "",
        },

        "ui": {
            # dark = control-room default, light = bright office
            "theme": "dark",
            # Privacy mode is ALWAYS ON for release builds.
            # UI should not display full paths, site identity, emails, etc.
            "privacy_mode": True,
            # Chart performance cap (points drawn per series).
            "max_plot_points": 1000,
        },

        # Site identity is optional. Keep defaults generic/blank.
        "site": {
            "plant_name": "Power Monitoring & Reporting",
            "location": "",
            "description": "",
            # Nominal phase-to-neutral voltage [V] for CT/PT sanity and
            # UV/OV alarm reference.  Sri Lanka standard: 230 V (LV) per
            # LECO/CEB distribution regulations.  Set higher for MV sites
            # with a PT (e.g. 11000 for 11 kV / PT ratio 47.83).
            "nominal_vln": 230.0,
            # Country / grid standard (informational, used for tariff defaults)
            "country": "LK",
        },

        "serial": {
            # Transport: "rtu" = RS-485 serial (default); "tcp" = Modbus TCP gateway
            "transport": "rtu",
            # Modbus TCP gateway host + port (only used when transport = "tcp")
            "tcp_host": "",
            "tcp_port": 502,
            "port": "",
            "baud": 9600,
            "parity": "N",
            "stopbits": 1,
            "word_order": "CDAB",
            "timeout_ms": 800,
            "reconnect": True,
            # Modbus register base address offset.
            # 0 = standard (MFM384 default, IEC 61158).
            # 1 = some older Schneider/ABB devices that use address+1 convention.
            # Never change this unless the device datasheet explicitly requires it.
            "base_address": 0,

            # Robust comms (field-ready)
            "auto_connect": False,
            # If no GOOD read within this time, UI marks STALE (data quality)
            "stale_seconds": 5.0,
            # If no GOOD bus activity within this time, force reconnect (watchdog)
            "watchdog_seconds": 12.0,
            # After N consecutive bus failures, force reconnect
            "bus_reconnect_threshold": 3,
            "backoff_initial_sec": 1.0,
            "backoff_max_sec": 30.0,
            "backoff_jitter": 0.15,
            "port_check_interval_sec": 2.0,
            "latency_warn_ms": 1000.0,
            # Default auto-scan upper slave address. Keep small for site startup;
            # engineers can raise this for larger RS485 segments.
            "scan_max_slave_id": 3,
            # Per-meter retry attempts before marking failure
            "meter_retry_count": 2,
            "timeout": 1.0,
        },

        # Unified data quality (SCADA-grade).
        # UI, protection, alarms and logging should respect these.
        "quality": {
            "stale_after_s": 5.0,
            "offline_after_s": 20.0,
        },

        # Data accuracy guardrails. Limits are intentionally broad by default
        # so CT/PT-scaled LV/MV systems are not rejected accidentally.
        "data_quality": {
            "enabled": True,
            "reject_non_finite": True,
            "max_abs_voltage_v": 1500000.0,
            "max_abs_current_a": 200000.0,
            "max_abs_power_kw": 1000000.0,
            "max_abs_energy_kwh": 10000000000.0,
            "frequency_min_hz": 40.0,
            "frequency_max_hz": 70.0,
            "pf_abs_max": 1.05,
            "clock_jump_warn_s": 5.0,
            # CT/PT sanity check: set ctpt_sanity_enabled = False to suppress
            # the ratio-plausibility warnings (e.g. HV sites with wide voltage swings).
            "ctpt_sanity_enabled": True,
            # Historian retention tiers.
            # hot_days  : keep full-resolution rows.  Default 30 days.
            # warm_days : keep hourly-average rows after hot.  Default 180 days.
            # Rows older than warm_days are deleted outright.
            # CSV day-folders are retained for max(hot_days, warm_days) days.
            "retention": {
                "hot_days":  30,
                "warm_days": 180,
            },
        },

        # Live plant analytics. Defaults match Sri Lankan grid standards:
        #   Voltage: CEB/LECO distribution code ±10% of 230 V (207–253 V)
        #   Frequency: SL Grid Code 2017 ±1 Hz normal, ±0.5 Hz tight
        #   PF: PUCSL recommended minimum 0.90 lagging for industrial consumers
        "analytics": {
            "enabled": True,
            "forecast_horizon_min": 15.0,
            # LV phase-to-neutral band: CEB distribution code ±10% of 230 V
            # Raise voltage_ln_min_v / max_v for MV/HV metering via PT.
            "voltage_ln_min_v": 207.0,   # 230 × 0.90
            "voltage_ln_max_v": 253.0,   # 230 × 1.10
            "voltage_ll_min_v": 360.0,   # 400 × 0.90
            "voltage_ll_max_v": 440.0,   # 400 × 1.10
            # SL Grid Code 2017 section 4: normal operating band ±1 Hz
            # Use ±0.5 Hz for stricter process monitoring.
            "freq_min_hz": 49.0,
            "freq_max_hz": 51.0,
            # PF alarm threshold — PUCSL industrial tariff: surcharge below 0.85
            # Warn at 0.90 to give headroom before penalty zone.
            "pf_warn": 0.90,
            "pf_trip": 0.85,            # trip / billing-penalty level
            "pf_target": 0.95,
            # CEB distribution code: 2% unbalance warn, 3% alarm
            "voltage_unbalance_warn_pct": 2.0,
            "voltage_unbalance_alarm_pct": 3.0,
            "thd_v_warn_pct": 5.0,
            "thd_i_warn_pct": 8.0,
            "deduct_ratio_warn_pct": 15.0,
            "single_meter_share_warn_pct": 85.0,
        },

        # Explainable predictive-maintenance intelligence.
        # These are warning thresholds, not trip limits.
        "intelligence": {
            "enabled": True,
            "comm_latency_warn_ms": 900.0,
            "poll_fail_rate_warn_pct": 8.0,
            "meter_fail_rate_warn_pct": 12.0,
            "meter_consecutive_fail_warn": 3,
            "current_unbalance_warn_pct": 20.0,
            "voltage_unbalance_warn_pct": 2.5,
            "pf_warn": 0.90,
            "thd_v_warn_pct": 5.0,
            "thd_i_warn_pct": 10.0,
            "forecast_rise_warn_pct": 20.0,
            "kw_ramp_warn_per_min": 5.0,
            "remote_queue_warn_pct": 50.0,
        },

        "meters": [],

        # Optional advanced TOTAL math overrides.
        # If enabled, you can define per-key per-meter sign overrides.
        # Example:
        #  "total_math": {
        #     "enabled": True,
        #     "per_key": {"kW": {"1": "+", "2": "-"}}
        #  }
        "total_math": {
            "enabled": False,
            "per_key": {}
        },

        # Set to True after the first-run wizard completes.
        # Wizard will not re-open once this is True.
        "setup_complete": False,

        "auto_connect": False,
        "auto_scan_meters": True,
        "auto_start_logging": False,

        "alarms": {
            "enabled": True,
            "stale_sec": 10,
            "startup_inhibit_sec": 10,
            "beep_critical": False
        },

        "logging": {
            "enabled": False,
            "interval_sec": 10,
            "folder": "",
            "keys": [],
            "sources": {},
            # Keep daily log folders for this many days then auto-delete.
            "retain_days": 90,
            # Storage backend:
            #   "sqlite" — SQLite historian only (default; fast queries, compact)
            #   "csv"    — CSV flat-files only   (legacy; YYYY-MM-DD subfolders)
            #   "both"   — write to both         (use during migration)
            "backend": "sqlite",
        },

        "reports": {
            "facility_name": "",
            "facility_code": "",
            "auto_save_dir": "",  # if blank -> base_dir/reports (or %APPDATA%/PowerMonitoringReporting/reports)
            "pdf_footer": "",
            "template": "default",
            "include_keys": []
        },

        "email": {
            "enabled": False,
            "smtp": {},
            "daily": {},
            "alarm": {},
            "recipients": []
        },

        # ── Solar Plant KPI  (IEC 61724-1:2017) ───────────────────────────
        # Leave dc_capacity_kwp = 0.0 until the operator configures the plant.
        # The KPI tab is always visible but shows "configure plant" message
        # until a non-zero capacity is entered.
        "solar_plant": {
            # Installed DC nameplate capacity [kWp]
            # = sum of all module Wp ratings at STC ÷ 1000
            "dc_capacity_kwp": 0.0,

            # Inverter rated AC output [kW]  (0 = use dc_capacity as proxy)
            "ac_capacity_kw": 0.0,

            # Design Peak Sun Hours [h/day]  — from PVGIS/NASA SSE or pyranometer
            # Sri Lanka typical: 4.5–5.5 h/day (Western/Southern provinces ~4.8,
            # North Central / Northern provinces ~5.2–5.5)
            "psh_design": 4.8,

            # Design Performance Ratio [0–1]
            # Accounts for temperature, soiling, wiring, inverter, mismatch losses
            # Typical crystalline-Si: 0.75–0.82
            "pr_design": 0.78,

            # Today's measured in-plane irradiation H_POA [kWh/m²]
            # 0.0 = not available; PR computed from PSH_design (estimated)
            "irradiance_kwh_m2": 0.0,

            # CO₂ displacement factor [kg CO₂/kWh]
            # Source: PUCSL Sri Lanka Grid Emission Factor 2023 — 0.7306 kg CO₂/kWh
            # (weighted average of CEB hydro + thermal generation mix)
            # Update annually from PUCSL annual performance report.
            "emission_factor_kg_kwh": 0.7306,

            # Annual energy generation target [kWh/year]
            # 0 = auto-compute as dc_capacity × psh_design × pr_design × 365
            "annual_target_kwh": 0.0,

            # Meter register keys (matched to TOTAL aggregator output keys)
            "energy_key": "Today_kWh",   # Today_kWh | Import_kWh | Net_kWh
            "power_key":  "kW",           # kW | Total_kW
        },

        # ── Small Hydro Plant KPI  (IEC 60041 / CBIP) ─────────────────────
        # Leave rated_capacity_kw = 0.0 until the operator configures the plant.
        "hydro_plant": {
            # Generator nameplate rating at generator terminals [kW]
            "rated_capacity_kw": 0.0,

            # Design net head [m]  (H_gross - penstock/valve losses)
            # Source: project DPR or model acceptance test report
            "design_head_m": 0.0,

            # Design discharge at rated conditions [m³/s]
            "design_flow_m3_s": 0.0,

            # Turbine technology: PELTON | FRANCIS | KAPLAN | CROSS_FLOW | TURGO
            "turbine_type": "FRANCIS",

            # Design Plant Load Factor [%]
            # = E_annual / (P_rated × 8760) × 100
            # Source: project DPR.
            # Typical Sri Lanka run-of-river small hydro: 35–55%
            # (monsoon-fed rivers: higher May–Nov, lower Jan–Apr)
            "design_plf_pct": 45.0,

            # Annual generation target [kWh/year]
            # 0 = auto-compute from rated_capacity × design_plf × 8760
            "design_annual_kwh": 0.0,

            # Minimum power for "unit is running" [kW]
            # 0 = auto (3% of rated_capacity_kw)
            "min_operating_kw": 0.0,

            # Water density [kg/m³].  Fresh water = 1000 (IEC 60041 default)
            # Silt-laden rivers: 1005–1050 (measure with hydrometer)
            "water_density_kg_m3": 1000.0,

            # Today's live operational inputs (updated by operator or sensor)
            "flow_m3_s":  0.0,   # Average discharge today [m³/s]
            "head_net_m": 0.0,   # Net head today [m]; 0 = use design_head_m

            # CO₂ factor [kg CO₂/kWh]  — PUCSL Sri Lanka 2023
            "emission_factor_kg_kwh": 0.7306,

            # Register keys
            "energy_key": "Today_kWh",
            "power_key":  "kW",
        },

        # ── Connectivity — MQTT Publisher  (OASIS MQTT v3.1.1) ──────────────
        # Set enabled = True and configure broker to start publishing live readings.
        "mqtt": {
            "enabled":              False,
            "broker_host":          "localhost",
            "broker_port":          1883,
            # Leave blank to auto-generate a unique client ID at runtime.
            "client_id":            "",
            "username":             "",
            "password":             "",
            "password_env":         "",
            "password_keyring_service": "",
            "password_keyring_username": "",
            # Topic root — actual topics are {prefix}/live/{source}/{key}
            "topic_prefix":         "power_monitor",
            # How often to publish a snapshot (seconds).  Minimum effective: 1s.
            "publish_interval_sec": 5,
            # QoS 0 = fire-and-forget, 1 = at-least-once (recommended), 2 = exactly-once
            "qos":                  1,
            # Retain the last message so new subscribers get current values immediately.
            "retain":               False,
            # TLS (optional).  Set tls_ca_cert to a PEM file path if the broker
            # uses a private CA.  Leave blank to use the system trust store.
            "tls_enabled":          False,
            "tls_ca_cert":          "",

            # ── Bandwidth / GPRS-4G mode ──────────────────────────────────────
            # Enable when the SCADA host connects via a metered mobile data link
            # (Dialog 4G, Mobitel, SLT CDMA, etc.) to reduce data usage and
            # avoid throttling on prepaid SIMs.
            #
            # bandwidth_mode:
            #   "normal"  — standard operation, publish_interval_sec respected.
            #   "gprs"    — applies minimum interval floor + payload compression.
            #               Effective interval = max(publish_interval_sec,
            #                                        min_publish_interval_sec).
            #
            # min_publish_interval_sec: hard floor on publish interval in "gprs"
            #   mode.  Default 60 s (one publish per minute) is practical for
            #   most prepaid 4G plans (Dialog 1 GB/month @ 60 s ≈ 150 MB/month
            #   for 4 meters).  Set lower only if the plan allows it.
            #
            # compress_payload: zlib-compress the summary JSON before publishing.
            #   Reduces per-message size ~65–75% (JSON compresses well).
            #   Broker and all subscribers must handle binary payloads.
            #   Leave False unless you control the full subscriber stack.
            #   Per-parameter topics are NOT compressed (too small to benefit).
            #   Only the /summary and /alarms topics are compressed.
            #
            # publish_summary_only: skip per-parameter /live/{src}/{key} topics;
            #   publish only the compact /summary topic.  Reduces message count
            #   from N_params × N_meters + 1 down to 1 per interval — the single
            #   biggest bandwidth reduction available short of compression.
            "bandwidth_mode":            "normal",
            "min_publish_interval_sec":  60,
            "compress_payload":          False,
            "publish_summary_only":      False,
        },

        # ── Connectivity — REST API  (HTTP/JSON pull endpoint) ────────────────
        # Exposes live readings on http://<host>:<port>/api/v1/readings
        # Bind to 127.0.0.1 for local-only; 0.0.0.0 to allow LAN access.
        "rest_api": {
            "enabled":  False,
            "host":     "127.0.0.1",
            "port":     8080,
            # Leave blank to disable authentication.
            # Never pass via query string — use X-API-Key header only.
            "api_key":  "",
            "api_key_env": "",
            "api_key_keyring_service": "",
            "api_key_keyring_username": "",
            # CORS allowed origin.  Use "*" only on trusted isolated LANs.
            # Default is localhost-only.  Set to "http://192.168.x.x" for LAN dashboards.
            "cors_origin": "http://127.0.0.1",
        },

        # Offline-first remote monitoring bridge.
        # REST keeps the latest local snapshot. MQTT snapshots are queued on
        # disk when the broker/internet is unavailable and replayed later.
        "remote_sync": {
            "enabled": True,
            "max_queue": 300,
            "replay_batch": 5,
            "min_replay_interval_sec": 1.0,
        },

        # ── Firebase Cloud Sync ──────────────────────────────────────────────
        # Set enabled = True and place firebase_key.json next to app.py.
        # Generate key: Firebase console -> Project Settings -> Service accounts
        #               -> Generate new private key -> save as firebase_key.json
        "firebase": {
            "enabled":           False,
            # Firestore document ID for this site. Use a short slug, no spaces.
            # e.g. "plant_01", "kumburutheniwela_hydro", "factory_main"
            "site_id":           "site_01",
            # Path to service account key JSON file.
            # Relative paths are resolved from the app.py directory.
            "key_path":          "firebase_key.json",
            "key_path_env":      "",
            "key_path_keyring_service": "",
            "key_path_keyring_username": "",
            # How often to push a reading per meter (seconds). Min 10s.
            # At 30s: 6 meters = ~17k writes/day — within Spark free plan (20k/day).
            "push_interval_sec": 30,
            # Also write to history sub-collection (uses more write quota).
            "enable_history":    False,
        },

        # ── Industrial Load KPI  (PUCSL / CEB Tariff Structure) ────────────────
        # Defaults reflect Sri Lanka CEB industrial tariff (General Purpose —
        # Three Phase, as at 2024 gazette revision).  Update rates each tariff
        # revision; structure (demand + energy + PF surcharge) is stable.
        # Leave contract_demand_kva = 0.0 until the operator configures the site.
        # The KPI tab is always visible; billing estimates require tariff rates.
        "load_plant": {
            # Sanctioned / contracted demand [kVA] as per CEB agreement letter.
            # 0 = not set (MD utilisation % will not be computed)
            "contract_demand_kva": 0.0,

            # Demand measurement window [min].
            # PUCSL Metering Code: 15 min standard for LV industrial consumers.
            "demand_interval_min": 15,

            # PF thresholds (PUCSL electricity tariff order):
            # PF below pf_penalty_threshold  → surcharge applies (typically 10–15%)
            # PF at/above pf_incentive_threshold → rebate applies
            "pf_penalty_threshold":   0.85,
            "pf_incentive_threshold": 0.95,

            # CEB tariff rates — General Purpose Three Phase (update per gazette)
            # Demand rate:  LKR/kVA/month  (billed on monthly MD peak)
            # Energy rates: LKR/kWh per block
            # Set to 0.0 until operator enters site-specific rates.
            "tariff_demand_rate":       0.0,   # LKR/kVA/month
            "tariff_energy_rate":       0.0,   # LKR/kWh (flat or block 1)
            # Optional block tariff (0 = disabled, uses tariff_energy_rate flat)
            "tariff_block2_kwh":        0.0,   # kWh threshold for block 2
            "tariff_block2_rate":       0.0,   # LKR/kWh for block 2
            # Fixed monthly charge [LKR] (service charge / fixed charge per CEB bill)
            "tariff_fixed_charge":      0.0,

            # Meter register keys (matched to TOTAL aggregator output keys)
            "power_key":          "kW",        # active power
            "apparent_power_key": "kVA",       # apparent power (for MD and PF)
            "pf_key":             "PF",        # displacement power factor
            "energy_key":         "Today_kWh",

            # ── CEB Time-of-Use (TOU) / Evening Peak schedule ──────────────────
            # CEB General Purpose Three Phase tariff: evening peak surcharge period.
            # Source: CEB Tariff Revision Gazette — typical peak window 18:30–22:30.
            # Set tou_enabled = True and configure peak/off-peak rates per gazette.
            #
            # tou_peak_start / tou_peak_end — 24-h "HH:MM" strings (local time).
            #   Overnight wrap is supported (e.g. "22:00" start, "06:00" end).
            # tou_peak_rate_lkr_kwh  — energy charge during peak window [LKR/kWh].
            # tou_offpeak_rate_lkr_kwh — energy charge outside peak window [LKR/kWh].
            #   Set both to 0.0 until operator enters site-specific gazette rates.
            # tou_peak_demand_rate — additional demand charge for peak-period MD
            #   [LKR/kVA/month].  0 = not applicable for this tariff.
            "tou_enabled":              False,
            "tou_peak_start":           "18:30",   # CEB default evening peak start
            "tou_peak_end":             "22:30",   # CEB default evening peak end
            "tou_peak_rate_lkr_kwh":    0.0,       # LKR/kWh during peak window
            "tou_offpeak_rate_lkr_kwh": 0.0,       # LKR/kWh outside peak window
            "tou_peak_demand_rate":     0.0,       # LKR/kVA/month for peak-period MD
        },

        # ── SMS / WhatsApp Alert Service ───────────────────────────────────────
        # Sends a text message when a new ALARM-severity alarm fires.
        # Only ALARM severity is sent by default (WARN is too noisy for SMS).
        # Rate-limited per (meter_id, code) to avoid SMS flooding during a
        # sustained fault (e.g. comm loss for hours).
        #
        # Supported providers:
        #   twilio       — global, also WhatsApp via twilio sandbox
        #                  pip install twilio  |  twilio.com
        #   dialog       — Dialog Axiata SMS Business API (Sri Lanka)
        #                  Contact Dialog Enterprise: 1678
        #   mobitel      — SLT-Mobitel SMS Push Enterprise (Sri Lanka)
        #                  Contact Mobitel Enterprise: mobitel.lk
        #   generic_http — any REST SMS gateway / local GSM modem
        #
        # Security: store credentials as environment variable names, not
        # plaintext.  Set account_sid to "env:TWILIO_SID" and the service
        # will read os.environ["TWILIO_SID"] at send time.
        "sms_alert": {
            "enabled":         False,

            # Provider: "twilio" | "dialog" | "mobitel" | "generic_http"
            "provider":        "twilio",

            # Recipient phone numbers in E.164 format: "+94771234567"
            # WhatsApp (Twilio): prefix with "whatsapp:" → "whatsapp:+94771234567"
            "recipients":      [],

            # Send a follow-up SMS when the alarm clears (default off —
            # CLEAR spam is annoying unless operators are fully remote).
            "notify_clear":    False,

            # Send WARN-severity alarms in addition to ALARM (default off —
            # WARN is very chatty: PF dips, minor voltage excursions, etc.)
            "notify_warn":     False,

            # Minimum time between SMS for the same (meter_id, code) [min].
            # Prevents SMS flooding: a sustained comm-loss won't send > 1 SMS/hour.
            "cooldown_min":    60,

            # Message template.  Substitution fields:
            #   {site}      — cfg['site']['plant_name']
            #   {meter}     — meter name
            #   {meter_id}  — meter ID
            #   {code}      — alarm code (e.g. OV, UV, CONN_LOSS)
            #   {message}   — alarm message text
            #   {ts}        — local timestamp (YYYY-MM-DD HH:MM:SS)
            "message_template": (
                "[{site}] ALARM: {code} on {meter}\n"
                "{message}\n"
                "{ts}"
            ),

            # Provider-specific credentials and options.
            # Use "env:VAR_NAME" values to avoid plaintext secrets in config.json.
            "provider_config": {
                # ── Twilio ──────────────────────────────────────────────────
                # account_sid: Twilio Account SID (starts with "AC")
                # auth_token:  Twilio Auth Token
                # from_number: your Twilio phone number (+1xxxxxxxxxx)
                #   For WhatsApp: "whatsapp:+14155238886" (sandbox) or verified number
                "account_sid":  "",
                "auth_token":   "",
                "from_number":  "",

                # ── Dialog / Mobitel ─────────────────────────────────────────
                # username / password: API credentials from Dialog/Mobitel Enterprise
                # sender_id: approved sender name (max 11 chars for Dialog)
                # api_url: override if Dialog/Mobitel updates their endpoint
                "username":   "",
                "password":   "",
                "sender_id":  "SCADA",
                "api_url":    "",

                # ── generic_http ─────────────────────────────────────────────
                # api_url:       required — POST endpoint
                # body_template: JSON string with {to} and {message} placeholders
                # headers:       extra HTTP headers dict
                # api_key_header / api_key: for Bearer/X-Api-Key style auth
                "body_template":  '{"to": "{to}", "message": "{message}"}',
                "headers":        {},
                "api_key_header": "",
                "api_key":        "",
            },
        },
    }
