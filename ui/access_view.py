from __future__ import annotations

import time
import csv
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from config.features import FEATURE_KEYS, DEFAULT_ROLE_PERMISSIONS
from utils.autostart import get_autostart_enabled, set_autostart_enabled


# ── Display metadata ──────────────────────────────────────────────────────────
FEATURE_LABELS = {
    "home":             "Start Here",
    "dashboard":        "Dashboard",
    "status":           "Meter Status",
    "data_graphs":      "Data & Graphs",
    "analytics":        "Plant Analytics",
    "intelligence":     "Intelligence",
    "solar_kpi":        "Solar KPI",
    "hydro_kpi":        "Hydro KPI",
    "load_kpi":         "Load KPI",
    "protection":       "Protection Rules",
    "alarms":           "Active Alarms",
    "incident_summary": "Incident Summary",
    "journal":          "Event Journal",
    "logging":          "Data Logger",
    "reports":          "Reports",
    "setup":            "Setup",
    "comm":             "Communication",
    "connectivity":     "Connectivity",
    "email":            "Email Alerts",
    "access_control":   "User & Access",
}

# Groups in display order — every key that exists in FEATURE_KEYS
FEATURE_GROUPS: list[tuple[str, str, list[str]]] = [
    ("#00d4ff", "START",       ["home"]),
    ("#00d4ff", "OPERATIONS",  ["dashboard", "status", "data_graphs"]),
    ("#39ff6e", "INSIGHTS",    ["analytics", "intelligence", "solar_kpi", "hydro_kpi", "load_kpi"]),
    ("#ff4f5e", "EVENTS",      ["alarms", "incident_summary", "protection"]),
    ("#7b8cff", "RECORDS",     ["logging", "reports", "journal"]),
    ("#c8a0ff", "SETTINGS",    ["setup", "comm", "connectivity", "email", "access_control"]),
]

ROLE_COLORS = {
    "operator":     "#00d4ff",
    "owner":        "#39ff6e",
    "engineer":     "#c8a0ff",
    "admin_master": "#ff4f5e",
}
ROLE_BADGE = {
    "operator":     "OPERATOR",
    "owner":        "OWNER",
    "engineer":     "ENGINEER",
    "admin_master": "ADMIN",
}

# Palette — mirrors main_window sidebar tokens
_BG      = "#141414"
_PANEL   = "#1c1c1e"
_SURFACE = "#2c2c2e"
_BORDER  = "#38383a"
_TEXT    = "#e8e4dc"
_MUTED   = "#9a9490"
_DIM     = "#5e5a56"
_ACCENT  = "#4da6ff"
_GOOD    = "#30c060"
_WARN    = "#d08030"
_ALARM   = "#c03030"
_VIOLET  = "#c8a0ff"


class AccessControlTab(tk.Frame):
    """
    User & Access management panel.

    ┌─────────────────────────────────────────────────────────────────────┐
    │  SESSION HEADER  (user · role badge · logout)                       │
    ├───────────────────┬─────────────────────────────────────────────────┤
    │  USER LIST (left) │  USER EDITOR (top-right) + PERMS (bottom-right) │
    │  220px fixed      │  fills remaining width                          │
    ├───────────────────┴─────────────────────────────────────────────────┤
    │  SYSTEM  (autostart + misc)                                         │
    ├─────────────────────────────────────────────────────────────────────┤
    │  AUDIT LOG  (full width, auto-refresh 2 s)                          │
    └─────────────────────────────────────────────────────────────────────┘
    """

    def __init__(self, parent, auth_service, session,
                 on_save_config=None, on_logout=None):
        super().__init__(parent, bg=_BG)
        self.auth_service   = auth_service
        self.session        = session or {}
        self.on_save_config = on_save_config
        self.on_logout      = on_logout

        self._selected_username: str | None = None
        self._perm_vars: dict[str, tk.BooleanVar] = {
            k: tk.BooleanVar(value=False) for k in FEATURE_KEYS
        }
        self._autostart_var = tk.BooleanVar(value=get_autostart_enabled())

        self._setup_styles()
        self._build_ui()
        self._refresh_all()
        self._tick_id = self.after(2000, self._tick)

    # ── Styles ─────────────────────────────────────────────────────────────────
    def _setup_styles(self):
        s = ttk.Style(self)
        s.configure("ACTree.Treeview",
                    background=_SURFACE, fieldbackground=_SURFACE,
                    foreground=_TEXT, rowheight=26, font=("Segoe UI", 9))
        s.configure("ACTree.Treeview.Heading",
                    background=_PANEL, foreground=_MUTED,
                    font=("Segoe UI", 8, "bold"))
        s.map("ACTree.Treeview",
              background=[("selected", "#1a2a40")],
              foreground=[("selected", "#ffffff")])

        s.configure("AuditTree.Treeview",
                    background=_BG, fieldbackground=_BG,
                    foreground=_TEXT, rowheight=22, font=("Segoe UI", 8))
        s.configure("AuditTree.Treeview.Heading",
                    background=_PANEL, foreground=_MUTED,
                    font=("Segoe UI", 8, "bold"))
        s.map("AuditTree.Treeview",
              background=[("selected", "#1a2a40")])

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _is_master(self) -> bool:
        return bool(self.session.get("is_master_admin", False))

    def _actor(self) -> str:
        return str(self.session.get("username", "") or "")

    def _role_color(self, role: str) -> str:
        return ROLE_COLORS.get(str(role).lower(), _MUTED)

    # ── Widget factories ───────────────────────────────────────────────────────
    def _section_header(self, parent, title: str, color: str,
                        row: int, colspan: int = 1,
                        right_widget_cb=None) -> tk.Frame:
        """Colored top-border section header row. Returns the inner frame."""
        outer = tk.Frame(parent, bg=_PANEL)
        outer.grid(row=row, column=0, columnspan=colspan, sticky="ew")
        tk.Frame(outer, bg=color, height=2).pack(fill="x", side="top")
        inner = tk.Frame(outer, bg=_PANEL)
        inner.pack(fill="x", padx=12, pady=(5, 4))
        tk.Label(inner, text=title, bg=_PANEL, fg=color,
                 font=("Segoe UI", 8, "bold")).pack(side="left")
        if callable(right_widget_cb):
            right_widget_cb(inner)
        return inner

    def _flat_btn(self, parent, text, fg, bg=_SURFACE,
                  command=None, font_size=8, bold=False, padx=10, pady=3) -> tk.Button:
        weight = "bold" if bold else "normal"
        return tk.Button(parent, text=text, bg=bg, fg=fg,
                         font=("Segoe UI", font_size, weight),
                         bd=0, padx=padx, pady=pady,
                         activebackground=_BORDER, activeforeground=_TEXT,
                         cursor="hand2", command=command or (lambda: None),
                         relief="flat")

    # ── Top-level build ────────────────────────────────────────────────────────
    def _build_ui(self):
        self.grid_rowconfigure(1, weight=1)   # body (user list + editor)
        self.grid_rowconfigure(2, weight=0)   # system strip
        self.grid_rowconfigure(3, weight=1)   # audit log (expands)
        self.grid_columnconfigure(0, weight=1)

        self._build_session_header(row=0)
        self._build_body(row=1)
        self._build_system_strip(row=2)
        self._build_audit_section(row=3)

    # ── Session header ─────────────────────────────────────────────────────────
    def _build_session_header(self, row: int):
        hdr = tk.Frame(self, bg=_PANEL, height=46)
        hdr.grid(row=row, column=0, sticky="ew")
        hdr.grid_propagate(False)
        hdr.columnconfigure(1, weight=1)

        tk.Frame(hdr, bg=_ACCENT, width=3).grid(row=0, column=0, sticky="ns")

        info = tk.Frame(hdr, bg=_PANEL)
        info.grid(row=0, column=1, sticky="w", padx=(14, 0))

        username = self.session.get("username", "Unknown")
        role     = self.session.get("role", "")
        color    = self._role_color(role)
        badge    = ROLE_BADGE.get(role, role.upper())

        tk.Label(info, text="Logged in as:", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 8)).pack(side="left", padx=(0, 6))
        tk.Label(info, text=username, bg=_PANEL, fg=_TEXT,
                 font=("Segoe UI", 11, "bold")).pack(side="left")

        badge_frame = tk.Frame(info, bg=color, padx=7, pady=2)
        badge_frame.pack(side="left", padx=(8, 0))
        tk.Label(badge_frame, text=badge, bg=color, fg="#000000",
                 font=("Segoe UI", 7, "bold")).pack()

        if not self._is_master():
            tk.Label(info, text="  —  View only.  Master Admin required to edit.",
                     bg=_PANEL, fg=_DIM, font=("Segoe UI", 8)).pack(side="left", padx=(14, 0))

        # Right side: logout
        right = tk.Frame(hdr, bg=_PANEL)
        right.grid(row=0, column=2, sticky="e", padx=(0, 14))
        if callable(self.on_logout):
            self._flat_btn(right, "⏻  Logout", fg="#ff6060", bg=_SURFACE,
                           command=self._logout, font_size=9, bold=True,
                           padx=12, pady=5).pack()

    # ── Body: left user list + right editor ───────────────────────────────────
    def _build_body(self, row: int):
        body = tk.Frame(self, bg=_BG)
        body.grid(row=row, column=0, sticky="nsew")
        body.grid_rowconfigure(0, weight=1)
        body.grid_columnconfigure(0, minsize=230, weight=0)
        body.grid_columnconfigure(1, weight=0)   # 1px divider
        body.grid_columnconfigure(2, weight=1)

        tk.Frame(body, bg=_BORDER, width=1).grid(row=0, column=1, sticky="ns")

        self._build_user_list_pane(body, col=0)
        self._build_right_pane(body, col=2)

    # ── Left: user list ────────────────────────────────────────────────────────
    def _build_user_list_pane(self, parent, col: int):
        pane = tk.Frame(parent, bg=_PANEL)
        pane.grid(row=0, column=col, sticky="nsew")
        pane.grid_rowconfigure(2, weight=1)
        pane.grid_columnconfigure(0, weight=1)

        # Header
        self._section_header(pane, "USERS", _ACCENT, row=0)

        # Count chip row
        count_row = tk.Frame(pane, bg=_PANEL)
        count_row.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 4))
        self._user_count_var = tk.StringVar(value="")
        tk.Label(count_row, textvariable=self._user_count_var,
                 bg=_PANEL, fg=_DIM, font=("Segoe UI", 7)).pack(side="left")

        # Tree
        tree_wrap = tk.Frame(pane, bg=_PANEL)
        tree_wrap.grid(row=2, column=0, sticky="nsew", padx=(6, 2), pady=(0, 4))
        tree_wrap.grid_rowconfigure(0, weight=1)
        tree_wrap.grid_columnconfigure(0, weight=1)

        self.user_tree = ttk.Treeview(
            tree_wrap,
            columns=("dot", "username", "role", "st"),
            show="headings",
            style="ACTree.Treeview",
            selectmode="browse",
        )
        self.user_tree.heading("dot",      text="")
        self.user_tree.heading("username", text="Username")
        self.user_tree.heading("role",     text="Role")
        self.user_tree.heading("st",       text="")
        self.user_tree.column("dot",      width=18,  stretch=False, anchor="center")
        self.user_tree.column("username", width=110, stretch=True,  anchor="w")
        self.user_tree.column("role",     width=72,  stretch=False, anchor="w")
        self.user_tree.column("st",       width=22,  stretch=False, anchor="center")

        vsb = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.user_tree.yview)
        self.user_tree.configure(yscrollcommand=vsb.set)
        self.user_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        self.user_tree.bind("<<TreeviewSelect>>", self._on_user_select)

        # Action buttons
        btn_row = tk.Frame(pane, bg=_PANEL)
        btn_row.grid(row=3, column=0, sticky="ew", padx=6, pady=(2, 8))

        self._btn_new = self._flat_btn(btn_row, "+ New User", fg=_TEXT, command=self._new_user)
        self._btn_new.pack(side="left", padx=(0, 4))

        self._btn_del = self._flat_btn(btn_row, "Delete", fg="#ff6060", command=self._delete_user)
        self._btn_del.pack(side="left")

        if not self._is_master():
            for b in (self._btn_new, self._btn_del):
                b.configure(state="disabled", fg=_DIM, cursor="")

    # ── Right pane: editor (top) + permissions (bottom) ───────────────────────
    def _build_right_pane(self, parent, col: int):
        pane = tk.Frame(parent, bg=_BG)
        pane.grid(row=0, column=col, sticky="nsew")
        pane.grid_rowconfigure(0, weight=0)   # editor fields
        pane.grid_rowconfigure(1, weight=1)   # permission grid
        pane.grid_columnconfigure(0, weight=1)

        self._build_user_editor_fields(pane, row=0)
        self._build_permission_grid(pane, row=1)

    def _build_user_editor_fields(self, parent, row: int):
        card = tk.Frame(parent, bg=_PANEL)
        card.grid(row=row, column=0, sticky="ew", pady=(0, 1))
        card.grid_columnconfigure(0, weight=1)

        self._section_header(card, "USER EDITOR", _ACCENT, row=0)

        form = tk.Frame(card, bg=_PANEL)
        form.grid(row=1, column=0, sticky="ew", padx=14, pady=10)

        self.username_var = tk.StringVar()
        self.role_var     = tk.StringVar(value="operator")
        self.enabled_var  = tk.BooleanVar(value=True)
        self.password_var = tk.StringVar()

        # ── Row A: Username · Enabled ──────────────────────────────────────────
        ra = tk.Frame(form, bg=_PANEL)
        ra.pack(fill="x", pady=(0, 8))

        # Username block
        ub = tk.Frame(ra, bg=_PANEL)
        ub.pack(side="left", padx=(0, 30))
        tk.Label(ub, text="USERNAME", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")
        self._username_ent = tk.Entry(
            ub, textvariable=self.username_var, width=22,
            bg=_SURFACE, fg=_TEXT, insertbackground=_TEXT,
            bd=0, highlightthickness=1,
            highlightbackground=_BORDER, highlightcolor=_ACCENT,
            font=("Segoe UI", 10), relief="flat",
        )
        self._username_ent.pack(fill="x", ipady=5, pady=(3, 0))

        # Role block
        rb = tk.Frame(ra, bg=_PANEL)
        rb.pack(side="left", padx=(0, 20))
        tk.Label(rb, text="ROLE", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")
        role_row = tk.Frame(rb, bg=_PANEL)
        role_row.pack(anchor="w", pady=(3, 0))
        self._role_cb = ttk.Combobox(
            role_row, textvariable=self.role_var,
            values=["operator", "owner", "engineer"],
            state="readonly", width=13, font=("Segoe UI", 9),
        )
        self._role_cb.pack(side="left")
        self._role_cb.bind("<<ComboboxSelected>>", self._on_role_change)
        self._role_dot = tk.Label(role_row, text="●", bg=_PANEL,
                                   fg=_MUTED, font=("Segoe UI", 11))
        self._role_dot.pack(side="left", padx=(6, 0))

        # Enabled block
        eb = tk.Frame(ra, bg=_PANEL)
        eb.pack(side="left", padx=(0, 20))
        tk.Label(eb, text="ENABLED", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")
        self._enabled_cb = tk.Checkbutton(
            eb, variable=self.enabled_var,
            bg=_PANEL, fg=_TEXT, selectcolor=_SURFACE,
            activebackground=_PANEL, bd=0, font=("Segoe UI", 9),
        )
        self._enabled_cb.pack(anchor="w", pady=(3, 0))

        # ── Row B: Password ────────────────────────────────────────────────────
        rb2 = tk.Frame(form, bg=_PANEL)
        rb2.pack(fill="x", pady=(0, 8))

        pwb = tk.Frame(rb2, bg=_PANEL)
        pwb.pack(side="left", padx=(0, 30))
        tk.Label(pwb, text="NEW PASSWORD", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")
        pw_row = tk.Frame(pwb, bg=_PANEL)
        pw_row.pack(anchor="w", pady=(3, 0))
        self._pw_ent = tk.Entry(
            pw_row, textvariable=self.password_var, show="●", width=22,
            bg=_SURFACE, fg=_TEXT, insertbackground=_TEXT,
            bd=0, highlightthickness=1,
            highlightbackground=_BORDER, highlightcolor=_ACCENT,
            font=("Segoe UI", 10), relief="flat",
        )
        self._pw_ent.pack(side="left", ipady=5)
        tk.Label(pw_row, text="  leave blank = no change",
                 bg=_PANEL, fg=_DIM, font=("Segoe UI", 7)).pack(side="left")

        # ── Save button ────────────────────────────────────────────────────────
        rc = tk.Frame(form, bg=_PANEL)
        rc.pack(fill="x", pady=(0, 2))
        self._btn_save_user = tk.Button(
            rc, text="Save User",
            bg=_ACCENT, fg="#000000",
            font=("Segoe UI", 9, "bold"), bd=0, padx=16, pady=5,
            activebackground="#6ab8ff", activeforeground="#000000",
            cursor="hand2", command=self._save_user,
        )
        self._btn_save_user.pack(side="left")

        # Status label (inline feedback)
        self._save_status_var = tk.StringVar()
        tk.Label(rc, textvariable=self._save_status_var,
                 bg=_PANEL, fg=_GOOD, font=("Segoe UI", 8)).pack(side="left", padx=(10, 0))

        # ── Row D: TOTP 2FA ───────────────────────────────────────────────────
        rd = tk.Frame(form, bg=_PANEL)
        rd.pack(fill="x", pady=(8, 0))

        tk.Label(rd, text="TWO-FACTOR AUTH (TOTP)",
                 bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 7, "bold")).pack(anchor="w")

        totp_row = tk.Frame(rd, bg=_PANEL)
        totp_row.pack(fill="x", pady=(4, 0))

        self._totp_status_var = tk.StringVar(value="")
        self._lbl_totp_status = tk.Label(totp_row, textvariable=self._totp_status_var,
                                          bg=_PANEL, fg=_DIM, font=("Segoe UI", 8),
                                          width=26, anchor="w")
        self._lbl_totp_status.pack(side="left", padx=(0, 10))

        self._btn_totp_setup = tk.Button(
            totp_row, text="Setup 2FA",
            bg=_SURFACE, fg=_TEXT,
            font=("Segoe UI", 8), bd=0, padx=10, pady=3,
            activebackground=_BORDER, activeforeground=_TEXT,
            cursor="hand2", relief="flat",
            command=self._totp_setup,
        )
        self._btn_totp_setup.pack(side="left", padx=(0, 6))

        self._btn_totp_disable = tk.Button(
            totp_row, text="Disable 2FA",
            bg=_SURFACE, fg="#ff9060",
            font=("Segoe UI", 8), bd=0, padx=10, pady=3,
            activebackground=_BORDER, activeforeground=_TEXT,
            cursor="hand2", relief="flat",
            command=self._totp_disable,
        )
        self._btn_totp_disable.pack(side="left")

        if not self._is_master():
            for w in (self._btn_totp_setup, self._btn_totp_disable):
                w.configure(state="disabled", cursor="")


        if not self._is_master():
            for w in (self._username_ent, self._enabled_cb,
                      self._role_cb, self._btn_save_user, self._pw_ent):
                try:
                    w.configure(state="disabled")
                except Exception:
                    pass

    # ── Permission grid ────────────────────────────────────────────────────────
    def _build_permission_grid(self, parent, row: int):
        outer = tk.Frame(parent, bg=_SURFACE)
        outer.grid(row=row, column=0, sticky="nsew")
        outer.grid_rowconfigure(1, weight=1)
        outer.grid_columnconfigure(0, weight=1)

        # Header with inline "select all / clear" shortcuts
        def _phdr_extras(inner):
            if self._is_master():
                self._flat_btn(inner, "Select All",  fg=_MUTED, command=self._perms_all,
                               padx=7, pady=1).pack(side="right", padx=(4, 0))
                self._flat_btn(inner, "Clear All",   fg=_MUTED, command=self._perms_none,
                               padx=7, pady=1).pack(side="right")

        phdr = tk.Frame(outer, bg=_PANEL)
        phdr.grid(row=0, column=0, sticky="ew")
        tk.Frame(phdr, bg=_VIOLET, height=2).pack(fill="x", side="top")
        ph_inner = tk.Frame(phdr, bg=_PANEL)
        ph_inner.pack(fill="x", padx=12, pady=(5, 4))
        tk.Label(ph_inner, text="TAB PERMISSIONS", bg=_PANEL, fg=_VIOLET,
                 font=("Segoe UI", 8, "bold")).pack(side="left")
        self._perm_user_lbl = tk.Label(ph_inner, text=" — select a user", bg=_PANEL,
                                        fg=_DIM, font=("Segoe UI", 8))
        self._perm_user_lbl.pack(side="left")
        _phdr_extras(ph_inner)

        # Scrollable canvas
        perm_canvas = tk.Canvas(outer, bg=_SURFACE, highlightthickness=0, bd=0)
        perm_vsb    = ttk.Scrollbar(outer, orient="vertical", command=perm_canvas.yview)
        perm_canvas.configure(yscrollcommand=perm_vsb.set)
        perm_canvas.grid(row=1, column=0, sticky="nsew")
        perm_vsb.grid(row=1, column=1, sticky="ns")

        perm_frame = tk.Frame(perm_canvas, bg=_SURFACE)
        _pw = perm_canvas.create_window((0, 0), window=perm_frame, anchor="nw")

        def _reconfigure(e=None):
            perm_canvas.configure(scrollregion=perm_canvas.bbox("all"))
        def _resize_win(e=None):
            perm_canvas.itemconfig(_pw, width=perm_canvas.winfo_width())

        perm_frame.bind("<Configure>", _reconfigure)
        perm_canvas.bind("<Configure>", _resize_win)

        def _on_mwheel(e):
            try:
                perm_canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
            except Exception:
                pass
        perm_canvas.bind("<MouseWheel>", _on_mwheel)

        self._perm_checks: list[tk.Checkbutton] = []

        for color, group_name, keys in FEATURE_GROUPS:
            grp = tk.Frame(perm_frame, bg=_SURFACE)
            grp.pack(fill="x", padx=14, pady=(10, 0))

            # Group label with colored pip
            glbl_row = tk.Frame(grp, bg=_SURFACE)
            glbl_row.pack(fill="x", pady=(0, 5))
            tk.Frame(glbl_row, bg=color, width=3, height=12).pack(side="left", padx=(0, 6))
            tk.Label(glbl_row, text=group_name, bg=_SURFACE, fg=color,
                     font=("Segoe UI", 7, "bold")).pack(side="left")

            # Checkboxes: 4 per row (fills width better)
            cb_frame = tk.Frame(grp, bg=_SURFACE)
            cb_frame.pack(fill="x")
            COLS = 4
            for idx, k in enumerate(keys):
                label = FEATURE_LABELS.get(k, k)
                cb = tk.Checkbutton(
                    cb_frame,
                    text=label,
                    variable=self._perm_vars[k],
                    bg=_SURFACE, fg=_TEXT,
                    selectcolor=_PANEL,
                    activebackground=_SURFACE,
                    activeforeground=_TEXT,
                    font=("Segoe UI", 9), anchor="w",
                    bd=0,
                )
                r, c = divmod(idx, COLS)
                cb.grid(row=r, column=c, sticky="w", padx=(0, 18), pady=2)
                cb_frame.columnconfigure(c, weight=1)
                self._perm_checks.append(cb)

        # Bottom action bar
        pact = tk.Frame(perm_frame, bg=_SURFACE)
        pact.pack(fill="x", padx=14, pady=(14, 10))

        self._btn_reset_perms = self._flat_btn(
            pact, "↺  Reset to Role Defaults", fg=_MUTED,
            command=self._reset_to_role, padx=10, pady=4,
        )
        self._btn_reset_perms.pack(side="left", padx=(0, 8))

        self._btn_save_perm = tk.Button(
            pact, text="✓  Save Permissions",
            bg="#1e3a18", fg="#39ff6e",
            font=("Segoe UI", 9, "bold"), bd=0, padx=14, pady=5,
            activebackground="#2a5020", activeforeground="#ffffff",
            cursor="hand2", command=self._save_permissions, relief="flat",
        )
        self._btn_save_perm.pack(side="left")

        self._perm_status_var = tk.StringVar()
        tk.Label(pact, textvariable=self._perm_status_var,
                 bg=_SURFACE, fg=_GOOD, font=("Segoe UI", 8)).pack(side="left", padx=(10, 0))

        if not self._is_master():
            for cb in self._perm_checks:
                cb.configure(state="disabled", fg=_DIM)
            for w in (self._btn_reset_perms, self._btn_save_perm):
                w.configure(state="disabled", fg=_DIM, cursor="")

    # ── System strip ───────────────────────────────────────────────────────────
    def _build_system_strip(self, row: int):
        strip = tk.Frame(self, bg=_PANEL)
        strip.grid(row=row, column=0, sticky="ew", pady=(1, 0))
        strip.grid_columnconfigure(0, weight=1)

        tk.Frame(strip, bg="#ff9f40", height=2).pack(fill="x", side="top")
        inner = tk.Frame(strip, bg=_PANEL)
        inner.pack(fill="x", padx=14, pady=8)

        tk.Label(inner, text="SYSTEM", bg=_PANEL, fg="#ff9f40",
                 font=("Segoe UI", 8, "bold")).pack(side="left", padx=(0, 24))

        # ── Autostart block ──────────────────────────────────────────────────
        as_frame = tk.Frame(inner, bg=_PANEL)
        as_frame.pack(side="left", padx=(0, 0))

        tk.Label(as_frame, text="Launch at Windows startup", bg=_PANEL, fg=_TEXT,
                 font=("Segoe UI", 9)).pack(side="left", padx=(0, 8))

        # Toggle switch (tk canvas pill)
        self._autostart_canvas = tk.Canvas(as_frame, width=44, height=22,
                                            bg=_PANEL, highlightthickness=0, bd=0)
        self._autostart_canvas.pack(side="left")
        self._draw_toggle(self._autostart_var.get())
        self._autostart_canvas.bind("<Button-1>", self._toggle_autostart)
        self._autostart_canvas.configure(cursor="hand2")

        self._autostart_status = tk.Label(as_frame, text="", bg=_PANEL, fg=_DIM,
                                           font=("Segoe UI", 7))
        self._autostart_status.pack(side="left", padx=(8, 0))
        self._update_autostart_status_label()

        # Separator
        tk.Frame(inner, bg=_BORDER, width=1, height=20).pack(side="left", padx=20)

        # ── App info ─────────────────────────────────────────────────────────
        try:
            from utils.version import VERSION
        except Exception:
            VERSION = "—"
        tk.Label(inner, text=f"MFM384 Monitoring  v{VERSION}", bg=_PANEL, fg=_DIM,
                 font=("Segoe UI", 8)).pack(side="left")

    def _draw_toggle(self, state: bool):
        c = self._autostart_canvas
        c.delete("all")
        bg   = _GOOD  if state else _SURFACE
        knob = _TEXT  if state else _MUTED
        # Pill background
        c.create_oval(1, 1, 21, 21, fill=bg, outline="")
        c.create_oval(23, 1, 43, 21, fill=bg, outline="")
        c.create_rectangle(11, 1, 33, 21, fill=bg, outline="")
        # Knob
        kx = 32 if state else 11
        c.create_oval(kx - 8, 3, kx + 8, 19, fill=knob, outline="")

    def _toggle_autostart(self, _e=None):
        new_val = not self._autostart_var.get()
        ok = set_autostart_enabled(new_val)
        if ok:
            self._autostart_var.set(new_val)
            self._draw_toggle(new_val)
            self._update_autostart_status_label()
        else:
            self._autostart_status.configure(text="Failed (check permissions)", fg=_ALARM)

    def _update_autostart_status_label(self):
        enabled = self._autostart_var.get()
        if enabled:
            self._autostart_status.configure(text="App will start automatically with Windows", fg=_GOOD)
        else:
            self._autostart_status.configure(text="Manual start only", fg=_DIM)

    # ── Audit section ──────────────────────────────────────────────────────────
    def _build_audit_section(self, row: int):
        outer = tk.Frame(self, bg=_PANEL)
        outer.grid(row=row, column=0, sticky="nsew", pady=(1, 0))
        outer.grid_rowconfigure(1, weight=1)
        outer.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(row, weight=1, minsize=160)

        # Header
        tk.Frame(outer, bg=_WARN, height=2).pack(fill="x", side="top")
        ahdr_inner = tk.Frame(outer, bg=_PANEL)
        ahdr_inner.pack(fill="x", padx=12, pady=(5, 4))

        tk.Label(ahdr_inner, text="AUDIT LOG", bg=_PANEL, fg=_WARN,
                 font=("Segoe UI", 8, "bold")).pack(side="left")
        tk.Label(ahdr_inner, text="  auto-refreshes every 2 s", bg=_PANEL, fg=_DIM,
                 font=("Segoe UI", 7)).pack(side="left")

        self._flat_btn(ahdr_inner, "Refresh", fg=_TEXT,
                       command=self._refresh_audit, padx=8, pady=2).pack(side="right", padx=(4, 0))
        self._flat_btn(ahdr_inner, "Export CSV", fg=_ACCENT,
                       command=self._export_audit, padx=8, pady=2).pack(side="right", padx=(4, 0))
        self._btn_clear_audit = self._flat_btn(
            ahdr_inner, "Clear History", fg="#ff6060",
            command=self._clear_audit, padx=8, pady=2,
        )
        self._btn_clear_audit.pack(side="right")

        if not self._is_master():
            self._btn_clear_audit.configure(state="disabled", fg=_DIM, cursor="")

        # Treeview
        tree_wrap = tk.Frame(outer, bg=_PANEL)
        tree_wrap.pack(fill="both", expand=True, padx=(4, 0), pady=(0, 4))
        tree_wrap.grid_rowconfigure(0, weight=1)
        tree_wrap.grid_columnconfigure(0, weight=1)

        self.audit_tree = ttk.Treeview(
            tree_wrap,
            columns=("time", "user", "action", "detail", "ok"),
            show="headings",
            style="AuditTree.Treeview",
        )
        for col, lbl, w, stretch in (
            ("time",   "Timestamp",  155, False),
            ("user",   "User",        110, False),
            ("action", "Action",      130, False),
            ("detail", "Detail",      100, True),
            ("ok",     "Result",       60, False),
        ):
            self.audit_tree.heading(col, text=lbl)
            self.audit_tree.column(col, width=w, anchor="w", stretch=stretch)

        self.audit_tree.tag_configure("fail", foreground="#ff6060")
        self.audit_tree.tag_configure("ok_row", foreground=_TEXT)

        avsb = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.audit_tree.yview)
        ahsb = ttk.Scrollbar(tree_wrap, orient="horizontal", command=self.audit_tree.xview)
        self.audit_tree.configure(yscrollcommand=avsb.set, xscrollcommand=ahsb.set)
        self.audit_tree.grid(row=0, column=0, sticky="nsew")
        avsb.grid(row=0, column=1, sticky="ns")
        ahsb.grid(row=1, column=0, sticky="ew")

    # ── Event callbacks ────────────────────────────────────────────────────────
    def _logout(self):
        if callable(self.on_logout):
            self.on_logout()

    def _tick(self):
        try:
            self._refresh_audit()
        except Exception:
            pass
        try:
            self._tick_id = self.after(2000, self._tick)
        except Exception:
            pass

    def _new_user(self):
        self._selected_username = None
        self.username_var.set("")
        self.role_var.set("operator")
        self.enabled_var.set(True)
        self.password_var.set("")
        self._save_status_var.set("")
        self._perm_status_var.set("")
        self._perm_user_lbl.configure(text=" — new user")
        self._update_role_dot()
        self._load_role_defaults("operator")
        try:
            self._totp_status_var.set("")
        except Exception:
            pass

    def _on_user_select(self, _evt=None):
        sel = self.user_tree.selection()
        if not sel:
            return
        vals = self.user_tree.item(sel[0], "values")
        if not vals:
            return
        username = str(vals[1])
        self._selected_username = username
        users = self.auth_service.list_users()
        rec = next((u for u in users if str(u.get("username", "")) == username), None)
        if not rec:
            return
        self.username_var.set(str(rec.get("username", "")))
        role = str(rec.get("role", "operator"))
        self.role_var.set(role)
        self.enabled_var.set(bool(rec.get("enabled", True)))
        self.password_var.set("")
        self._save_status_var.set("")
        self._perm_status_var.set("")
        self._perm_user_lbl.configure(text=f" — editing: {username}")
        self._update_role_dot()
        perms = rec.get("permissions", {}) or {}
        for k, v in self._perm_vars.items():
            v.set(bool(perms.get(k, False)))
        self._refresh_totp_status(username)

    def _on_role_change(self, _evt=None):
        self._update_role_dot()
        if self._is_master():
            self._load_role_defaults(self.role_var.get())

    def _update_role_dot(self):
        try:
            self._role_dot.configure(fg=self._role_color(self.role_var.get()))
        except Exception:
            pass

    def _load_role_defaults(self, role: str):
        allowed = DEFAULT_ROLE_PERMISSIONS.get(role, set())
        for k, v in self._perm_vars.items():
            v.set(k in allowed)

    def _reset_to_role(self):
        if not self._is_master():
            return
        self._load_role_defaults(self.role_var.get())
        self._perm_status_var.set("Defaults loaded — click Save Permissions to apply")

    def _perms_all(self):
        for v in self._perm_vars.values():
            v.set(True)

    def _perms_none(self):
        for v in self._perm_vars.values():
            v.set(False)

    def _save_user(self):
        if not self._is_master():
            return
        username = self.username_var.get().strip()
        if not username:
            messagebox.showwarning("User & Access", "Username is required.", parent=self)
            return
        perms = {k: bool(v.get()) for k, v in self._perm_vars.items()}
        ok = self.auth_service.upsert_user(
            actor=self._actor(),
            username=username,
            role=self.role_var.get().strip(),
            enabled=bool(self.enabled_var.get()),
            password=self.password_var.get(),
            permissions=perms,
        )
        if not ok:
            messagebox.showerror("User & Access", "Failed to save user.", parent=self)
            return
        self.password_var.set("")
        self._save_status_var.set("✓  Saved")
        self.after(2500, lambda: self._save_status_var.set(""))
        self._refresh_users()
        if callable(self.on_save_config):
            self.on_save_config()

    def _delete_user(self):
        if not self._is_master():
            return
        username = self._selected_username or self.username_var.get().strip()
        if not username:
            return
        if not messagebox.askyesno("Delete User",
                                   f"Delete user '{username}'?\nThis cannot be undone.",
                                   parent=self):
            return
        ok = self.auth_service.delete_user(actor=self._actor(), username=username)
        if ok:
            self._new_user()
            self._refresh_users()
            if callable(self.on_save_config):
                self.on_save_config()

    def _save_permissions(self):
        if not self._is_master():
            return
        username = self._selected_username or self.username_var.get().strip()
        if not username:
            messagebox.showwarning("User & Access", "Select a user first.", parent=self)
            return
        perms = {k: bool(v.get()) for k, v in self._perm_vars.items()}
        ok = self.auth_service.set_permissions(actor=self._actor(),
                                               username=username,
                                               permissions=perms)
        if ok:
            self._perm_status_var.set("✓  Saved")
            self.after(2500, lambda: self._perm_status_var.set(""))
            self._refresh_users()
            if callable(self.on_save_config):
                self.on_save_config()

    def _clear_audit(self):
        if not self._is_master():
            return
        if not messagebox.askyesno("Clear Audit",
                                   "Delete all audit history?\nThis cannot be undone.",
                                   parent=self):
            return
        self.auth_service.clear_audit(actor=self._actor(), allow=True)
        self._refresh_audit()

    def _export_audit(self):
        rows = self.auth_service.list_audit(limit=20000)
        if not rows:
            messagebox.showinfo("Audit Export", "No audit rows to export.", parent=self)
            return
        stamp = time.strftime("%Y%m%d_%H%M%S")
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Export audit log",
            defaultextension=".csv",
            initialfile=f"scada_audit_{stamp}.csv",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "user", "action", "detail", "success"])
                for r in rows:
                    writer.writerow([
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(r.get("ts", 0)))),
                        str(r.get("user_name", "")),
                        str(r.get("action", "")),
                        str(r.get("detail", "")),
                        "OK" if r.get("success", True) else "FAIL",
                    ])
            self.auth_service.record_audit(actor=self._actor(), action="AUDIT_EXPORTED", detail=str(path), success=True)
            messagebox.showinfo("Audit Export", "Audit CSV exported.", parent=self)
            self._refresh_audit()
        except Exception as exc:
            self.auth_service.record_audit(actor=self._actor(), action="AUDIT_EXPORT_FAILED", detail=str(exc), success=False)
            messagebox.showerror("Audit Export", f"Export failed:\n{exc}", parent=self)

    # ── TOTP ───────────────────────────────────────────────────────────────────
    def _refresh_totp_status(self, username: str) -> None:
        try:
            status = self.auth_service.totp_status(username)
            if status.get("enabled"):
                self._totp_status_var.set("2FA: ENABLED")
                self._lbl_totp_status.configure(fg=_GOOD)
            elif status.get("secret"):
                self._totp_status_var.set("2FA: pending enrollment")
                self._lbl_totp_status.configure(fg=_WARN)
            else:
                self._totp_status_var.set("2FA: disabled")
                self._lbl_totp_status.configure(fg=_DIM)
        except Exception:
            pass

    def _totp_setup(self) -> None:
        """Enroll TOTP for the selected user: show QR code + manual secret, verify first code."""
        if not self._is_master():
            return
        username = self._selected_username or self.username_var.get().strip()
        if not username:
            messagebox.showwarning("2FA Setup", "Select a user first.", parent=self)
            return

        secret = self.auth_service.totp_generate_secret(username)
        if not secret:
            messagebox.showerror("2FA Setup", "Could not generate secret.", parent=self)
            return

        uri = self.auth_service.totp_uri_for_user(username)
        self._show_totp_enrollment_dialog(username, secret, uri)

    def _show_totp_enrollment_dialog(self, username: str, secret: str, uri: str) -> None:
        dlg = tk.Toplevel(self)
        dlg.title("Setup Two-Factor Authentication")
        dlg.configure(bg=_PANEL)
        dlg.resizable(False, False)
        dlg.grab_set()

        # Header
        tk.Frame(dlg, bg=_ACCENT, height=3).pack(fill="x")
        hdr = tk.Frame(dlg, bg=_PANEL)
        hdr.pack(fill="x", padx=20, pady=(14, 0))
        tk.Label(hdr, text="Setup 2FA for: ", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 9)).pack(side="left")
        tk.Label(hdr, text=username, bg=_PANEL, fg=_TEXT,
                 font=("Segoe UI", 10, "bold")).pack(side="left")

        # QR code attempt
        qr_frame = tk.Frame(dlg, bg=_PANEL)
        qr_frame.pack(padx=20, pady=10)
        try:
            import qrcode  # type: ignore
            from PIL import Image as _PILImage, ImageTk as _PILImageTk  # type: ignore
            qr = qrcode.make(uri)
            qr_resized = qr.resize((180, 180))
            photo = _PILImageTk.PhotoImage(qr_resized)
            lbl_qr = tk.Label(qr_frame, image=photo, bg=_PANEL)
            lbl_qr.image = photo   # keep reference
            lbl_qr.pack()
            tk.Label(qr_frame, text="Scan with Google Authenticator / Authy",
                     bg=_PANEL, fg=_DIM, font=("Segoe UI", 8)).pack()
        except ImportError:
            tk.Label(qr_frame,
                     text="Install 'qrcode[pil]' for QR display.\n(pip install 'qrcode[pil]')",
                     bg=_PANEL, fg=_WARN, font=("Segoe UI", 8)).pack()

        # Manual entry secret
        sep_frame = tk.Frame(dlg, bg=_PANEL)
        sep_frame.pack(fill="x", padx=20, pady=(0, 8))
        tk.Label(sep_frame, text="Or enter secret manually:", bg=_PANEL, fg=_MUTED,
                 font=("Segoe UI", 8)).pack(anchor="w")
        secret_var = tk.StringVar(value=secret)
        secret_ent = tk.Entry(sep_frame, textvariable=secret_var,
                              bg=_SURFACE, fg=_TEXT, insertbackground=_TEXT,
                              bd=0, highlightthickness=1,
                              highlightbackground=_BORDER, highlightcolor=_ACCENT,
                              font=("Courier New", 11, "bold"), relief="flat",
                              state="readonly", readonlybackground=_SURFACE,
                              width=34)
        secret_ent.pack(fill="x", ipady=5, pady=(3, 0))

        # Verify field
        tk.Frame(dlg, bg=_BORDER, height=1).pack(fill="x", padx=20, pady=8)
        verify_frame = tk.Frame(dlg, bg=_PANEL)
        verify_frame.pack(fill="x", padx=20, pady=(0, 10))
        tk.Label(verify_frame,
                 text="Enter the 6-digit code from your app to complete enrollment:",
                 bg=_PANEL, fg=_TEXT, font=("Segoe UI", 9)).pack(anchor="w")
        code_var = tk.StringVar()
        code_ent = tk.Entry(verify_frame, textvariable=code_var,
                            bg=_SURFACE, fg=_TEXT, insertbackground=_TEXT,
                            bd=0, highlightthickness=1,
                            highlightbackground=_BORDER, highlightcolor=_ACCENT,
                            font=("Segoe UI", 18, "bold"), relief="flat",
                            width=10, justify="center")
        code_ent.pack(ipady=8, pady=(6, 0))
        code_ent.focus_set()

        err_var = tk.StringVar()
        tk.Label(verify_frame, textvariable=err_var, bg=_PANEL,
                 fg="#ff6060", font=("Segoe UI", 8)).pack(anchor="w", pady=(4, 0))

        # Buttons
        btn_row = tk.Frame(dlg, bg=_PANEL)
        btn_row.pack(fill="x", padx=20, pady=(0, 16))

        def _confirm():
            code = code_var.get().strip()
            ok = self.auth_service.totp_enable(
                actor=self._actor(),
                username=username,
                code=code,
            )
            if not ok:
                err_var.set("Code incorrect or expired. Try again.")
                code_var.set("")
                code_ent.focus_set()
                return
            dlg.destroy()
            messagebox.showinfo("2FA Enabled",
                                f"Two-factor authentication is now enabled for '{username}'.",
                                parent=self)
            self._refresh_totp_status(username)

        def _cancel():
            # Remove pending secret if user cancels
            self.auth_service.totp_disable(actor=self._actor(), username=username)
            dlg.destroy()
            self._refresh_totp_status(username)

        tk.Button(btn_row, text="Enable 2FA",
                  bg=_ACCENT, fg="#000000",
                  font=("Segoe UI", 9, "bold"), bd=0, padx=14, pady=5,
                  activebackground="#6ab8ff", activeforeground="#000000",
                  cursor="hand2", command=_confirm).pack(side="right")
        tk.Button(btn_row, text="Cancel",
                  bg=_SURFACE, fg=_MUTED,
                  font=("Segoe UI", 9), bd=0, padx=10, pady=5,
                  activebackground=_BORDER, activeforeground=_TEXT,
                  cursor="hand2", command=_cancel).pack(side="right", padx=(0, 8))

        dlg.bind("<Return>", lambda _e: _confirm())
        dlg.bind("<Escape>", lambda _e: _cancel())
        dlg.protocol("WM_DELETE_WINDOW", _cancel)

        # Center over parent
        dlg.update_idletasks()
        pw = self.winfo_rootx() + self.winfo_width() // 2
        ph = self.winfo_rooty() + self.winfo_height() // 2
        dw = dlg.winfo_reqwidth()
        dh = dlg.winfo_reqheight()
        dlg.geometry(f"{dw}x{dh}+{max(0, pw - dw//2)}+{max(0, ph - dh//2)}")

    def _totp_disable(self) -> None:
        if not self._is_master():
            return
        username = self._selected_username or self.username_var.get().strip()
        if not username:
            messagebox.showwarning("2FA", "Select a user first.", parent=self)
            return
        status = self.auth_service.totp_status(username)
        if not status.get("enabled") and not status.get("secret"):
            messagebox.showinfo("2FA", f"2FA is not enabled for '{username}'.", parent=self)
            return
        if not messagebox.askyesno("Disable 2FA",
                                   f"Disable two-factor authentication for '{username}'?",
                                   parent=self):
            return
        self.auth_service.totp_disable(actor=self._actor(), username=username)
        self._refresh_totp_status(username)
        if callable(self.on_save_config):
            self.on_save_config()

    # ── Refresh ────────────────────────────────────────────────────────────────
    def _refresh_users(self):
        prev = self._selected_username
        for i in self.user_tree.get_children():
            self.user_tree.delete(i)
        users = self.auth_service.list_users()
        for u in users:
            uname   = str(u.get("username", ""))
            role    = str(u.get("role", ""))
            enabled = bool(u.get("enabled", True))
            tag     = f"role_{role}"
            color   = ROLE_COLORS.get(role, _MUTED)
            iid = self.user_tree.insert("", "end",
                values=("●", uname, role, "✓" if enabled else "✗"),
                tags=(tag,))
            try:
                self.user_tree.tag_configure(tag, foreground=color)
            except Exception:
                pass
            if uname == prev:
                self.user_tree.selection_set(iid)
                self.user_tree.see(iid)
        n = len(users)
        self._user_count_var.set(f"{n} user{'s' if n != 1 else ''}")

    def _refresh_audit(self):
        rows = self.auth_service.list_audit(limit=400)
        for i in self.audit_tree.get_children():
            self.audit_tree.delete(i)
        for r in rows:
            ts     = time.strftime("%Y-%m-%d  %H:%M:%S",
                                   time.localtime(float(r.get("ts", 0))))
            ok_val = r.get("success", True)
            self.audit_tree.insert("", "end",
                values=(
                    ts,
                    str(r.get("user_name", "")),
                    str(r.get("action", "")),
                    str(r.get("detail", "")),
                    "OK" if ok_val else "FAIL",
                ),
                tags=("ok_row" if ok_val else "fail",),
            )

    def _refresh_all(self):
        self._refresh_users()
        self._refresh_audit()

    def on_show(self):
        """Called by main_window when this tab becomes visible."""
        self._refresh_all()
        # Sync autostart state (may have changed externally)
        try:
            current = get_autostart_enabled()
            if current != self._autostart_var.get():
                self._autostart_var.set(current)
                self._draw_toggle(current)
                self._update_autostart_status_label()
        except Exception:
            pass
