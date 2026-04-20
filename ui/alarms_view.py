"""
Alarms View — color-coded alarm history table.

Features:
- Color-tagged rows: ALARM=red, WARN=amber
- Severity filter (ALL / ALARM / WARN)
- Meter filter
- Active alarm count badge
- Scrollbar + export CSV
"""
from __future__ import annotations

import csv
import tkinter as tk
from datetime import datetime
from tkinter import ttk, filedialog, messagebox
from typing import Optional

from ui.styles import get_theme
from ui import pro_chrome


class AlarmsTab(ttk.Frame):

    def __init__(self, parent, alarm_engine, meters, session=None):
        super().__init__(parent)
        self.alarm_engine = alarm_engine
        self.meters = meters
        self.session = session  # dict with 'username', 'role', etc.

        self._build_ui()
        self.update_view()

    # ── build ─────────────────────────────────────────────────────────────────

    def _build_ui(self):
        t = get_theme()
        root = pro_chrome.page(self)
        pro_chrome.header(
            root,
            "Alarm Center",
            "Active alarm response, acknowledgement and historical fault review.",
            accent=t.alarm,
        )

        # ── Toolbar ───────────────────────────────────────────────────────────
        toolbar = ttk.Frame(root, style="App.Panel.TFrame", padding=(10, 8))
        toolbar.pack(fill="x", padx=12, pady=(0, 8))

        ttk.Label(toolbar, text="FILTERS", style="H3.TLabel").pack(side="left", padx=(0, 16))

        # Active count badge
        self._count_var = tk.StringVar(value="")
        self._count_lbl = tk.Label(
            toolbar,
            textvariable=self._count_var,
            font=("Segoe UI", 8, "bold"),
            bg=t.alarm, fg="#ffffff",
            padx=6, pady=1, relief="flat",
        )
        self._count_lbl.pack(side="left", padx=(0, 16))

        ttk.Label(toolbar, text="Meter:", style="FieldLabel.TLabel").pack(side="left")
        meter_vals = ["ALL"] + [f"Meter {m.meter_id}" for m in self.meters]
        self._meter_var = tk.StringVar(value="ALL")
        self._cmb_meter = ttk.Combobox(
            toolbar, textvariable=self._meter_var,
            values=meter_vals, state="readonly", width=12,
        )
        self._cmb_meter.pack(side="left", padx=(4, 14))
        self._cmb_meter.bind("<<ComboboxSelected>>", lambda _e: self.update_view())

        ttk.Label(toolbar, text="Severity:", style="FieldLabel.TLabel").pack(side="left")
        self._sev_var = tk.StringVar(value="ALL")
        self._cmb_sev = ttk.Combobox(
            toolbar, textvariable=self._sev_var,
            values=["ALL", "ALARM", "WARN"], state="readonly", width=8,
        )
        self._cmb_sev.pack(side="left", padx=(4, 14))
        self._cmb_sev.bind("<<ComboboxSelected>>", lambda _e: self.update_view())

        ttk.Button(toolbar, text="↻ Refresh", style="Ghost.TButton",
                   command=self.update_view).pack(side="left", padx=(0, 4))

        ttk.Button(toolbar, text="✓ ACK Selected", style="Ghost.TButton",
                   command=self._ack_selected).pack(side="left", padx=(0, 4))

        ttk.Button(toolbar, text="✓✓ ACK All", style="Ghost.TButton",
                   command=self._ack_all).pack(side="left", padx=(0, 4))

        ttk.Button(toolbar, text="🔕 Shelve…", style="Ghost.TButton",
                   command=self._shelve_selected).pack(side="left", padx=(0, 4))

        ttk.Button(toolbar, text="🔔 Unshelve", style="Ghost.TButton",
                   command=self._unshelve_selected).pack(side="left", padx=(0, 4))

        ttk.Button(toolbar, text="Export CSV…", style="Ghost.TButton",
                   command=self._export_csv).pack(side="left")

        ttk.Button(toolbar, text="Clear Log", style="Danger.TButton",
                   command=self._clear_log).pack(side="right", padx=(0, 8))

        # ── Table ─────────────────────────────────────────────────────────────
        tbl_card = pro_chrome.card(root, accent=t.alarm, padx=12, pady=(0, 8))
        tk.Label(tbl_card, text="Alarm Log", bg=t.card, fg=t.text, font=("Segoe UI", 11, "bold")).pack(
            anchor="w", padx=10, pady=(8, 4)
        )
        tbl_frame = tk.Frame(tbl_card, bg=t.card)
        tbl_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        cols = ("ts", "meter", "sev", "ack", "code", "msg", "hint")
        self.tree = ttk.Treeview(
            tbl_frame, columns=cols, show="headings",
            selectmode="browse",
        )
        self.tree.heading("ts",    text="Timestamp",     anchor="w")
        self.tree.heading("meter", text="Meter",         anchor="w")
        self.tree.heading("sev",   text="Severity",      anchor="center")
        self.tree.heading("ack",   text="ACK",           anchor="center")
        self.tree.heading("code",  text="Code",          anchor="w")
        self.tree.heading("msg",   text="Message",       anchor="w")
        self.tree.heading("hint",  text="Operator Hint", anchor="w")
        self.tree.column("ts",    width=155, minwidth=120, stretch=False, anchor="w")
        self.tree.column("meter", width=75,  minwidth=60,  stretch=False, anchor="w")
        self.tree.column("sev",   width=75,  minwidth=60,  stretch=False, anchor="center")
        self.tree.column("ack",   width=90,  minwidth=70,  stretch=False, anchor="center")
        self.tree.column("code",  width=140, minwidth=80,  stretch=False, anchor="w")
        self.tree.column("msg",   width=360, minwidth=200, stretch=True,  anchor="w")
        self.tree.column("hint",  width=260, minwidth=160, stretch=True,  anchor="w")

        # severity + ack state row tags
        t = get_theme()
        self.tree.tag_configure("ALARM",      foreground=t.alarm)
        self.tree.tag_configure("WARN",       foreground=t.warn)
        self.tree.tag_configure("INFO",       foreground=t.text_muted)
        self.tree.tag_configure("ALARM_ACKED", foreground=t.warn)   # red → amber once acked
        self.tree.tag_configure("WARN_ACKED",  foreground=t.text_muted)
        self.tree.tag_configure("SHELVED",     foreground=t.text_muted)  # muted = suppressed

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tbl_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tbl_frame.grid_rowconfigure(0, weight=1)
        tbl_frame.grid_columnconfigure(0, weight=1)

        # ── Footer status ─────────────────────────────────────────────────────
        self._status_var = tk.StringVar(value="")
        ttk.Label(root, textvariable=self._status_var, style="Caption.TLabel").pack(
            side="bottom", anchor="w", padx=12, pady=(0, 6))

    # ── data refresh ─────────────────────────────────────────────────────────

    def update_view(self):
        self.tree.delete(*self.tree.get_children())
        t = get_theme()

        meter_flt = self._meter_var.get()
        sev_flt   = self._sev_var.get()

        if hasattr(self.alarm_engine, "history_snapshot"):
            history = self.alarm_engine.history_snapshot(500)
        else:
            history = list(getattr(self.alarm_engine, "history", [])[-500:])

        shown = 0
        active_alarms = 0
        unacked_alarms = 0

        # ── Severity-priority sort ────────────────────────────────────────────
        # Bands (ascending = higher priority at top):
        #   0 — active + unacked ALARM
        #   1 — active + acked ALARM
        #   2 — active WARN (any ack state)
        #   3 — cleared ALARM
        #   4 — cleared WARN
        #   5 — INFO / other
        # Within each band: newest first (-ts).
        def _sort_key(ev):
            sev    = str(getattr(ev, "severity", "WARN")).upper()
            active = bool(getattr(ev, "active", False))
            acked  = bool(getattr(ev, "acknowledged", False))
            if sev == "ALARM":
                band = 0 if (active and not acked) else (1 if active else 3)
            elif sev == "WARN":
                band = 2 if active else 4
            else:
                band = 5
            return (band, -ev.ts)

        sorted_history = sorted(history, key=_sort_key)

        for ev in sorted_history:
            sev          = str(getattr(ev, "severity", "WARN")).upper()
            meter_label  = "TOTAL" if ev.meter_id == "TOTAL" else f"Meter {ev.meter_id}"
            is_active    = bool(getattr(ev, "active", False))
            is_acked     = bool(getattr(ev, "acknowledged", False))
            is_first_out = bool(getattr(ev, "is_first_out", False))

            if sev == "ALARM" and is_active and not is_acked:
                active_alarms += 1
                unacked_alarms += 1
            elif sev == "ALARM" and is_active:
                active_alarms += 1

            if meter_flt != "ALL" and meter_flt != meter_label:
                continue
            if sev_flt   != "ALL" and sev_flt   != sev:
                continue

            ts = datetime.fromtimestamp(ev.ts).strftime("%Y-%m-%d  %H:%M:%S")

            if is_acked:
                by = str(getattr(ev, "acknowledged_by", "") or "")
                ack_txt = f"✓ {by}" if by else "✓ ACK"
                tag = f"{sev}_ACKED" if sev in ("ALARM", "WARN") else "INFO"
            else:
                ack_txt = "— pending" if is_active else ""
                tag = sev if sev in ("ALARM", "WARN") else "INFO"

            # Prepend first-out star so the root-cause alarm is instantly visible
            hint = _alarm_hint(ev.code, ev.message)
            if is_first_out:
                hint = f"★ FIRST-OUT  {hint}"

            self.tree.insert("", "end",
                values=(ts, meter_label, sev, ack_txt, ev.code, ev.message, hint),
                tags=(tag,),
                iid=f"{id(ev)}",
            )
            shown += 1

        # Update count badge — show unacked count
        if unacked_alarms > 0:
            self._count_var.set(f"  ● {unacked_alarms} UNACKED  ")
            self._count_lbl.config(bg=t.alarm)
            self._count_lbl.pack(side="left", padx=(0, 16))
        elif active_alarms > 0:
            self._count_var.set(f"  ✓ {active_alarms} ACTIVE  ")
            self._count_lbl.config(bg=t.warn)
            self._count_lbl.pack(side="left", padx=(0, 16))
        else:
            self._count_lbl.pack_forget()

        # ── Active shelves ────────────────────────────────────────────────────
        # Insert at top (before alarm history rows) so the operator always sees
        # what maintenance suppressions are currently in effect.
        shelved_count = 0
        if hasattr(self.alarm_engine, "shelved_items"):
            try:
                for s in self.alarm_engine.shelved_items():
                    mid  = s["meter_id"]
                    code = s["code"]
                    rem  = s["remaining_sec"]
                    if rem is not None:
                        rem_txt = f"{rem/60:.0f} min remaining" if rem >= 60 else f"{rem:.0f} s remaining"
                    else:
                        rem_txt = "indefinite"
                    meter_lbl = "ALL" if mid == "*" else ("TOTAL" if mid == "TOTAL" else f"Meter {mid}")
                    code_lbl  = "ALL" if code == "*" else code
                    self.tree.insert("", 0,   # insert at top
                        values=("— shelved —", meter_lbl, "—", "SHELVED", code_lbl,
                                f"🔕 Alarm shelved ({rem_txt})", "Click Unshelve to re-enable"),
                        tags=("SHELVED",),
                    )
                    shelved_count += 1
            except Exception:
                pass

        total = len(history)
        shelf_txt = f"  |  {shelved_count} shelved" if shelved_count else ""
        self._status_var.set(
            f"Showing {shown} of {total} events{shelf_txt}"
            + (f"  |  Filtered by: "
               f"meter={meter_flt} severity={sev_flt}" if (meter_flt != "ALL" or sev_flt != "ALL") else "")
        )

    # ── actions ──────────────────────────────────────────────────────────────

    def _actor(self) -> str:
        """Return current operator name for audit trail."""
        if self.session and isinstance(self.session, dict):
            return str(self.session.get("username", "operator") or "operator")
        return "operator"

    def _ack_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("ACK", "Select an alarm row first.")
            return
        actor = self._actor()
        count = 0
        for iid in sel:
            vals = self.tree.item(iid, "values")
            # vals: (ts, meter_label, sev, ack_txt, code, msg, hint)
            if len(vals) < 5:
                continue
            sev_txt     = str(vals[2]).upper()
            code_txt    = str(vals[4])
            meter_label = str(vals[1])
            # Resolve meter_id
            if meter_label.upper() == "TOTAL":
                meter_id = "TOTAL"
            else:
                try:
                    meter_id = int(meter_label.replace("Meter ", "").strip())
                except Exception:
                    meter_id = meter_label
            if self.alarm_engine.acknowledge(meter_id, code_txt, actor):
                count += 1
        self.update_view()
        if count:
            self._status_var.set(f"Acknowledged {count} alarm(s) — operator: {actor}")

    def _ack_all(self):
        actor = self._actor()
        count = self.alarm_engine.acknowledge_all(actor)
        self.update_view()
        if count:
            self._status_var.set(f"Acknowledged {count} alarm(s) — operator: {actor}")
        else:
            self._status_var.set("No unacknowledged active alarms.")

    def _shelve_selected(self):
        """Open the shelve dialog for the selected alarm row."""
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Shelve", "Select an alarm row first.")
            return
        iid  = sel[0]
        vals = self.tree.item(iid, "values")
        if len(vals) < 5:
            return
        meter_label = str(vals[1])
        code_txt    = str(vals[4])
        # Ignore shelved-marker rows (already shelved)
        if code_txt == "ALL" or str(vals[3]).upper() == "SHELVED":
            messagebox.showinfo("Shelve", "This item is already shelved.")
            return
        # Resolve meter_id
        if meter_label.upper() in ("ALL", "TOTAL"):
            meter_id = meter_label.upper()
        else:
            try:
                meter_id = int(meter_label.replace("Meter ", "").strip())
            except Exception:
                meter_id = meter_label

        _ShelveDialog(self, self.alarm_engine, meter_id, code_txt, self._actor(),
                      on_done=self.update_view)

    def _unshelve_selected(self):
        """Remove shelf for the selected row."""
        sel = self.tree.selection()
        if not sel:
            # No selection — show all active shelves and let operator pick
            items = []
            if hasattr(self.alarm_engine, "shelved_items"):
                try:
                    items = self.alarm_engine.shelved_items()
                except Exception:
                    pass
            if not items:
                messagebox.showinfo("Unshelve", "No active shelves.")
                return
            # Build a simple picker
            labels = [
                f"({s['meter_id']}, {s['code']})"
                for s in items
            ]
            choice = _pick_from_list(self, "Unshelve", "Select shelf to remove:", labels)
            if choice is None:
                return
            s = items[choice]
            self.alarm_engine.unshelve(s["meter_id"], s["code"], self._actor())
            self.update_view()
            return

        iid  = sel[0]
        vals = self.tree.item(iid, "values")
        if len(vals) < 5:
            return
        meter_label = str(vals[1])
        code_txt    = str(vals[4])
        if meter_label.upper() in ("ALL", "TOTAL"):
            meter_id = meter_label.upper()
        else:
            try:
                meter_id = int(meter_label.replace("Meter ", "").strip())
            except Exception:
                meter_id = meter_label
        if hasattr(self.alarm_engine, "unshelve"):
            removed = self.alarm_engine.unshelve(meter_id, code_txt, self._actor())
            if removed:
                self._status_var.set(f"Unshelved ({meter_id}, {code_txt})")
            else:
                messagebox.showinfo("Unshelve", f"No shelf found for ({meter_id}, {code_txt}).")
        self.update_view()

    def _export_csv(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Alarm Log",
        )
        if not path:
            return
        try:
            items = self.tree.get_children()
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["Timestamp", "Meter", "Severity", "ACK", "Code", "Message", "Operator Hint"])
                for iid in items:
                    w.writerow(self.tree.item(iid, "values"))
            messagebox.showinfo("Export", f"Saved {len(items)} rows to:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))

    def _clear_log(self):
        # Count active alarms first so we can warn the operator if any are live.
        active_count = len(getattr(self.alarm_engine, "active", {}) or {})
        if active_count > 0:
            prompt = (
                f"There are {active_count} active alarm(s) still live.\n"
                "Cleared (inactive) alarms will be removed.\n"
                "Active alarms are preserved.\n\nContinue?"
            )
        else:
            prompt = "Remove cleared (inactive) alarms from the log?\nActive alarms are always kept."

        if not messagebox.askyesno("Clear Log", prompt):
            return
        try:
            if hasattr(self.alarm_engine, "clear_inactive_history"):
                removed = self.alarm_engine.clear_inactive_history()
            elif hasattr(self.alarm_engine, "clear_history"):
                # Fallback: old method clears everything
                self.alarm_engine.clear_history()
                removed = -1
            else:
                h = getattr(self.alarm_engine, "history", None)
                if h is not None:
                    h.clear()
                removed = -1
            if removed > 0:
                messagebox.showinfo("Log cleared", f"Removed {removed} inactive alarm(s) from the log.")
        except Exception:
            pass
        self.update_view()


def _alarm_hint(code, message) -> str:
    """Return a one-line operator action hint for the given alarm code/message."""
    c = str(code or "").upper()
    text = f"{c} {message}".upper()

    # Exact-code matches first (fast path, no false positives)
    if c == "UV":
        return "Undervoltage: check supply breaker, upstream fault, and transformer tap setting."
    if c == "OV":
        return "Overvoltage: check transformer tap, AVR/OLTC setting, or generator excitation."
    if c == "FREQ_LOW":
        return "Low frequency: grid disturbance or generator under-speed. Check governor/load."
    if c == "FREQ_HIGH":
        return "High frequency: generator over-speed or light-load condition. Check governor."
    if c == "PF_LOW":
        return "Low PF: check reactive load (motors, ballasts). Consider capacitor bank. CEB surcharge applies below 0.85."
    if c == "V_UNBAL":
        return "Voltage unbalance: check for single-phase loads, blown fuse, or phase loss. CEB limit: 3%."
    if c == "NEG_KW":
        return "Negative kW: check CT direction, import/export sign convention, and deduct meter setting."
    if c in ("CONN_LOSS", "CONN"):
        return "Comm loss: check RS-485 wiring, COM port, slave ID, baud rate, and USB adapter."
    if c == "STALE":
        return "Stale data: confirm polling is running and meter is responding within stale timeout."

    # Fallback: keyword scan
    if "CONN" in text or "COMM" in text or "NO RESPONSE" in text:
        return "Check RS-485 wiring, COM port, slave ID, baud rate, and USB adapter."
    if "STALE" in text or "OLD DATA" in text:
        return "Data is delayed. Confirm polling is running and the meter is responding."
    if "NEG" in text and "KW" in text:
        return "Check CT direction, import/export sign, and total-meter add/deduct setting."
    if "UNDERVOLT" in text or c.startswith("UV"):
        return "Undervoltage: check supply breaker, upstream fault, transformer tap."
    if "OVERVOLT" in text or c.startswith("OV"):
        return "Overvoltage: check transformer tap, AVR setting, or generator excitation."
    if "FREQ" in text or "HZ" in text:
        return "Check grid frequency stability and generator/inverter synchronization."
    if "UNBAL" in text:
        return "Check for single-phase loads, blown fuse, or phase loss on site."
    if "PF" in text or "POWER FACTOR" in text:
        return "Check reactive load, capacitor bank status, and CT/PT wiring."
    return "Open Meter Status and Communication pages to confirm live data before action."


# ── Shelve dialog ─────────────────────────────────────────────────────────────

class _ShelveDialog(tk.Toplevel):
    """
    Modal dialog for shelving (suppressing) an alarm for a maintenance window.

    Operator selects:
    • Scope    — this alarm code only / all alarms for this meter / all alarms
    • Duration — preset (15 min / 30 min / 1 h / 2 h / 4 h / 8 h) or indefinite
    """

    _DURATIONS = [
        ("15 minutes",  15 * 60),
        ("30 minutes",  30 * 60),
        ("1 hour",      60 * 60),
        ("2 hours",     2 * 3600),
        ("4 hours",     4 * 3600),
        ("8 hours",     8 * 3600),
        ("Indefinite (until restart)", None),
    ]

    def __init__(self, parent, alarm_engine, meter_id, code: str, actor: str,
                 on_done=None):
        super().__init__(parent)
        self.alarm_engine = alarm_engine
        self.meter_id  = meter_id
        self.code      = code
        self.actor     = actor
        self._on_done  = on_done

        self.title("Shelve Alarm — Maintenance Suppression")
        self.resizable(False, False)
        self.grab_set()   # modal
        self.transient(parent)

        self._build()
        self.after(50, self._centre)

    def _centre(self):
        try:
            self.update_idletasks()
            pw = self.master.winfo_rootx() + self.master.winfo_width() // 2
            ph = self.master.winfo_rooty() + self.master.winfo_height() // 2
            w  = self.winfo_width()
            h  = self.winfo_height()
            self.geometry(f"+{pw - w//2}+{ph - h//2}")
        except Exception:
            pass

    def _build(self):
        pad = {"padx": 16, "pady": 6}

        ttk.Label(self, text="Shelve Alarm", font=("Segoe UI", 12, "bold")).pack(
            anchor="w", **pad, pady=(14, 2))

        # Info label
        info = (f"Alarm:   {self.code}\n"
                f"Meter:   {self.meter_id}\n\n"
                "Shelving suppresses new alarm raises for the selected duration.\n"
                "The alarm will re-evaluate automatically when the shelf expires.")
        ttk.Label(self, text=info, justify="left",
                  font=("Segoe UI", 9)).pack(anchor="w", padx=16, pady=(0, 8))

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=12, pady=4)

        # Scope
        scope_frm = ttk.LabelFrame(self, text="  Scope  ", padding=(12, 6))
        scope_frm.pack(fill="x", padx=12, pady=4)

        self._scope_var = tk.StringVar(value="code")
        ttk.Radiobutton(scope_frm,
                        text=f"This alarm only  ({self.code}  on meter {self.meter_id})",
                        variable=self._scope_var, value="code").pack(anchor="w")
        ttk.Radiobutton(scope_frm,
                        text=f"All alarms for meter {self.meter_id}",
                        variable=self._scope_var, value="meter").pack(anchor="w")
        ttk.Radiobutton(scope_frm,
                        text="All alarms system-wide  (full maintenance mode)",
                        variable=self._scope_var, value="all").pack(anchor="w")

        # Duration
        dur_frm = ttk.LabelFrame(self, text="  Duration  ", padding=(12, 6))
        dur_frm.pack(fill="x", padx=12, pady=4)

        self._dur_var = tk.StringVar(value=self._DURATIONS[2][0])  # default: 1 hour
        for label, _ in self._DURATIONS:
            ttk.Radiobutton(dur_frm, text=label,
                            variable=self._dur_var, value=label).pack(anchor="w")

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=12, pady=8)

        btn_frm = ttk.Frame(self)
        btn_frm.pack(fill="x", padx=12, pady=(0, 12))
        ttk.Button(btn_frm, text="🔕  Apply Shelf",
                   command=self._apply, style="Primary.TButton").pack(side="left")
        ttk.Button(btn_frm, text="Cancel",
                   command=self.destroy).pack(side="left", padx=(8, 0))

    def _apply(self):
        # Resolve duration
        dur_label = self._dur_var.get()
        duration  = next(
            (sec for lbl, sec in self._DURATIONS if lbl == dur_label),
            3600,   # fallback: 1 h
        )

        # Resolve scope → (meter_id, code) pairs to shelve
        scope = self._scope_var.get()
        if scope == "code":
            pairs = [(self.meter_id, self.code)]
        elif scope == "meter":
            pairs = [(self.meter_id, "*")]
        else:
            pairs = [("*", "*")]

        if hasattr(self.alarm_engine, "shelve"):
            for mid, code in pairs:
                self.alarm_engine.shelve(mid, code, duration, actor=self.actor)

        self.destroy()
        if callable(self._on_done):
            self._on_done()


def _pick_from_list(parent, title: str, prompt: str, options: list) -> Optional[int]:
    """
    Simple modal listbox picker.
    Returns the selected index, or None if cancelled.
    """
    result: list = [None]

    dlg = tk.Toplevel(parent)
    dlg.title(title)
    dlg.resizable(False, False)
    dlg.grab_set()
    dlg.transient(parent)

    ttk.Label(dlg, text=prompt).pack(padx=16, pady=(12, 4), anchor="w")

    lb_frame = ttk.Frame(dlg)
    lb_frame.pack(fill="both", padx=12, pady=4)
    lb = tk.Listbox(lb_frame, selectmode="single", height=min(len(options), 10), width=40)
    for opt in options:
        lb.insert("end", opt)
    lb.pack(side="left", fill="both")
    sb = ttk.Scrollbar(lb_frame, orient="vertical", command=lb.yview)
    lb.configure(yscrollcommand=sb.set)
    sb.pack(side="left", fill="y")

    btn_frm = ttk.Frame(dlg)
    btn_frm.pack(fill="x", padx=12, pady=(4, 12))

    def _ok():
        sel = lb.curselection()
        if sel:
            result[0] = sel[0]
        dlg.destroy()

    ttk.Button(btn_frm, text="OK",     command=_ok).pack(side="left")
    ttk.Button(btn_frm, text="Cancel", command=dlg.destroy).pack(side="left", padx=(6, 0))

    dlg.wait_window()
    return result[0]
