import os
import time
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from core.protection_engine import ProtectionEngine, PARAM_SPECS
from utils.paths import logs_dir


COLOR = {
    "NORMAL":  "#1a7f37",   # green
    "WARNING": "#d97706",   # amber
    "ALARM":   "#b91c1c",   # red (for violations)
    "UNKNOWN": "#6b7280",   # gray
    "SUPPRESSED": "#6b7280",
    "GOOD":    "#1a7f37",
    "STALE":   "#d97706",
}


class ProtectionView(ttk.Frame):
    def __init__(self, parent, cfg, meters, snapshot_fn, protection_engine: ProtectionEngine | None = None, tier_filter: int | None = None, *args, **kwargs):
        if "padding" not in kwargs:
            kwargs["padding"] = 8
        super().__init__(parent, *args, **kwargs)
        self.cfg = cfg
        self.meters = meters
        self.snapshot_fn = snapshot_fn
        # Allow sharing one ProtectionEngine across tabs (Dashboard highlighting)
        self.engine = protection_engine or ProtectionEngine(cfg)
        self.tier_filter = tier_filter

        # Separate event stream per tab (Protection vs Smart Advisory).
        self.event_stream = "advisory" if int(tier_filter or 1) == 2 else "protection"

        # Performance guards (avoid lag from full table rebuilds)
        self._last_render_ts = 0.0
        self._render_period_sec = 0.5  # table refresh rate
        self._last_hist_head = None    # (ts,state,rid) of newest event

        self._build_ui()

    def _build_ui(self):
        # TOP: tiles (customizable: each tile shows any rule)
        tiles_wrap = ttk.Frame(self)
        tiles_wrap.pack(fill="x", padx=8, pady=6)

        ui_key = "protection_ui" if (self.tier_filter in (None, 1)) else "smart_advisory_ui"
        self._ui_key = ui_key
        ui_cfg = self.cfg.setdefault(ui_key, {})
        # Apply optional tier filter (Tier-1=Protection, Tier-2=Smart Advisory)
        rules = list(self.engine.rules)
        if self.tier_filter is not None:
            rules = [r for r in rules if int(getattr(r, 'tier', 1)) == int(self.tier_filter)]
        rule_rids = [r.rid for r in rules]
        self._allowed_rids = set(rule_rids)


        # Normalize mapping: keep existing order where possible, append new rules at the end.
        tr = ui_cfg.get("tile_rids")
        if not isinstance(tr, list) or not tr:
            tr = list(rule_rids)
        else:
            tr = [rid for rid in tr if rid in rule_rids]
            for rid in rule_rids:
                if rid not in tr:
                    tr.append(rid)

        # De-duplicate while preserving order (prevents duplicated tiles).
        seen = set()
        tr_unique = []
        for rid in tr:
            if rid in seen:
                continue
            seen.add(rid)
            tr_unique.append(rid)
        tr = tr_unique
        ui_cfg["tile_rids"] = tr
        self.tile_rids = ui_cfg["tile_rids"]

        # Tiles are rebuilt dynamically so that enabling/disabling rules immediately updates the view.
        self._tiles_wrap = tiles_wrap
        self._tile_vars = [None] * len(self.tile_rids)   # indexed by slot_index
        self._visible_tile_rids: list[str] = []
        self._rebuild_tiles(force=True)
        # MIDDLE: conditions + event history (split)
        mid = ttk.Panedwindow(self, orient="vertical")
        mid.pack(fill="both", expand=True, padx=8, pady=6)

        cond_wrap = ttk.Frame(mid)
        hist_wrap = ttk.Frame(mid)
        mid.add(cond_wrap, weight=3)
        mid.add(hist_wrap, weight=2)

        cols = (
            'Rule', 'Parameter', 'Scope', 'Live Value', 'Compare', 'Pickup/Limit', 'Delay(s)',
            'Time in violation(s)', 'State', 'Last Event', 'Ack', 'Clear', 'To Event', 'Chart HL'
        )
        self.table = ttk.Treeview(cond_wrap, columns=cols, show="headings", height=12)
        for c in cols:
            self.table.heading(c, text=c)
            self.table.column(c, width=110, anchor='center')

        self.table.column('Rule', width=240, anchor='w')
        self.table.column('Parameter', width=120)
        self.table.column('Scope', width=80)
        self.table.column('Live Value', width=110)
        self.table.column('Compare', width=70)
        self.table.column('Pickup/Limit', width=110)
        self.table.column('Delay(s)', width=70)
        self.table.column('Time in violation(s)', width=120)
        self.table.column('State', width=110)
        self.table.column('Last Event', width=160)
        self.table.column('Ack', width=70)
        self.table.column('Clear', width=70)
        self.table.column('To Event', width=80)
        self.table.column('Chart HL', width=80)


        # row highlighting for rules that opt-in to chart highlighting
        self.table.tag_configure('HL_ALARM', background='#3b0a0a')
        self.table.tag_configure('HL_WARNING', background='#3b2a0a')
        self.table.tag_configure('HL_NORMAL', background='')

        vs = ttk.Scrollbar(cond_wrap, orient="vertical", command=self.table.yview)
        self.table.configure(yscrollcommand=vs.set)
        self.table.pack(side="left", fill="both", expand=True)
        vs.pack(side="right", fill="y")

        # Right-click menu
        self.menu = tk.Menu(self, tearoff=0)
        self.menu.add_command(label="Edit Rule...", command=self._edit_selected)
        self.menu.add_command(label="Acknowledge", command=self._ack_selected)
        self.menu.add_command(label="Clear (manual)", command=self._clear_selected)
        self.menu.add_separator()
        self.menu.add_command(label="Toggle Enable/Disable", command=self._toggle_enabled)
        self.menu.add_command(label="Toggle Log To Event History", command=self._toggle_log_to_history)
        self.menu.add_command(label="Toggle Chart Highlight", command=self._toggle_highlight)
        self.table.bind("<Button-3>", self._on_right_click)
        self.table.bind("<Double-1>", self._on_double_click)

        # EVENT HISTORY
        hist_title = "Protection Event History" if self.event_stream != "advisory" else "Smart Advisory Event History"
        lf = ttk.Labelframe(hist_wrap, text=hist_title)
        lf.pack(fill="both", expand=True, padx=0, pady=(6, 0))

        top_hist = ttk.Frame(lf)
        top_hist.pack(fill="x", padx=8, pady=(6, 2))

        self.save_csv_var = tk.BooleanVar(value=bool(self.engine.get_events_cfg(self.event_stream).get("enabled", True)))
        ttk.Checkbutton(top_hist, text="Save events to CSV", variable=self.save_csv_var, command=self._on_toggle_csv).pack(side="left")

        default_name = "advisory_events.csv" if self.event_stream == "advisory" else "protection_events.csv"
        self.csv_path_var = tk.StringVar(value=str(self.engine.get_events_cfg(self.event_stream).get("csv_path", os.path.join(logs_dir(), default_name))))
        ttk.Entry(top_hist, textvariable=self.csv_path_var, width=60).pack(side="left", padx=(8, 4))
        ttk.Button(top_hist, text="Browse...", command=self._browse_csv).pack(side="left")
        ttk.Button(top_hist, text="Clear History", command=self._clear_history).pack(side="right")

        hcols = ("Time", "Rule", "State", "Value")
        self.hist = ttk.Treeview(lf, columns=hcols, show="headings", height=8)
        for c in hcols:
            self.hist.heading(c, text=c)
            self.hist.column(c, width=120, anchor="center")
        self.hist.column("Time", width=160)
        self.hist.column("Rule", width=260, anchor="w")

        hsv = ttk.Scrollbar(lf, orient="vertical", command=self.hist.yview)
        self.hist.configure(yscrollcommand=hsv.set)
        self.hist.pack(side="left", fill="both", expand=True, padx=(8, 0), pady=(2, 8))
        hsv.pack(side="right", fill="y", padx=(0, 8), pady=(2, 8))

        # BOTTOM: profile + reload
        bottom = ttk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(bottom, text="Reload Rules", command=self._reload).pack(side="right", padx=4)
        ttk.Button(bottom, text="Load Profile JSON", command=self._load_profile).pack(side="right", padx=4)
        ttk.Button(bottom, text="Save Profile JSON", command=self._save_profile).pack(side="right", padx=4)

        self.status_var = tk.StringVar(value="Protection supervision: stale suppression ON (no false alarms from comm loss).")
        ttk.Label(bottom, textvariable=self.status_var).pack(side="left")

    def _enabled_tile_rids(self) -> list[str]:
        """Return the current list of tile rule IDs that should be visible (enabled only)."""
        out: list[str] = []
        for rid in (self.tile_rids or []):
            r = self.engine._find(rid)
            if not r:
                continue
            if not bool(getattr(r, "enabled", True)):
                continue
            out.append(str(rid))
        return out

    def _rebuild_tiles(self, force: bool = False) -> None:
        """Rebuild the tile grid.

        - Shows ONLY enabled rules (disabled rules create no tile; no empty space).
        - Keeps slot_index stable for right-click remap (config mapping is slot-based).
        """
        visible = self._enabled_tile_rids()
        if (not force) and (visible == self._visible_tile_rids):
            return
        self._visible_tile_rids = list(visible)

        # Clear existing tile widgets
        for w in list(self._tiles_wrap.winfo_children()):
            try:
                w.destroy()
            except Exception:
                pass

        # Reset vars (preserve length)
        self._tile_vars = [None] * len(self.tile_rids)

        cols = 5
        visible_idx = 0
        for slot_index, rid in enumerate(self.tile_rids):
            r = self.engine._find(rid)
            if not r or not bool(getattr(r, "enabled", True)):
                continue

            title = r.display_name
            row = visible_idx // cols
            col = visible_idx % cols
            visible_idx += 1

            card = ttk.Frame(self._tiles_wrap, relief="groove", borderwidth=2)
            card.grid(row=row, column=col, padx=2, pady=2, sticky="nsew")
            self._tiles_wrap.columnconfigure(col, weight=1)
            self._tiles_wrap.rowconfigure(row, weight=1)

            title_var = tk.StringVar(value=title)
            value_var = tk.StringVar(value="—")
            state_var = tk.StringVar(value="UNKNOWN")

            lbl_t = ttk.Label(card, textvariable=title_var, font=("Segoe UI", 9, "bold"))
            lbl_v = ttk.Label(card, textvariable=value_var, font=("Segoe UI", 12, "bold"))
            lbl_s = ttk.Label(card, textvariable=state_var, font=("Segoe UI", 9))
            lbl_t.pack(pady=(4, 0))
            lbl_v.pack()
            lbl_s.pack(pady=(0, 4))

            # Right-click on a tile to remap it to a different rule (slot index = mapping index)
            for w in (card, lbl_t, lbl_v, lbl_s):
                w.bind("<Button-3>", lambda e, si=slot_index: self._on_tile_right_click(e, si))

            self._tile_vars[slot_index] = {
                "rid": rid,
                "title_var": title_var,
                "value_var": value_var,
                "state_var": state_var,
                "lbl_v": lbl_v,
                "lbl_s": lbl_s,
            }

    def update_view(self):
        try:
            now = time.time()
            # Throttle UI rendering (engine can tick often; UI should not rebuild constantly)
            if (now - self._last_render_ts) < self._render_period_sec:
                return
            self._last_render_ts = now

            # IMPORTANT: MainWindow ticks the ProtectionEngine even when this tab is not visible.
            # Rendering must use the cached result to avoid double CPU work (tick() is heavy).
            rows, summary, ts = ([], {}, 0.0)
            if hasattr(self.engine, 'get_last'):
                rows, summary, ts = self.engine.get_last()
            # Fallback: if engine hasn't ticked yet, tick once.
            if not rows and not summary:
                snap = self.snapshot_fn()
                rows, _events, summary = self.engine.tick(snap)
            # If operator toggled rule enable/disable, rebuild tiles so disabled rules disappear immediately.
            self._rebuild_tiles()

            if hasattr(self, '_allowed_rids'):
                rows = [rr for rr in (rows or []) if str(rr.get('id') or '') in self._allowed_rids]
            self._render(summary, rows)
            self._render_history()
        except Exception as e:
            self.status_var.set(f"Protection error: {e}")

    def _render_history(self):
        # Update only if something changed (prevents UI stutter)
        hist = self.engine.get_event_history(self.event_stream)[:500]
        if hasattr(self, '_allowed_rids'):
            hist = [ev for ev in hist if str(ev.get('rid') or '') in self._allowed_rids]
        head = None
        if hist:
            ev0 = hist[0]
            head = (
                float(ev0.get("ts") or ev0.get("ts_epoch") or 0.0),
                str(ev0.get("state", "")),
                str(ev0.get("rid", "")),
            )
        if head == self._last_hist_head:
            return
        self._last_hist_head = head

        self.hist.delete(*self.hist.get_children())
        for i, ev in enumerate(hist):
            ts = float(ev.get("ts") or ev.get("ts_epoch") or time.time())
            ttxt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            rid = str(ev.get("rid", ""))
            rule_name = rid
            for r in self.engine.rules:
                if r.rid == rid:
                    rule_name = r.display_name
                    break
            st = str(ev.get("state", ""))
            val = ev.get("value")
            vtxt = "—" if val is None else f"{float(val):.4g}"
            self.hist.insert("", "end", iid=f"EV{i}", values=[ttxt, rule_name, st, vtxt])

    def _render(self, summary: dict, rows: list):
        # Map id -> row for fast access
        row_by_id = {str(r.get("id") or ""): r for r in (rows or [])}

        # Tiles: each tile shows one selected rule (right-click tile to remap)
        for idx, tv in enumerate(self._tile_vars):
            if tv is None:
                continue
            rid = str(self.tile_rids[idx]) if idx < len(self.tile_rids) else ""
            rr = row_by_id.get(rid)

            # Title always follows assigned rule
            if rr:
                tv["title_var"].set(str(rr.get("name") or rid))
            else:
                # fall back to rule object name if exists
                r = self.engine._find(rid) if rid else None
                tv["title_var"].set(r.display_name if r else (rid or "UNASSIGNED"))

            if not rr:
                tv["value_var"].set("—")
                tv["state_var"].set("UNKNOWN")
                col = COLOR.get("UNKNOWN")
                tv["lbl_v"].configure(foreground=col)
                tv["lbl_s"].configure(foreground=col)
                continue

            st = str(rr.get("state") or "UNKNOWN")
            live = rr.get("live_value")
            param = str(rr.get("param") or "").strip()

            unit = ""
            spec = PARAM_SPECS.get(param)
            if spec:
                unit = str(spec.get("unit") or "").strip()

            if live is None:
                vtxt = "—"
            else:
                if param == "DataValid":
                    vtxt = "GOOD" if float(live) >= 1.0 else "STALE"
                else:
                    try:
                        vtxt = f"{float(live):.2f}"
                    except Exception:
                        vtxt = str(live)
                if unit and vtxt not in ("GOOD", "STALE"):
                    vtxt = f"{vtxt} {unit}"

            tv["value_var"].set(vtxt)
            tv["state_var"].set(st)

            col = COLOR.get(st, COLOR["UNKNOWN"])
            tv["lbl_v"].configure(foreground=col)
            tv["lbl_s"].configure(foreground=col)

        # update table (incremental: update existing rows in place)

        existing = set(self.table.get_children())
        seen = set()
        for r in rows:
            live = r.get("live_value")
            live_txt = "—" if live is None else f"{live:.2f}"

            pickup = r.get("pickup_high")
            if pickup is None:
                pickup = r.get("pickup_low")
            pickup_txt = "—" if pickup is None else f"{pickup:.2f}"

            last_event = r.get("last_event_ts")
            last_txt = '—' if not last_event else time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_event))

            ack_txt = "Acked" if r.get("acked") else "Ack"
            clr_txt = "Cleared" if r.get("cleared") else "Clear"

            iid = r["id"]
            seen.add(iid)
            hl = bool(r.get('highlight_chart', True))
            st_row = str(r.get('state', 'UNKNOWN'))
            tag = ''
            if hl and st_row in ('ALARM','WARNING'):
                tag = 'HL_ALARM' if st_row=='ALARM' else 'HL_WARNING'

            values = [
                r.get('name', ''),
                r.get('param', ''),
                r.get('scope', 'TOTAL'),
                live_txt,
                ('<' if str(r.get('compare','HIGH')).upper()=='LOW' else '>'),
                pickup_txt,
                f"{float(r.get('delay_sec', 0.0)):.1f}",
                f"{float(r.get('time_in_violation', 0.0)):.1f}",
                r.get('state', 'UNKNOWN'),
                last_txt,
                ack_txt,
                clr_txt,
                ('Yes' if r.get('log_to_history', True) else 'No'),
                ('Yes' if r.get('highlight_chart', True) else 'No'),

            ]

            if iid in existing:
                # Only touch the item if something changed (reduces flicker)
                try:
                    cur = self.table.item(iid, "values")
                    if tuple(cur) != tuple(values):
                        self.table.item(iid, values=values)
                    self.table.item(iid, tags=(tag,) if tag else ())
                except Exception:
                    self.table.item(iid, values=values, tags=(tag,) if tag else ())
            else:
                self.table.insert('', 'end', iid=iid, tags=(tag,) if tag else (), values=values)

        # Remove any old rows that no longer exist
        for iid in (existing - seen):
            try:
                self.table.delete(iid)
            except Exception:
                pass



    # ---------- TILE MAPPING (right-click tiles) ----------
    def _on_tile_right_click(self, event, slot_index: int):
        """Right-click a tile to select which protection rule it displays."""
        try:
            menu = tk.Menu(self, tearoff=0)
            # Only offer rules relevant to this tab (tier filter)
            for r in self.engine.rules:
                if hasattr(self, '_allowed_rids') and r.rid not in self._allowed_rids:
                    continue
                label = f"{r.display_name}  [{r.rid}]"
                menu.add_command(label=label, command=lambda rid=r.rid: self._set_tile_rule(slot_index, rid))
            menu.tk_popup(event.x_root, event.y_root)
        except Exception:
            pass

    def _set_tile_rule(self, slot_index: int, rid: str):
        rid = str(rid or '').strip()
        if not rid:
            return
        # Ensure mapping list is long enough
        ui_cfg = self.cfg.setdefault(getattr(self, '_ui_key', 'protection_ui'), {})
        tile_rids = ui_cfg.get('tile_rids')
        if not isinstance(tile_rids, list):
            tile_rids = []
        while len(tile_rids) <= slot_index:
            tile_rids.append('')
        tile_rids[slot_index] = rid

        # Enforce uniqueness without shrinking the list (slot indices are sacred).
        # If the selected rid already exists in another slot, clear the other slot.
        for i, x in enumerate(tile_rids):
            if i != slot_index and str(x) == rid:
                tile_rids[i] = ""

        ui_cfg['tile_rids'] = tile_rids
        self.tile_rids = tile_rids
        self._tile_vars = [None] * len(self.tile_rids)
        self._rebuild_tiles(force=True)
        # Update title immediately
        r = self.engine._find(rid)
        title = (r.display_name if r else rid)
        try:
            tv = self._tile_vars[slot_index] if slot_index < len(self._tile_vars) else None
            if isinstance(tv, dict):
                tv['title_var'].set(title)
        except Exception:
            pass
    # ---------- ACK / CLEAR ----------
    def _selected_id(self):
        sel = self.table.selection()
        return sel[0] if sel else None

    def _on_right_click(self, event):
        iid = self.table.identify_row(event.y)
        if iid:
            self.table.selection_set(iid)
            self.menu.tk_popup(event.x_root, event.y_root)

    def _on_double_click(self, event):
        iid = self.table.identify_row(event.y)
        col = self.table.identify_column(event.x)  # '#1'..'#N'
        if not iid:
            return
        self.table.selection_set(iid)

        # Map column index to column name
        cols = list(self.table["columns"])
        try:
            cidx = int(col.replace('#', '')) - 1
            col_name = cols[cidx]
        except Exception:
            col_name = ""

        # Double-click behavior:
        # - Ack/Clear columns act like buttons
        # - Editable columns open inline editor
        # - Otherwise opens full dialog
        if col_name == "Ack":
            self._ack_selected()
            return
        if col_name == "Clear":
            self._clear_selected()
            return

        editable = {"Rule", "Parameter", "Scope", "Compare", "Pickup/Limit", "Delay(s)", "To Event", "Chart HL"}
        if col_name in editable:
            self._begin_inline_edit(iid, col_name)
            return

        # Fallback: open full dialog
        self._edit_selected()

    def _begin_inline_edit(self, iid: str, col_name: str):
        """Inline cell editor (Entry/Combobox) for fast SCADA-style tuning."""
        # Clean up previous editor
        if getattr(self, "_cell_editor", None):
            try:
                self._cell_editor.destroy()
            except Exception:
                pass
            self._cell_editor = None

        # Treeview.bbox expects a column identifier like '#1' or the internal column id.
        cols = list(self.table["columns"])
        try:
            col_id = f"#{cols.index(col_name) + 1}"
        except Exception:
            col_id = col_name
        bbox = self.table.bbox(iid, col_id)
        if not bbox:
            return
        x, y, w, h = bbox
        value = self.table.set(iid, col_name)

        def commit(new_val: str):
            try:
                self._apply_cell_edit(iid, col_name, new_val)
            finally:
                try:
                    if getattr(self, "_cell_editor", None):
                        self._cell_editor.destroy()
                except Exception:
                    pass
                self._cell_editor = None

        # Build editor based on column
        if col_name in ("To Event", "Chart HL"):
            # Toggle Yes/No
            commit("No" if str(value).strip().lower() == "yes" else "Yes")
            return

        if col_name == "Scope":
            scope_opts = ['TOTAL']
            for mtr in self.meters:
                try:
                    mid = int(getattr(mtr, 'meter_id', 0))
                except Exception:
                    continue
                if mid > 0:
                    scope_opts.append(f'M{mid}')
            var = tk.StringVar(value=value or 'TOTAL')
            cb = ttk.Combobox(self.table, textvariable=var, values=scope_opts, state="readonly")
            cb.place(x=x, y=y, width=w, height=h)
            cb.focus_set()
            cb.bind("<<ComboboxSelected>>", lambda e: commit(var.get()))
            cb.bind("<FocusOut>", lambda e: commit(var.get()))
            cb.bind("<Return>", lambda e: commit(var.get()))
            self._cell_editor = cb
            return

        if col_name == "Parameter":
            try:
                from core.protection_engine import PARAM_SPECS
                param_opts = list(PARAM_SPECS.keys())
            except Exception:
                param_opts = ['Vavg','V1N','Frequency','ROCOF','Unbalance','Iavg','I1','kVA_pct','THD_V','THD_I','PF','kW','kVA','kVAr','ExportKW']
            # keep a stable operator-friendly order
            preferred = ['Vavg','V1N','Frequency','ROCOF','Unbalance','Iavg','I1','kVA_pct','THD_V','THD_I','PF','kW','kVA','kVAr','ExportKW']
            ordered = [p for p in preferred if p in param_opts] + [p for p in param_opts if p not in preferred]
            var = tk.StringVar(value=value or '')
            cb = ttk.Combobox(self.table, textvariable=var, values=ordered, state="readonly")
            cb.place(x=x, y=y, width=w, height=h)
            cb.focus_set()
            cb.bind("<<ComboboxSelected>>", lambda e: commit(var.get()))
            cb.bind("<FocusOut>", lambda e: commit(var.get()))
            cb.bind("<Return>", lambda e: commit(var.get()))
            self._cell_editor = cb
            return

        if col_name == "Compare":
            var = tk.StringVar(value=value or ">")
            cb = ttk.Combobox(self.table, textvariable=var, values=[">", "<"], state="readonly")
            cb.place(x=x, y=y, width=w, height=h)
            cb.focus_set()
            cb.bind("<<ComboboxSelected>>", lambda e: commit(var.get()))
            cb.bind("<FocusOut>", lambda e: commit(var.get()))
            cb.bind("<Return>", lambda e: commit(var.get()))
            self._cell_editor = cb
            return

        # Entry editor
        var = tk.StringVar(value=value)
        ent = ttk.Entry(self.table, textvariable=var)
        ent.place(x=x, y=y, width=w, height=h)
        ent.focus_set()
        ent.selection_range(0, tk.END)
        ent.bind("<Return>", lambda e: commit(var.get()))
        ent.bind("<Escape>", lambda e: commit(value))
        ent.bind("<FocusOut>", lambda e: commit(var.get()))
        self._cell_editor = ent

    def _apply_cell_edit(self, iid: str, col_name: str, new_val: str):
        """Apply an inline edit to engine + refresh UI."""
        r = None
        for rr in self.engine.rules:
            if rr.rid == iid:
                r = rr
                break
        if not r:
            return

        # Normalize
        nv = str(new_val).strip()

        if col_name == "Rule":
            self.engine.update_rule_meta(iid, display_name=nv)
            self.update_view()
            return

        if col_name == "Scope":
            self.engine.set_rule_flags(iid, scope=nv)
            self.update_view()
            return

        if col_name == "Parameter":
            self.engine.set_rule_flags(iid, param=nv)
            self.update_view()
            return

        if col_name == "Compare":
            cmp = "LOW" if nv in ("<", "LOW", "low") else "HIGH"
            # keep pickup as-is, but re-store under correct pickup_low/pickup_high
            current_pickup = r.pickup_low if cmp == "LOW" else r.pickup_high
            if current_pickup is None:
                # if opposite exists, reuse it
                current_pickup = r.pickup_high if cmp == "LOW" else r.pickup_low
            self.engine.set_rule_flags(iid, compare=cmp)
            if current_pickup is not None:
                self.engine.update_rule_setpoints(iid, pickup=float(current_pickup), compare=cmp)
            self.update_view()
            return

        if col_name == "Pickup/Limit":
            try:
                p = float(nv)
            except Exception:
                return
            self.engine.update_rule_setpoints(iid, pickup=p, compare=r.compare_mode)
            self.update_view()
            return

        if col_name == "Delay(s)":
            try:
                d = float(nv)
            except Exception:
                return
            self.engine.update_rule_setpoints(iid, delay_sec=d)
            self.update_view()
            return

        if col_name == "To Event":
            self.engine.set_rule_flags(iid, log_to_history=(nv.lower() == "yes"))
            self.update_view()
            return

        if col_name == "Chart HL":
            self.engine.set_rule_flags(iid, highlight_chart=(nv.lower() == "yes"))
            self.update_view()
            return

    def _toggle_enabled(self):
        rid = self._selected_id()
        if not rid:
            return
        f = self.engine.get_rule_flags(rid)
        self.engine.set_rule_flags(rid, enabled=not bool(f.get("enabled", True)))
        self.update_view()

    def _toggle_log_to_history(self):
        rid = self._selected_id()
        if not rid:
            return
        f = self.engine.get_rule_flags(rid)
        self.engine.set_rule_flags(rid, log_to_history=not bool(f.get("log_to_history", True)))
        self.update_view()

    def _toggle_highlight(self):
        rid = self._selected_id()
        if not rid:
            return
        f = self.engine.get_rule_flags(rid)
        self.engine.set_rule_flags(rid, highlight_chart=not bool(f.get("highlight_chart", True)))
        self.update_view()

    def _edit_selected(self):
        rid = self._selected_id()
        if not rid:
            return

        rule = None
        for r in self.engine.rules:
            if r.rid == rid:
                rule = r
                break
        if not rule:
            return

        # effective pickup shown (based on compare mode)
        pickup = rule.pickup_low if str(rule.compare_mode).upper() == 'LOW' else rule.pickup_high
        flags = self.engine.get_rule_flags(rid)

        # Build scope options (TOTAL + enabled meter IDs)
        scope_opts = ['TOTAL']
        for mtr in self.meters:
            try:
                mid = int(getattr(mtr, 'meter_id', 0))
            except Exception:
                continue
            if mid > 0:
                scope_opts.append(f'M{mid}')

        # Parameter options from engine
        try:
            from core.protection_engine import PARAM_SPECS
            param_opts = list(PARAM_SPECS.keys())
        except Exception:
            param_opts = ['Vavg','Frequency','Iavg','PF','kW','kVA','kVAr','ExportKW','kVA_pct','ROCOF','Unbalance','THD_V','THD_I']

        dlg = _RuleEditDialog(
            self,
            title=f"Edit: {rule.display_name}",
            rule_name=str(rule.display_name or ""),
            pickup_value=pickup,
            delay_value=rule.delay_sec,
            enabled=bool(flags.get('enabled', True)),
            log_to_history=bool(flags.get('log_to_history', True)),
            highlight_chart=bool(flags.get('highlight_chart', True)),
            scope=str(rule.scope or 'TOTAL'),
            param=str(rule.param_key or ''),
            compare=str(rule.compare_mode or 'HIGH'),
            scope_options=scope_opts,
            param_options=param_opts,
        )
        self.wait_window(dlg)
        if not dlg.result:
            return

        try:
            # Update rule label first so event history shows the new name.
            self.engine.update_rule_meta(rid, display_name=dlg.result.get("rule_name", ""))
            self.engine.set_rule_flags(
                rid,
                enabled=dlg.result['enabled'],
                log_to_history=dlg.result['log_to_history'],
                highlight_chart=dlg.result['highlight_chart'],
                scope=dlg.result['scope'],
                param=dlg.result['param'],
                compare=dlg.result['compare'],
            )
            self.engine.update_rule_setpoints(
                rid,
                pickup=dlg.result['pickup'],
                delay_sec=dlg.result['delay'],
                compare=dlg.result['compare'],
            )
            self.update_view()
        except Exception as e:
            messagebox.showerror('Error', str(e), parent=self)


    def _ack_selected(self):
        rid = self._selected_id()
        if not rid:
            return
        self.engine.ack_rule(rid)
        self.update_view()

    def _clear_selected(self):
        rid = self._selected_id()
        if not rid:
            return
        self.engine.clear_rule(rid)
        self.update_view()

    # ---------- Profile ----------
    def _save_profile(self):
        path = filedialog.asksaveasfilename(
            title="Save Protection Profile",
            defaultextension=".json",
            filetypes=[("JSON", "*.json")],
        )
        if not path:
            return
        if os.path.exists(path):
            if not messagebox.askyesno("Confirm Overwrite", "Profile already exists. Overwrite?", parent=self):
                return
        try:
            self.engine.save_profile(path)
            messagebox.showinfo("Saved", "Protection profile saved.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _load_profile(self):
        path = filedialog.askopenfilename(
            title="Load Protection Profile",
            filetypes=[("JSON", "*.json")],
        )
        if not path:
            return
        try:
            self.engine.load_profile(path)
            messagebox.showinfo("Loaded", "Protection profile loaded and rules refreshed.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _reload(self):
        self.engine.reload_from_cfg()
        self.update_view()

    # ---------- CSV controls ----------
    def _on_toggle_csv(self):
        self.engine.set_events_cfg(self.event_stream, enabled=bool(self.save_csv_var.get()), csv_path=self.csv_path_var.get())

    def _browse_csv(self):
        path = filedialog.asksaveasfilename(
            title="Protection Events CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
        )
        if not path:
            return
        self.csv_path_var.set(path)
        self.engine.set_events_cfg(self.event_stream, enabled=bool(self.save_csv_var.get()), csv_path=path)

    def _clear_history(self):
        self.engine.clear_event_history(self.event_stream)
        self._render_history()


class _RuleEditDialog(tk.Toplevel):
    def __init__(
        self,
        parent,
        *,
        title: str,
        rule_name: str,
        pickup_value,
        delay_value,
        enabled: bool,
        log_to_history: bool,
        highlight_chart: bool,
        scope: str,
        param: str,
        compare: str,
        scope_options: list[str],
        param_options: list[str],
    ):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        self.result = None

        frm = ttk.Frame(self, padding=10)
        frm.grid(row=0, column=0, sticky="nsew")

        # Rule name (display)
        ttk.Label(frm, text="Rule name:").grid(row=0, column=0, sticky="w")
        self.name_var = tk.StringVar(value=rule_name)
        e0 = ttk.Entry(frm, textvariable=self.name_var, width=22)
        e0.grid(row=0, column=1, sticky="w", padx=(8, 0))

        # Scope
        ttk.Label(frm, text="Scope (meter):").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.scope_var = tk.StringVar(value=scope)
        cb_scope = ttk.Combobox(frm, textvariable=self.scope_var, values=scope_options, width=18, state="readonly")
        cb_scope.grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(8, 0))

        # Parameter
        ttk.Label(frm, text="Considering parameter:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.param_var = tk.StringVar(value=param)
        cb_param = ttk.Combobox(frm, textvariable=self.param_var, values=param_options, width=18, state="readonly")
        cb_param.grid(row=2, column=1, sticky="w", padx=(8, 0), pady=(8, 0))

        # Compare
        ttk.Label(frm, text="Trip when:").grid(row=3, column=0, sticky="w", pady=(8, 0))
        self.compare_var = tk.StringVar(value=("LOW" if str(compare).upper()=="LOW" else "HIGH"))
        cb_cmp = ttk.Combobox(frm, textvariable=self.compare_var, values=["HIGH (>)", "LOW (<)"], width=18, state="readonly")
        # store text mapping
        cb_cmp.grid(row=3, column=1, sticky="w", padx=(8, 0), pady=(8, 0))
        # normalize display
        cb_cmp.set("LOW (<)" if self.compare_var.get()=="LOW" else "HIGH (>)")

        # Pickup
        ttk.Label(frm, text="Pickup/Limit:").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.pickup_var = tk.StringVar(value="" if pickup_value is None else f"{float(pickup_value):.4g}")
        e1 = ttk.Entry(frm, textvariable=self.pickup_var, width=18)
        e1.grid(row=4, column=1, sticky="w", padx=(8, 0), pady=(8, 0))

        # Delay
        ttk.Label(frm, text="Delay (s):").grid(row=5, column=0, sticky="w", pady=(8, 0))
        self.delay_var = tk.StringVar(value=f"{float(delay_value):.4g}")
        e2 = ttk.Entry(frm, textvariable=self.delay_var, width=18)
        e2.grid(row=5, column=1, sticky="w", padx=(8, 0), pady=(8, 0))

        self.enabled_var = tk.BooleanVar(value=enabled)
        ttk.Checkbutton(frm, text="Enabled", variable=self.enabled_var).grid(row=6, column=0, columnspan=2, sticky="w", pady=(10, 0))

        self.route_var = tk.BooleanVar(value=log_to_history)
        ttk.Checkbutton(frm, text="Log to Event History (and CSV)", variable=self.route_var).grid(row=7, column=0, columnspan=2, sticky="w")

        self.hl_var = tk.BooleanVar(value=highlight_chart)
        ttk.Checkbutton(frm, text="Highlight chart when active", variable=self.hl_var).grid(row=8, column=0, columnspan=2, sticky="w")

        btns = ttk.Frame(frm)
        btns.grid(row=9, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(btns, text="Cancel", command=self._cancel).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(btns, text="Save", command=self._save).grid(row=0, column=1)

        e0.focus_set()

    def _cancel(self):
        self.result = None
        self.destroy()

    def _save(self):
        try:
            pickup = float(self.pickup_var.get().strip())
            delay = float(self.delay_var.get().strip())
            if delay < 0:
                raise ValueError("Delay must be >= 0")
        except Exception as e:
            messagebox.showerror("Invalid", str(e), parent=self)
            return

        cmp_txt = str(self.compare_var.get())
        if "LOW" in cmp_txt.upper() or "<" in cmp_txt:
            cmp_mode = "LOW"
        else:
            cmp_mode = "HIGH"

        self.result = {
            "rule_name": str(self.name_var.get()),
            "enabled": bool(self.enabled_var.get()),
            "log_to_history": bool(self.route_var.get()),
            "highlight_chart": bool(self.hl_var.get()),
            "scope": str(self.scope_var.get()),
            "param": str(self.param_var.get()),
            "compare": cmp_mode,
            "pickup": pickup,
            "delay": delay,
        }
        self.destroy()
