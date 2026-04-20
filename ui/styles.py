"""ui.styles
=========
Premium, low-fatigue ttk design system (PyInstaller-safe).

Revision 2 — comprehensive component library:
  • Richer layered surface palette  (bg → panel → card → surface)
  • Full font scale  (h1 … caption, kpi_val, mono)
  • Extended style catalogue  (Form sections, Badge labels, Ghost/Danger buttons …)
  • Global  get_theme()  accessor so any view can read palette tokens
  • Progressive-bar colour variants  (Good / Warn / Alarm)
  • Polished scrollbar, Treeview headings, Combobox, Entry focus ring

Rules
-----
  • Never pure black / white — use palette tones.
  • Red is reserved for live alarms and ALARM state.
  • Accent (blue) is for primary actions and live data highlight.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from tkinter import Tk
from tkinter import ttk

try:
    import sv_ttk as _sv_ttk
except ImportError:
    _sv_ttk = None


# ── Spacing tokens ─────────────────────────────────────────────────────────────
SPACING = {
    "xs":  4,
    "s":   8,
    "m":  12,
    "l":  16,
    "xl": 24,
    "xxl":36,
}

ROWH   = 28   # standard Treeview row height
RADIUS = 6    # logical corner-radius (Canvas-drawn widgets)


# ── Theme dataclass ─────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Theme:
    # Surfaces — layered depth (darker → lighter on dark theme)
    bg:       str    # root / page background
    panel:    str    # sidebar, header bar
    card:     str    # content cards, LabelFrames
    surface:  str    # elevated: dropdowns, hover backgrounds
    border:   str    # card / widget borders
    divider:  str    # hairline row separators
    input_bg: str    # text-entry / combobox field background

    # Text hierarchy
    text:       str  # primary body text
    text_muted: str  # secondary labels, hints
    text_dim:   str  # captions, placeholders

    # Status / accent
    accent:    str   # primary accent (blue)
    accent_lt: str   # lighter tint for hover / pressed
    good:      str   # healthy / OK
    warn:      str   # caution / approaching threshold
    alarm:     str   # active alarm / critical
    stale:     str   # offline / no recent data

    # Interaction
    selection:  str  # Treeview row selection, active nav button
    focus_ring: str  # keyboard focus border


# ── Dark — eye-comfort control-room theme ────────────────────────────────────────
# Warm-tinted dark: reduces blue fatigue for long shifts.
# Surfaces use a slight warm-grey base (not pure cold blue-black).
# Text contrast is kept at comfortable reading level — not glaring white.
CONTROL_ROOM_DARK = Theme(
    bg       = "#141414",   # warm near-black — easier than pure cold black
    panel    = "#1c1c1e",   # sidebar / header — slight warm lift
    card     = "#242426",   # content cards
    surface  = "#2c2c2e",   # dropdowns, hover states
    border   = "#38383a",   # subtle borders — not too harsh
    divider  = "#28282a",   # hairline separators
    input_bg = "#1a1a1c",   # input fields
    text       = "#e8e4dc",   # warm off-white — far less eye strain than #ffffff
    text_muted = "#9a9490",   # secondary labels — warm grey
    text_dim   = "#5e5a56",   # captions — warm dim
    accent    = "#4da6ff",   # blue accent — slightly warmer tone
    accent_lt = "#74bcff",   # hover tint
    good      = "#34c768",   # green — slightly warmer, less neon
    warn      = "#f0a030",   # amber — warm, not too bright
    alarm     = "#e84040",   # red alarm — clear but not harsh
    stale     = "#5e5a56",   # grey for stale/offline
    selection  = "#1a4fd6",   # active nav selection
    focus_ring = "#4da6ff",   # focus border
)

# ── Light — bright office ────────────────────────────────────────────────────────
OFFICE_LIGHT = Theme(
    bg       = "#edf1f7",
    panel    = "#ffffff",
    card     = "#f4f7fc",
    surface  = "#e8edf6",
    border   = "#cdd6e8",
    divider  = "#e0e8f2",
    input_bg = "#ffffff",
    text       = "#18243a",
    text_muted = "#4e637e",
    text_dim   = "#8096b0",
    accent    = "#2563eb",
    accent_lt = "#3b76f7",
    good      = "#16a34a",
    warn      = "#b45309",
    alarm     = "#dc2626",
    stale     = "#6b7280",
    selection  = "#2563eb",
    focus_ring = "#2563eb",
)


# ── Global accessor ──────────────────────────────────────────────────────────────
_active_theme: Optional[Theme] = None


def get_theme() -> Theme:
    """Return the currently active Theme (set by apply_styles).

    Views and widgets can call this after the main window is built to get
    palette tokens for canvas drawing, matplotlib, or raw tk.Label colours.
    """
    return _active_theme or CONTROL_ROOM_DARK


def _pick_theme(cfg: Optional[dict]) -> Theme:
    name = "dark"
    try:
        name = str(((cfg or {}).get("ui", {}) or {}).get("theme", "dark")).strip().lower()
    except Exception:
        name = "dark"
    return OFFICE_LIGHT if name == "light" else CONTROL_ROOM_DARK


# ── Main entry point ─────────────────────────────────────────────────────────────
def apply_styles(root: Tk, cfg: Optional[dict] = None) -> Theme:
    """Apply the full ttk style set.  Returns the active Theme."""
    global _active_theme
    t = _pick_theme(cfg)
    _active_theme = t
    s = ttk.Style(root)

    # Base skin (sv-ttk gives rounded corners + flat look; we override palette on top)
    if _sv_ttk is not None:
        try:
            _sv_ttk.set_theme("dark" if t is CONTROL_ROOM_DARK else "light", root=root)
        except Exception:
            try: s.theme_use("clam")
            except Exception: pass
    else:
        try: s.theme_use("clam")
        except Exception: pass

    # ── Font scale ────────────────────────────────────────────────────────────
    F = {
        "h1":      ("Segoe UI", 18, "bold"),
        "h2":      ("Segoe UI", 14, "bold"),
        "h3":      ("Segoe UI", 12, "bold"),
        "body":    ("Segoe UI", 10),
        "body_b":  ("Segoe UI", 10, "bold"),
        "small":   ("Segoe UI",  9),
        "small_b": ("Segoe UI",  9, "bold"),
        "caption": ("Segoe UI",  8),
        "mono":    ("Consolas",  10),
        "mono_b":  ("Consolas",  10, "bold"),
        # KPI display — deliberately large for control-room readability
        "kpi_xl":  ("Segoe UI", 26, "bold"),
        "kpi_lg":  ("Segoe UI", 22, "bold"),
        "kpi_md":  ("Segoe UI", 18, "bold"),
    }

    # Root window
    try:
        root.configure(background=t.bg)
    except Exception:
        pass

    # ── Frames ────────────────────────────────────────────────────────────────
    s.configure("TFrame",             background=t.bg)
    s.configure("App.Panel.TFrame",   background=t.panel)
    s.configure("Card.TFrame",        background=t.card)
    s.configure("Section.TFrame",     background=t.panel)
    s.configure("Inset.TFrame",       background=t.input_bg)
    s.configure("Surface.TFrame",     background=t.surface)

    # ── Separators ────────────────────────────────────────────────────────────
    s.configure("TSeparator",         background=t.divider)
    s.configure("VSep.TSeparator",    background=t.divider)

    # ── LabelFrames ───────────────────────────────────────────────────────────
    # Standard section card  (tile grids, target bars)
    s.configure("TLabelframe",
        background   = t.card,
        borderwidth  = 1,
        relief       = "groove",
        padding      = (SPACING["l"], SPACING["m"]),
    )
    s.configure("TLabelframe.Label",
        background = t.card,
        foreground = t.text_muted,
        font       = F["small_b"],
        padding    = (4, 0),
    )

    # Form / config panel  (accent title, more breathing room)
    s.configure("Form.TLabelframe",
        background   = t.card,
        borderwidth  = 1,
        relief       = "groove",
        padding      = (SPACING["l"], SPACING["l"]),
    )
    s.configure("Form.TLabelframe.Label",
        background = t.card,
        foreground = t.accent,
        font       = F["body_b"],
        padding    = (6, 0),
    )

    # KPI tile frame  (same as TLabelframe but explicit for future override)
    s.configure("Tile.TLabelframe",
        background   = t.card,
        borderwidth  = 1,
        relief       = "groove",
        padding      = (SPACING["m"], SPACING["s"]),
    )
    s.configure("Tile.TLabelframe.Label",
        background = t.card,
        foreground = t.text,
        font       = F["small_b"],
    )

    # ── Labels ────────────────────────────────────────────────────────────────
    # Base
    s.configure("TLabel",      background=t.bg,    foreground=t.text,       font=F["body"])
    s.configure("Muted.TLabel",background=t.bg,    foreground=t.text_muted, font=F["body"])
    s.configure("Dim.TLabel",  background=t.bg,    foreground=t.text_dim,   font=F["small"])
    s.configure("Mono.TLabel", background=t.bg,    foreground=t.text,       font=F["mono"])
    s.configure("MonoBold.TLabel", background=t.bg,foreground=t.text,       font=F["mono_b"])

    # Header hierarchy
    s.configure("H1.TLabel",   background=t.bg,    foreground=t.text,       font=F["h1"])
    s.configure("H2.TLabel",   background=t.bg,    foreground=t.text,       font=F["h2"])
    s.configure("H3.TLabel",   background=t.bg,    foreground=t.text,       font=F["h3"])
    s.configure("Header.TLabel",      background=t.bg,  foreground=t.text,       font=F["h2"])
    s.configure("HeaderLine.TLabel",  background=t.bg,  foreground=t.text,       font=F["body_b"])
    s.configure("HeaderLineMuted.TLabel", background=t.bg, foreground=t.text_muted, font=F["small"])
    s.configure("Caption.TLabel",     background=t.card, foreground=t.text_dim,   font=F["caption"])

    # Card-background labels (used inside LabelFrames / tiles)
    s.configure("SectionTitle.TLabel", background=t.card, foreground=t.accent,     font=F["small_b"])
    s.configure("FieldLabel.TLabel",   background=t.card, foreground=t.text_muted, font=F["small"])

    # Status text colours
    s.configure("Good.TLabel",  background=t.bg,   foreground=t.good,  font=F["body_b"])
    s.configure("Warn.TLabel",  background=t.bg,   foreground=t.warn,  font=F["body_b"])
    s.configure("Alarm.TLabel", background=t.bg,   foreground=t.alarm, font=F["body_b"])
    s.configure("Stale.TLabel", background=t.bg,   foreground=t.stale, font=F["body_b"])

    # Health strip / status badges
    s.configure("HealthBar.TFrame", background=t.panel)
    s.configure("HealthTitle.TLabel", background=t.panel, foreground=t.text_muted, font=F["small_b"])
    s.configure("HealthInfo.TLabel", background=t.panel, foreground=t.text, font=F["small"])

    # KPI tile typography  (card background)
    s.configure("TileTitle.TLabel",  background=t.card, foreground=t.text_muted, font=F["small_b"])
    s.configure("TileValue.TLabel",  background=t.card, foreground=t.accent,     font=F["kpi_lg"])
    s.configure("TileMedVal.TLabel", background=t.card, foreground=t.accent,     font=F["kpi_md"])
    s.configure("TileUnit.TLabel",   background=t.card, foreground=t.text_dim,   font=F["small"])
    s.configure("TileInfo.TLabel",   background=t.card, foreground=t.text_dim,   font=F["caption"])
    s.configure("TileStale.TLabel",  background=t.card, foreground=t.stale,      font=F["body_b"])

    # ── Buttons ───────────────────────────────────────────────────────────────
    # Default
    s.configure("TButton",  font=F["body_b"], padding=(12, 7), relief="flat")
    s.map("TButton",
        background=[("active", t.surface)],
        foreground=[("active", t.text)],
    )

    # Primary / Accent  (blue fill)
    s.configure("Primary.TButton",  font=F["body_b"], padding=(12, 7), relief="flat")
    s.map("Primary.TButton",
        background=[("!disabled", t.accent), ("active", t.accent_lt), ("disabled", t.border)],
        foreground=[("!disabled", "#ffffff"), ("disabled", t.stale)],
    )
    s.configure("Accent.TButton",   font=F["body_b"], padding=(12, 7), relief="flat")
    s.map("Accent.TButton",
        background=[("!disabled", t.accent), ("active", t.accent_lt)],
        foreground=[("!disabled", "#ffffff")],
    )

    # Danger  (disconnect, delete)
    s.configure("Danger.TButton",   font=F["body_b"], padding=(12, 7), relief="flat")
    s.map("Danger.TButton",
        background=[("!disabled", "#b91c1c"), ("active", "#dc2626")],
        foreground=[("!disabled", "#ffffff")],
    )

    # Ghost  (secondary / tertiary actions)
    s.configure("Ghost.TButton",    font=F["body"],   padding=(10, 6), relief="flat")
    s.map("Ghost.TButton",
        background=[("active", t.surface)],
        foreground=[("active", t.text), ("!disabled", t.text_muted)],
    )

    # Small utility
    s.configure("Small.TButton",    font=F["small_b"], padding=(8, 4), relief="flat")
    s.map("Small.TButton",
        background=[("active", t.surface)],
        foreground=[("active", t.text)],
    )

    # Chart controls
    s.configure("Hold.TCheckbutton", font=F["body_b"], padding=(6, 2))
    s.map("Hold.TCheckbutton",
        foreground=[("selected", t.warn), ("!selected", t.text_muted)],
    )
    s.configure("ChartHeader.TFrame", background=t.card)
    s.configure("Interval.TButton",  font=F["small"], padding=(8, 4), relief="flat")
    s.map("Interval.TButton",
        background=[("active", t.card)],
        foreground=[("active", t.text)],
    )
    s.configure("IntervalActive.TButton", font=F["small_b"], padding=(8, 4), relief="flat")
    s.map("IntervalActive.TButton",
        background=[("!disabled", t.accent), ("active", t.accent_lt)],
        foreground=[("!disabled", "#ffffff")],
    )
    s.configure("Nav.TButton", font=("Segoe UI", 11, "bold"), padding=(10, 6), relief="flat")
    s.map("Nav.TButton",
        background=[("active", t.panel)],
        foreground=[("active", t.text)],
    )
    s.configure("NavLive.TButton", font=("Segoe UI", 11, "bold"), padding=(10, 6), relief="flat")
    s.map("NavLive.TButton",
        background=[("!disabled", t.card), ("active", t.panel)],
        foreground=[("!disabled", t.text_muted), ("active", t.text)],
    )

    # ── Sidebar ────────────────────────────────────────────────────────────────
    s.configure("Sidebar.TFrame",      background=t.panel)
    s.configure("SidebarGroup.TFrame", background=t.panel)
    # Section group header — visually distinct from nav items
    s.configure("SidebarHeader.TButton",
        font=("Segoe UI", 8, "bold"), padding=(10, 5), relief="flat",
        background=t.bg, foreground=t.text_dim,
    )
    s.map("SidebarHeader.TButton",
        background=[("active", t.bg)],
        foreground=[("active", t.text_muted), ("!disabled", t.text_dim)],
    )
    # Nav item — clear, readable, left-anchored
    s.configure("SidebarNav.TButton",
        font=F["body"], padding=(14, 7), relief="flat",
        background=t.panel, foreground=t.text, anchor="w",
    )
    s.map("SidebarNav.TButton",
        background=[("active", t.surface)],
        foreground=[("active", t.text)],
    )
    # Active nav item — full accent highlight
    s.configure("SidebarNavActive.TButton",
        font=F["body_b"], padding=(14, 7), relief="flat",
        background=t.selection, foreground="#ffffff", anchor="w",
    )
    s.map("SidebarNavActive.TButton",
        background=[("active", t.selection), ("!disabled", t.selection)],
        foreground=[("!disabled", "#ffffff")],
    )
    s.configure("SidebarToggle.TButton",
        font=("Segoe UI", 13, "bold"), padding=(6, 4), relief="flat",
        background=t.panel, foreground=t.text,
    )
    s.map("SidebarToggle.TButton",
        background=[("active", t.surface)],
        foreground=[("active", t.text)],
    )
    s.configure("SidebarTitle.TLabel",
        font=F["caption"], background=t.panel, foreground=t.text_dim,
    )

    # ── Notebook tabs ──────────────────────────────────────────────────────────
    s.configure("TNotebook",
        background=t.bg, borderwidth=0, tabmargins=(0, 2, 0, 0),
    )
    s.configure("TNotebook.Tab",
        padding=(14, 8), background=t.panel,
        foreground=t.text_muted, font=F["body"],
    )
    s.map("TNotebook.Tab",
        background=[("selected", t.card),  ("active", t.surface)],
        foreground=[("selected", t.text),  ("active", t.text)],
        font     =[("selected", F["body_b"])],
    )
    s.configure("Sub.TNotebook", background=t.bg, borderwidth=0)
    s.configure("Sub.TNotebook.Tab",
        padding=(12, 7), background=t.panel,
        foreground=t.text_muted, font=F["small"],
    )
    s.map("Sub.TNotebook.Tab",
        background=[("selected", t.card),  ("active", t.surface)],
        foreground=[("selected", t.text),  ("active", t.text)],
        font     =[("selected", F["small_b"])],
    )

    # ── Inputs ─────────────────────────────────────────────────────────────────
    s.configure("TEntry",
        padding=(8, 6),
        fieldbackground=t.input_bg,
        foreground=t.text, insertcolor=t.text,
        bordercolor=t.border,
        lightcolor=t.border, darkcolor=t.border,
    )
    s.map("TEntry",
        fieldbackground=[("focus", t.input_bg)],
        bordercolor    =[("focus", t.focus_ring)],
    )
    s.configure("TCombobox",
        padding=(6, 5),
        fieldbackground=t.input_bg,
        foreground=t.text,
        selectbackground=t.selection,
        arrowcolor=t.text_muted,
    )
    s.map("TCombobox",
        fieldbackground =[("readonly", t.input_bg)],
        selectbackground=[("readonly", t.selection)],
    )
    s.configure("TCheckbutton",
        background=t.card, foreground=t.text, font=F["body"],
    )
    s.map("TCheckbutton",
        background=[("active", t.card)],
        foreground=[("active", t.text)],
    )
    s.configure("TSpinbox",
        padding=(6, 5), fieldbackground=t.input_bg, foreground=t.text,
    )

    # ── Meter selector (top-right checkboxes) ─────────────────────────────────
    s.configure("MeterSel.Label.TLabel",
        font=F["body_b"], foreground=t.text_muted, background=t.bg,
    )
    for name, fg in (
        ("MeterSel.On.TCheckbutton",    t.good),
        ("MeterSel.Stale.TCheckbutton", t.warn),
        ("MeterSel.Off.TCheckbutton",   t.stale),
    ):
        s.configure(name,
            font=F["body_b"], foreground=fg, background=t.bg, padding=(6, 2),
        )
        s.map(name,
            foreground=[("disabled", t.stale), ("selected", fg), ("!selected", fg)],
            background=[("active", t.bg), ("!active", t.bg)],
        )

    # ── Treeview ───────────────────────────────────────────────────────────────
    s.configure("Treeview",
        background      = t.panel,
        fieldbackground  = t.panel,
        foreground       = t.text,
        rowheight        = ROWH,
        bordercolor      = t.border,
        font             = F["body"],
    )
    s.configure("Treeview.Heading",
        background  = t.surface,
        foreground  = t.text,
        font        = F["small_b"],
        relief      = "flat",
        bordercolor = t.border,
        padding     = (6, 5),
    )
    s.map("Treeview",
        background=[("selected", t.selection)],
        foreground=[("selected", "#ffffff")],
    )
    s.map("Treeview.Heading",
        background=[("active", t.card)],
    )

    # ── Progressbar variants ──────────────────────────────────────────────────
    _pb_base = dict(bordercolor=t.border, troughcolor=t.surface, thickness=12)
    s.configure("TProgressbar",       background=t.accent, **_pb_base,
                darkcolor=t.accent,   lightcolor=t.accent)
    s.configure("Thin.TProgressbar",  background=t.accent, **{**_pb_base, "thickness": 6})
    s.configure("Good.TProgressbar",  background=t.good,   **_pb_base)
    s.configure("Warn.TProgressbar",  background=t.warn,   **_pb_base)
    s.configure("Alarm.TProgressbar", background=t.alarm,  **_pb_base)

    # ── Scrollbar ─────────────────────────────────────────────────────────────
    s.configure("TScrollbar",
        background  = t.panel,
        troughcolor = t.bg,
        bordercolor = t.bg,
        arrowcolor  = t.text_muted,
        arrowsize   = 14,
        gripcount   = 0,
    )
    s.map("TScrollbar",
        background=[("active", t.surface), ("pressed", t.border)],
        arrowcolor=[("active", t.text)],
    )

    # ── Scale ──────────────────────────────────────────────────────────────────
    s.configure("TScale",
        background   = t.bg,
        troughcolor  = t.surface,
        sliderlength = 18,
        sliderrelief = "flat",
    )

    return t
