from __future__ import annotations

import tkinter as tk
from typing import Optional

from ui.styles import get_theme


def page(parent) -> tk.Frame:
    t = get_theme()
    root = tk.Frame(parent, bg=t.bg)
    root.pack(fill="both", expand=True)
    return root


def header(parent, title: str, subtitle: str = "", *, accent: Optional[str] = None) -> tk.Frame:
    t = get_theme()
    accent = accent or t.accent
    wrap = tk.Frame(parent, bg=t.bg)
    wrap.pack(fill="x", padx=12, pady=(12, 8))

    card = tk.Frame(wrap, bg="#0b1220", highlightthickness=1, highlightbackground=t.border)
    card.pack(fill="x")
    tk.Frame(card, bg=accent, height=3).pack(fill="x", side="top")

    body = tk.Frame(card, bg="#0b1220")
    body.pack(fill="x", padx=14, pady=12)
    tk.Label(
        body,
        text=title,
        bg="#0b1220",
        fg=t.text,
        font=("Segoe UI", 17, "bold"),
    ).pack(side="left")
    if subtitle:
        tk.Label(
            body,
            text=subtitle,
            bg="#0b1220",
            fg=t.text_muted,
            font=("Segoe UI", 9),
        ).pack(side="left", padx=(12, 0), pady=(5, 0))
    return body


def card(parent, *, accent: Optional[str] = None, padx: int = 12, pady=(0, 10)) -> tk.Frame:
    t = get_theme()
    frame = tk.Frame(parent, bg=t.card, highlightthickness=1, highlightbackground=t.border)
    frame.pack(fill="both", expand=True, padx=padx, pady=pady)
    if accent:
        tk.Frame(frame, bg=accent, height=2).pack(fill="x", side="top")
    return frame


def metric(parent, title: str, value_var: tk.StringVar, *, accent: Optional[str] = None, col: int = 0) -> tk.Frame:
    t = get_theme()
    accent = accent or t.accent
    frame = tk.Frame(parent, bg=t.card, highlightthickness=1, highlightbackground=t.border)
    frame.grid(row=0, column=col, sticky="nsew", padx=(0 if col == 0 else 8, 0))
    tk.Frame(frame, bg=accent, height=3).pack(fill="x", side="top")
    tk.Label(frame, text=title.upper(), bg=t.card, fg=t.text_muted, font=("Segoe UI", 8, "bold")).pack(
        anchor="w", padx=10, pady=(8, 0)
    )
    tk.Label(frame, textvariable=value_var, bg=t.card, fg=t.text, font=("Segoe UI", 16, "bold")).pack(
        anchor="w", padx=10, pady=(4, 10)
    )
    return frame
