import tkinter as tk
from tkinter import ttk


class ScrollableContainer(ttk.Frame):
    """Vertical scroll container intended for Notebook tabs.

    Child widgets should be created/packed/gridded inside `self.body`.
    """

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)

        self.canvas = tk.Canvas(self, highlightthickness=0, borderwidth=0)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)

        self.vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        # Inner frame that holds the actual content
        self.body = ttk.Frame(self.canvas)
        self._win = self.canvas.create_window((0, 0), window=self.body, anchor="nw")

        # Keep scrollregion and width synced
        self.body.bind("<Configure>", self._on_body_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        # Mousewheel scrolling: bind only while pointer/focus is inside this container.
        # This avoids global wheel hijacking across tabs and external windows.
        self._wheel_bound = False
        self.canvas.bind("<Enter>", self._bind_mousewheel, add="+")
        self.canvas.bind("<Leave>", self._unbind_mousewheel, add="+")
        self.body.bind("<Enter>", self._bind_mousewheel, add="+")
        self.body.bind("<Leave>", self._unbind_mousewheel, add="+")


    def _bind_mousewheel(self, _event=None):
        if self._wheel_bound:
            return
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Shift-MouseWheel>", self._on_shift_mousewheel)
        self._wheel_bound = True

    def _unbind_mousewheel(self, _event=None):
        # Only release when pointer truly leaves this container.
        try:
            x, y = self.winfo_pointerx(), self.winfo_pointery()
            w = self.winfo_containing(x, y)
            if w is not None and self._is_descendant(w, self):
                return
        except Exception:
            pass
        if not self._wheel_bound:
            return
        try:
            self.canvas.unbind_all("<MouseWheel>")
            self.canvas.unbind_all("<Shift-MouseWheel>")
        finally:
            self._wheel_bound = False

    def _on_body_configure(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        # Make inner frame match the visible canvas width
        self.canvas.itemconfigure(self._win, width=event.width)

    def _on_mousewheel(self, event):
        # Only scroll if pointer is over *this* scroll container (or any child inside it).
        # We use bind_all(), so we must guard carefully to avoid hijacking other tabs.
        x, y = self.winfo_pointerx(), self.winfo_pointery()
        w = self.winfo_containing(x, y)
        if w is None:
            return
        if not self._is_descendant(w, self):
            return
        # Windows delta is 120 per notch
        delta = int(-1 * (event.delta / 120))
        self.canvas.yview_scroll(delta, "units")

    @staticmethod
    def _is_descendant(widget, ancestor):
        try:
            while widget is not None:
                if widget == ancestor:
                    return True
                widget = widget.master
        except Exception:
            return False
        return False

    def _on_shift_mousewheel(self, event):
        # don't scroll vertically on Shift+Wheel (reserved for chart zoom in some views)
        return

    # ------------------------------------------------------------------
    # Pass-through: allow MainWindow to call update_view() on the notebook tab.
    # ------------------------------------------------------------------
    def update_view(self):
        """Forward update_view to the first child inside the body that supports it."""
        try:
            for child in self.body.winfo_children():
                if hasattr(child, "update_view"):
                    child.update_view()
                    break
        except Exception:
            pass
