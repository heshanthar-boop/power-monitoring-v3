# ui/setup_home.py
from __future__ import annotations

from tkinter import ttk

from ui.setup_tab import SetupTab
from ui.station_tab import StationTab
from ui.maintenance_view import MaintenanceTab


class SetupHomeTab(ttk.Frame):
    """
    Wrapper that provides:
    - Device Setup (400xx) UI (existing SetupTab)
    - Operator Station / Multi-monitor + power options (StationTab)
    - Maintenance / backup / support bundle actions (MaintenanceTab)
    """
    def __init__(
        self,
        parent,
        cfg: dict,
        meters,
        root_tk,
        station_apply_cb,
        keep_awake_cb,
        auth_service=None,
        session=None,
        logging_engine=None,
        data_store=None,
        snapshot_bus=None,
        worker=None,
    ):
        super().__init__(parent, padding=0)
        self.cfg = cfg
        self.meters = meters
        self.auth_service = auth_service
        self.session = dict(session or {})
        self.logging_engine = logging_engine
        self.data_store = data_store
        self.snapshot_bus = snapshot_bus
        self.worker = worker

        banner = ttk.Frame(self, padding=(10,8))
        banner.pack(fill="x")
        ttk.Label(banner, text="CONFIGURATION MODE", style="H2.TLabel").pack(side="left")
        ttk.Label(banner, text="Monitoring continues, but writes are locked unless you unlock. Apply changes intentionally.", style="Muted.TLabel").pack(side="left", padx=10)

        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)

        self.tab_device = ttk.Frame(nb)
        self.tab_station = ttk.Frame(nb)
        self.tab_maintenance = ttk.Frame(nb)

        nb.add(self.tab_device, text="Device Setup (400xx)")
        nb.add(self.tab_station, text="Operator Station")
        nb.add(self.tab_maintenance, text="Maintenance & Backup")

        # Existing setup UI
        self.device_ui = SetupTab(self.tab_device, cfg, meters)
        self.device_ui.pack(fill="both", expand=True)

        # New station UI
        self.station_ui = StationTab(self.tab_station, cfg, root_tk, on_apply=station_apply_cb, on_keep_awake=keep_awake_cb)
        self.station_ui.pack(fill="both", expand=True)

        # Maintenance / backup / field support
        self.maintenance_ui = MaintenanceTab(
            self.tab_maintenance,
            cfg,
            auth_service=auth_service,
            session=self.session,
            logging_engine=logging_engine,
            data_store=data_store,
            snapshot_bus=snapshot_bus,
            worker=worker,
        )
        self.maintenance_ui.pack(fill="both", expand=True)

    def set_worker(self, worker):
        self.worker = worker
        # Forward worker to SetupTab
        if hasattr(self.device_ui, "set_worker"):
            self.device_ui.set_worker(worker)
        if hasattr(self, "maintenance_ui"):
            try:
                self.maintenance_ui.worker = worker
            except Exception:
                pass
