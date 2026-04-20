"""
config/features.py — Single source of truth for application feature keys.

Every tab/page in the UI has one key here.  The same list is used by:
  - core/auth_service.py  (permission checks, default role assignments)
  - ui/access_view.py     (permission grid display)
  - config/schema.py      (default config structure, if needed)

Adding a new tab: add its key here, add its label in ui/access_view.py,
add it to the appropriate FEATURE_GROUPS entry in access_view.py.
"""
from __future__ import annotations

# Ordered list — defines display order in permission grid.
FEATURE_KEYS: list[str] = [
    # Start
    "home",
    # Operations
    "dashboard",
    "status",
    "data_graphs",
    # Insights
    "analytics",
    "intelligence",
    "solar_kpi",
    "hydro_kpi",
    "load_kpi",
    # Events
    "alarms",
    "incident_summary",
    "protection",
    # Records
    "logging",
    "reports",
    "journal",
    # Settings
    "setup",
    "comm",
    "connectivity",
    "email",
    "access_control",
]

# Default permissions granted to each role.
# These are the factory defaults; individual users can be customised
# beyond these via auth_service.upsert_user() / auth_service.set_permissions().
DEFAULT_ROLE_PERMISSIONS: dict[str, set[str]] = {
    "operator": {
        "home",
        "dashboard",
        "status",
        "data_graphs",
        "analytics",
        "intelligence",
        "solar_kpi",
        "hydro_kpi",
        "load_kpi",
        "protection",
        "alarms",
        "incident_summary",
        "journal",
    },
    "owner": {
        "home",
        "dashboard",
        "status",
        "data_graphs",
        "analytics",
        "intelligence",
        "solar_kpi",
        "hydro_kpi",
        "load_kpi",
        "alarms",
        "incident_summary",
        "journal",
        "reports",
    },
    "engineer": {
        "home",
        "dashboard",
        "status",
        "data_graphs",
        "analytics",
        "intelligence",
        "solar_kpi",
        "hydro_kpi",
        "load_kpi",
        "protection",
        "alarms",
        "incident_summary",
        "journal",
        "logging",
        "reports",
        "setup",
        "comm",
        "connectivity",
        "email",
    },
}
