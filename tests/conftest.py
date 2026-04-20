"""
pytest conftest — add project root to sys.path so imports work.
"""
import sys
import os

# Ensure 'core', 'ui', 'utils', 'config' are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
