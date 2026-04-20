#!/usr/bin/env python3
"""
Test runner for MFM384 SCADA test suite.

Usage:
    python run_tests.py              # run all tests, verbose
    python run_tests.py -v           # same, explicit
    python run_tests.py -q           # quiet (dots only)
    python run_tests.py test_alarm   # run only tests matching pattern

Or via pytest (preferred):
    pip install pytest
    pytest tests/ -v
"""
import sys
import os
import unittest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "test*.py"
    verbosity = 1 if "-q" in sys.argv else 2

    loader = unittest.TestLoader()
    start_dir = os.path.join(os.path.dirname(__file__), "tests")
    suite = loader.discover(start_dir, pattern=pattern)

    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
