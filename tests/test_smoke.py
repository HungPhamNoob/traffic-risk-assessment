# tests/test_smoke.py
"""Smoke tests for the local Python test setup."""
import pytest


def test_pytest_works():
    """Verify that pytest can collect and run this repository."""
    assert True


def test_python_version():
    """Verify Python version trong CI"""
    import sys

    assert sys.version_info.major == 3
    assert sys.version_info.minor >= 10


def test_requirements_importable():
    """Verify that core data dependencies are importable."""
    try:
        import pandas  # noqa: F401
        import numpy  # noqa: F401

        assert True
    except ImportError as e:
        pytest.fail(f"Missing dependency: {e}")
