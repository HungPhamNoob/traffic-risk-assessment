# tests/test_smoke.py
"""Smoke test để verify pytest configuration"""
import pytest


def test_pytest_works():
    """Test này luôn pass - dùng để verify setup"""
    assert True


def test_python_version():
    """Verify Python version trong CI"""
    import sys

    assert sys.version_info.major == 3
    assert sys.version_info.minor >= 10


def test_requirements_importable():
    """Verify core packages có thể import"""
    try:
        # Fix F401: Thêm noqa để báo flake8 đây là intentional import
        import pandas  # noqa: F401
        import numpy  # noqa: F401

        assert True
    except ImportError as e:
        pytest.fail(f"Missing dependency: {e}")
