# tests/test_smoke.py
"""Smoke test để verify pytest configuration"""

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
        import pandas
        import numpy
        assert True
    except ImportError as e:
        pytest.fail(f"Missing dependency: {e}")