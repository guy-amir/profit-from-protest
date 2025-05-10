"""
Test cases for pfp core functionality.
"""
import pytest
from pfp.core import hello_world


def test_hello_world():
    """Test that hello_world returns the expected greeting."""
    result = hello_world()
    assert f"Hello from pfp!" in result
