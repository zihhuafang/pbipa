import pytest
from ipa import *

def test_nearest_divisor():
    assert 4 == nearest_divisor(24, 4.9)
    assert 5 == nearest_divisor(25, 4.9)
    assert 3 == nearest_divisor(21, 4.9)
