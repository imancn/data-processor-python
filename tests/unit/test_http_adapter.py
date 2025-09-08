import os
import sys

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

def test_http_adapter_importable():
    from adapters._bases.http_adapter import HttpAdapter
    assert HttpAdapter is not None


