import os
import sys

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from crons.registry import JOBS


def test_registry_has_cmc_job():
    assert 'cmc_hourly_prices' in JOBS and callable(JOBS['cmc_hourly_prices'])


