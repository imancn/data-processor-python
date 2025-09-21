#!/usr/bin/env python3
"""
Test runner for the data processing framework.

This is a wrapper that delegates to the framework implementation.
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import and run the framework test runner
from tests.framework.run_tests import main

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)