#!/usr/bin/env python3
"""
Test runner for the data processing framework.
Runs comprehensive tests and generates detailed results.

This script is a wrapper around the new unified test framework.
"""
import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def main():
    """Main function - delegates to the unified test framework."""
    # Import the main function from the unified test runner
    from tests.framework.run_tests import main as unified_main
    
    # Call the unified main function
    return unified_main()


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)