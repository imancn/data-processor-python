"""
Test discovery utilities for the data processing framework.
"""

from pathlib import Path
from typing import List, Dict, Any
from .test_config import TestFrameworkConfig, TestCategoryConfig


class TestFrameworkDiscovery:
    """Handles test file discovery and validation."""
    
    def __init__(self, config: TestFrameworkConfig):
        self.config = config
        self.project_root = config.project_root
    
    def discover_test_files(self, category: str) -> List[str]:
        """Discover test files for a category."""
        if category == 'all':
            return self._discover_all_test_files()
        
        category_info = self.config.get_category(category)
        if not category_info:
            return []
        
        # Filter existing files
        existing_files = []
        for test_file in category_info.test_files:
            file_path = self.project_root / test_file
            if file_path.exists():
                existing_files.append(test_file)
            else:
                print(f"Warning: Test file not found: {test_file}")
        
        return existing_files
    
    def _discover_all_test_files(self) -> List[str]:
        """Discover all test files in the project."""
        test_files = []
        test_dirs = ['tests/unit', 'tests/integration', 'tests/performance']
        
        for test_dir in test_dirs:
            test_path = self.project_root / test_dir
            if test_path.exists():
                for test_file in test_path.glob('test_*.py'):
                    test_files.append(str(test_file.relative_to(self.project_root)))
        
        return test_files
    
    def get_test_files_by_type(self, test_type: str) -> List[str]:
        """Get test files filtered by type."""
        test_files = []
        test_dir = f'tests/{test_type}'
        test_path = self.project_root / test_dir
        
        if test_path.exists():
            for test_file in test_path.glob('test_*.py'):
                test_files.append(str(test_file.relative_to(self.project_root)))
        
        return test_files
    
    def validate_test_files(self, test_files: List[str]) -> Dict[str, Any]:
        """Validate test files and return status."""
        valid_files = []
        invalid_files = []
        
        for test_file in test_files:
            file_path = self.project_root / test_file
            if file_path.exists():
                valid_files.append(test_file)
            else:
                invalid_files.append(test_file)
        
        return {
            'valid_files': valid_files,
            'invalid_files': invalid_files,
            'total_valid': len(valid_files),
            'total_invalid': len(invalid_files)
        }
    
    def get_test_statistics(self, category: str) -> Dict[str, Any]:
        """Get statistics about test files for a category."""
        test_files = self.discover_test_files(category)
        validation = self.validate_test_files(test_files)
        
        return {
            'category': category,
            'total_files': len(test_files),
            'valid_files': validation['total_valid'],
            'invalid_files': validation['total_invalid'],
            'files': test_files
        }
