"""
Integration Tests - Framework Integration
"""

import pytest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core import config, setup_logging, validate_config
from core.exceptions import ValidationError


class TestFrameworkIntegration:
    """Test complete framework integration."""
    
    def test_core_system_integration(self):
        """Test core system components integration."""
        assert config is not None
        assert hasattr(config, 'log_level')
        assert hasattr(config, 'timeout')
        
        setup_logging('INFO')
        assert True
        
        test_config = {
            'log_level': 'INFO',
            'timeout': 30,
            'batch_size': 1000
        }
        
        validated_config = validate_config(test_config)
        assert validated_config.log_level == 'INFO'
        assert validated_config.timeout == 30
    
    def test_error_handling_integration(self):
        """Test error handling integration."""
        invalid_config = {
            'log_level': 'INVALID_LEVEL',
            'timeout': -10
        }
        
        with pytest.raises(ValidationError):
            validate_config(invalid_config)


if __name__ == '__main__':
    pytest.main([__file__])
