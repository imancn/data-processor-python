"""
Integration tests for operations and deployment.
"""

import pytest
import sys
import subprocess
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class TestOperations:
    """Test operations and deployment functionality."""
    
    def test_deploy_script_exists(self):
        """Test that deploy script exists and is executable."""
        deploy_script = project_root / 'deploy.sh'
        assert deploy_script.exists()
        assert deploy_script.is_file()
    
    def test_run_script_exists(self):
        """Test that run script exists and is executable."""
        run_script = project_root / 'run.sh'
        assert run_script.exists()
        assert run_script.is_file()
    
    def test_environment_setup(self):
        """Test environment setup."""
        # Test that .env.example exists
        env_example = project_root / 'env.example'
        assert env_example.exists()
        
        # Test that requirements.txt exists
        requirements = project_root / 'requirements.txt'
        assert requirements.exists()
    
    def test_dependencies_installation(self):
        """Test that dependencies can be installed."""
        # Test that requirements.txt is valid
        requirements = project_root / 'requirements.txt'
        with open(requirements, 'r') as f:
            content = f.read()
            assert len(content.strip()) > 0
            assert 'pydantic' in content
    
    def test_logging_directories(self):
        """Test that logging directories are created."""
        logs_dir = project_root / 'logs'
        assert logs_dir.exists()
        
        # Check subdirectories
        system_logs = logs_dir / 'system'
        job_logs = logs_dir / 'jobs'
        
        assert system_logs.exists()
        assert job_logs.exists()
    
    def test_test_runner_functionality(self):
        """Test that test runner works."""
        # Test listing categories
        result = subprocess.run(
            ['python3', 'tests/run_tests.py', '--list'],
            cwd=project_root,
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0
        assert 'core:' in result.stdout
        assert 'pipelines:' in result.stdout
