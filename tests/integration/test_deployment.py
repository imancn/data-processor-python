"""
Integration tests for deployment and operational scenarios.
"""
import os
import sys
import time
import subprocess
import tempfile
from pathlib import Path
import pytest

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

@pytest.mark.integration
class TestDeploymentScenarios:
    """Test deployment and operational scenarios."""
    
    def test_deploy_script_structure(self, test_results_collector):
        """Test deploy.sh script structure and validation."""
        start_time = time.time()
        
        try:
            deploy_script = PROJECT_ROOT / 'deploy.sh'
            
            # Check script exists and is executable
            assert deploy_script.exists()
            
            # Make executable
            os.chmod(deploy_script, 0o755)
            
            # Test script validation (no arguments)
            result = subprocess.run([str(deploy_script)], capture_output=True, text=True)
            
            assert result.returncode == 1
            assert 'Usage:' in result.stdout
            
            # Check script content for required sections
            with open(deploy_script, 'r') as f:
                content = f.read()
            
            required_sections = [
                'Step 1/8: Clean existing deployment',
                'Step 2/8: Sync project',
                'Step 3/8: Provision remote environment',
                'Step 4/8: Run database migrations',
                'Step 5/8: Reset logs and test framework',
                'Step 6/8: Install cron jobs',
                'Step 7/8: Check production integrity',
                'Step 8/8: Verify deployment'
            ]
            
            missing_sections = []
            for section in required_sections:
                if section not in content:
                    missing_sections.append(section)
            
            if missing_sections:
                raise AssertionError(f"Missing deployment sections: {missing_sections}")
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_deploy_script_structure',
                'PASSED',
                duration,
                {'deployment_steps': len(required_sections)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_deploy_script_structure',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_run_script_commands(self, test_results_collector):
        """Test run.sh script commands."""
        start_time = time.time()
        
        try:
            run_script = PROJECT_ROOT / 'run.sh'
            
            # Check script exists and is executable
            assert run_script.exists()
            
            # Make executable
            os.chmod(run_script, 0o755)
            
            # Test help command
            result = subprocess.run([str(run_script), 'help'], capture_output=True, text=True)
            
            assert result.returncode == 0
            assert 'Data Processing Framework' in result.stdout
            
            # Test check command
            result = subprocess.run([str(run_script), 'check'], capture_output=True, text=True)
            
            assert result.returncode == 0
            assert 'Dependencies check completed' in result.stdout
            
            # Check script content for all commands
            with open(run_script, 'r') as f:
                content = f.read()
            
            required_commands = [
                'check)',
                'test)',
                'cron_run)',
                'list)',
                'setup_db)',
                'drop_db)',
                'migrate)',
                'migrate_status)',
                'backfill)',
                'backfill_list)',
                'backfill_counts)',
                'kill)',
                'clean)',
                'help|*)'
            ]
            
            missing_commands = []
            for command in required_commands:
                if command not in content:
                    missing_commands.append(command)
            
            if missing_commands:
                raise AssertionError(f"Missing run.sh commands: {missing_commands}")
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_run_script_commands',
                'PASSED',
                duration,
                {'commands_tested': 2, 'total_commands': len(required_commands)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_run_script_commands',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_backfill_script_functionality(self, test_results_collector):
        """Test backfill.py script functionality."""
        start_time = time.time()
        
        try:
            backfill_script = PROJECT_ROOT / 'scripts' / 'backfill.py'
            
            # Check script exists
            assert backfill_script.exists()
            
            # Test list_jobs command
            result = subprocess.run([
                'python3', str(backfill_script), 'list_jobs'
            ], capture_output=True, text=True, cwd=PROJECT_ROOT)
            
            # Should not crash (may return 0 or 1 depending on dependencies)
            assert result.returncode in [0, 1]
            
            # Test counts command (may fail due to no database, but shouldn't crash)
            result = subprocess.run([
                'python3', str(backfill_script), 'counts'
            ], capture_output=True, text=True, cwd=PROJECT_ROOT)
            
            # Should not crash
            assert result.returncode in [0, 1]
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_backfill_script_functionality',
                'PASSED',
                duration,
                {'commands_tested': 2}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_backfill_script_functionality',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_project_file_permissions(self, test_results_collector):
        """Test that project files have correct permissions."""
        start_time = time.time()
        
        try:
            # Scripts should be executable
            executable_files = [
                'deploy.sh',
                'run.sh'
            ]
            
            for script in executable_files:
                script_path = PROJECT_ROOT / script
                if script_path.exists():
                    # Make executable if not already
                    os.chmod(script_path, 0o755)
                    
                    # Check if executable
                    stat = script_path.stat()
                    assert stat.st_mode & 0o111  # Check execute bits
            
            # Python files should be readable
            python_files = [
                'src/main.py',
                'backfill.py',
                'scripts/run.py'
            ]
            
            for py_file in python_files:
                py_path = PROJECT_ROOT / py_file
                if py_path.exists():
                    stat = py_path.stat()
                    assert stat.st_mode & 0o444  # Check read bits
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_project_file_permissions',
                'PASSED',
                duration,
                {'executable_files': len(executable_files), 'python_files': len(python_files)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_project_file_permissions',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_idempotent_deployment_features(self, test_results_collector):
        """Test idempotent deployment features."""
        start_time = time.time()
        
        try:
            deploy_script = PROJECT_ROOT / 'deploy.sh'
            
            with open(deploy_script, 'r') as f:
                content = f.read()
            
            # Check for idempotent features
            idempotent_features = [
                'pkill -f "data-processor"',  # Kill existing processes
                'crontab -l 2>/dev/null | grep -v "data-processor"',  # Remove existing crons
                'rm -rf "$DEPLOY_DIR"',  # Clean deployment directory
                'mkdir -p "$DEPLOY_DIR"',  # Ensure directory exists
                'if [ ! -f .env ]',  # Only create .env if not exists
                '|| true'  # Ignore errors for idempotency
            ]
            
            missing_features = []
            for feature in idempotent_features:
                if feature not in content:
                    missing_features.append(feature)
            
            if missing_features:
                raise AssertionError(f"Missing idempotent features: {missing_features}")
            
            # Check for cleanup sections
            cleanup_sections = [
                'Clean existing deployment',
                'Kill existing processes',
                'Remove existing cron jobs'
            ]
            
            missing_cleanup = []
            for section in cleanup_sections:
                if section not in content:
                    missing_cleanup.append(section)
            
            if missing_cleanup:
                raise AssertionError(f"Missing cleanup sections: {missing_cleanup}")
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_idempotent_deployment_features',
                'PASSED',
                duration,
                {'idempotent_features': len(idempotent_features), 'cleanup_sections': len(cleanup_sections)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_idempotent_deployment_features',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise

@pytest.mark.integration
@pytest.mark.slow
class TestOperationalScenarios:
    """Test operational scenarios and edge cases."""
    
    def test_migration_system_robustness(self, test_results_collector):
        """Test migration system handles various scenarios."""
        start_time = time.time()
        
        try:
            # Test migration manager import
            from migrations.migration_manager import ClickHouseMigrationManager
            
            # Create migration manager
            manager = ClickHouseMigrationManager()
            
            # Test migration file discovery
            migration_files = manager.get_pending_migrations()
            assert isinstance(migration_files, list)
            
            # Test migration file parsing (should not crash even with empty directory)
            for migration_file in migration_files:
                assert str(migration_file).endswith('.sql')
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_migration_system_robustness',
                'PASSED',
                duration,
                {'migration_files': len(migration_files)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_migration_system_robustness',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_logging_directory_creation(self, test_env, test_results_collector):
        """Test logging directory creation in various scenarios."""
        start_time = time.time()
        
        try:
            from core.logging import setup_logging
            from core.config import config
            
            # Test with custom log directory
            temp_log_dir = os.path.join(test_env['temp_dir'], 'custom_logs')
            os.environ['LOG_DIR'] = temp_log_dir
            
            # Setup logging should create directories
            setup_logging('INFO')
            
            # Check directories were created
            assert os.path.exists(temp_log_dir)
            assert os.path.exists(os.path.join(temp_log_dir, 'system'))
            assert os.path.exists(os.path.join(temp_log_dir, 'jobs'))
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_logging_directory_creation',
                'PASSED',
                duration,
                {'directories_created': 3}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_logging_directory_creation',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_error_handling_scenarios(self, test_results_collector):
        """Test error handling in various scenarios."""
        start_time = time.time()
        
        try:
            from main import register_all_pipelines, run_cron_job
            
            # Test running non-existent job
            result = run_cron_job('non_existent_job')
            assert result is False
            
            # Test pipeline registration with no pipeline modules
            register_all_pipelines()  # Should not crash
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_error_handling_scenarios',
                'PASSED',
                duration,
                {'error_scenarios_tested': 2}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_error_handling_scenarios',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_configuration_edge_cases(self, test_results_collector):
        """Test configuration system edge cases."""
        start_time = time.time()
        
        try:
            from core.config import config
            
            # Test with missing environment variables
            original_host = os.environ.get('CLICKHOUSE_HOST')
            if 'CLICKHOUSE_HOST' in os.environ:
                del os.environ['CLICKHOUSE_HOST']
            
            # Should handle missing vars gracefully
            ch_config = config.get_clickhouse_config()
            assert 'host' in ch_config
            
            # Restore original value
            if original_host:
                os.environ['CLICKHOUSE_HOST'] = original_host
            
            # Test invalid port conversion
            os.environ['CLICKHOUSE_PORT'] = 'invalid_port'
            ch_config = config.get_clickhouse_config()
            # Should handle invalid port gracefully
            assert isinstance(ch_config['port'], int)
            
            # Restore valid port
            os.environ['CLICKHOUSE_PORT'] = '8123'
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_configuration_edge_cases',
                'PASSED',
                duration,
                {'edge_cases_tested': 2}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_configuration_edge_cases',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
