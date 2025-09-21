# src/core/logging.py
import logging
from datetime import datetime, timezone, timedelta
import os
from core.config import config

# Ensure logs directory exists (project /logs by default, fallback to ./logs if not writable)
LOG_DIR = config.get('LOG_DIR')
try:
    os.makedirs(LOG_DIR, exist_ok=True)
    test_path = os.path.join(LOG_DIR, '.write_test')
    with open(test_path, 'w') as _f:
        _f.write('ok')
    os.remove(test_path)
except Exception:
    LOG_DIR = os.path.join(os.getcwd(), 'logs')
    os.makedirs(LOG_DIR, exist_ok=True)

# Structured log subdirectories
SYSTEM_LOG_DIR = os.path.join(LOG_DIR, 'system')
JOB_LOG_DIR = os.path.join(LOG_DIR, 'jobs')
os.makedirs(SYSTEM_LOG_DIR, exist_ok=True)
os.makedirs(JOB_LOG_DIR, exist_ok=True)


def setup_logging(level: str = 'INFO', log_file: str = None):
    """
    Sets up global logging configuration.
    """
    # Reinitialize log directories in case LOG_DIR environment variable changed
    global LOG_DIR, SYSTEM_LOG_DIR, JOB_LOG_DIR
    
    # Reload config to pick up any environment variable changes
    config.reload()
    
    LOG_DIR = config.get('LOG_DIR')
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        test_path = os.path.join(LOG_DIR, '.write_test')
        with open(test_path, 'w') as _f:
            _f.write('ok')
        os.remove(test_path)
    except Exception:
        LOG_DIR = os.path.join(os.getcwd(), 'logs')
        os.makedirs(LOG_DIR, exist_ok=True)

    # Structured log subdirectories
    SYSTEM_LOG_DIR = os.path.join(LOG_DIR, 'system')
    JOB_LOG_DIR = os.path.join(LOG_DIR, 'jobs')
    os.makedirs(SYSTEM_LOG_DIR, exist_ok=True)
    os.makedirs(JOB_LOG_DIR, exist_ok=True)
    
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')

    # Configure handlers separately: file at configured level, console at WARNING+
    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.setLevel(numeric_level)

    target_file = log_file or os.path.join(SYSTEM_LOG_DIR, 'application.log')

    file_handler = logging.FileHandler(target_file)
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.WARNING)
    stream_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


def log_with_timestamp(message: str, name: str = "Pipeline", level: str = "info", category: str = None):
    """Log message with Tehran timestamp"""
    # Tehran is UTC+3:30
    tehran_tz = timezone(timedelta(hours=3, minutes=30))
    timestamp = datetime.now(tehran_tz).strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"{name}"
    if category:
        prefix += f"[{category}]"
    log_message = f"[{timestamp}] {prefix}: {message}"

    if level.lower() == "info":
        logging.info(log_message)
    elif level.lower() == "warning":
        logging.warning(log_message)
    elif level.lower() == "error":
        logging.error(log_message)
    elif level.lower() == "debug":
        logging.debug(log_message)
    else:
        logging.info(log_message) # Default to info


def get_job_log_path(job_name: str) -> str:
    """Get the log file path for a specific job."""
    return os.path.join(JOB_LOG_DIR, f"{job_name}.log")


def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name. If None, returns root logger.
        
    Returns:
        Logger instance configured with the framework's settings.
        
    Example:
        >>> logger = get_logger('MyPipeline')
        >>> logger.info('Pipeline started')
    """
    return logging.getLogger(name)


class LoggingContext:
    """
    Context manager for structured logging with additional context.
    
    Example:
        >>> with LoggingContext('DataProcessing', job_id='123', pipeline='my_pipeline'):
        ...     log_with_timestamp('Processing started', 'Pipeline')
        ...     # All logs in this context will include job_id and pipeline info
    """
    
    def __init__(self, name: str, **context):
        """
        Initialize logging context.
        
        Args:
            name: Context name
            **context: Additional context variables
        """
        self.name = name
        self.context = context
        self.original_factory = None
        
    def __enter__(self):
        """Enter the logging context."""
        # Store original record factory
        self.original_factory = logging.getLogRecordFactory()
        
        # Create new factory that adds context
        def record_factory(*args, **kwargs):
            record = self.original_factory(*args, **kwargs)
            # Add context to the record
            for key, value in self.context.items():
                setattr(record, key, value)
            return record
        
        # Set the new factory
        logging.setLogRecordFactory(record_factory)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the logging context."""
        # Restore original factory
        if self.original_factory:
            logging.setLogRecordFactory(self.original_factory)


def log_function_call(func_name: str = None, level: str = "debug"):
    """
    Decorator for logging function calls with execution time.
    
    Args:
        func_name: Custom function name for logging. If None, uses actual function name.
        level: Log level for the messages.
        
    Example:
        >>> @log_function_call('process_data', 'info')
        ... def my_function(data):
        ...     return process(data)
    """
    def decorator(func):
        import functools
        import time
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = func_name or func.__name__
            start_time = time.time()
            
            # Log function start
            log_with_timestamp(f"Starting {name}", "FunctionCall", level)
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                # Log successful completion
                log_with_timestamp(
                    f"Completed {name} in {execution_time:.2f}s", 
                    "FunctionCall", 
                    level
                )
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                
                # Log error
                log_with_timestamp(
                    f"Error in {name} after {execution_time:.2f}s: {e}", 
                    "FunctionCall", 
                    "error"
                )
                raise
                
        return wrapper
    return decorator


def log_pipeline_stage(stage_name: str, level: str = "info"):
    """
    Decorator for logging pipeline stages (extract, transform, load).
    
    Args:
        stage_name: Name of the pipeline stage
        level: Log level for the messages
        
    Example:
        >>> @log_pipeline_stage('Extract', 'info')
        ... def extract_data():
        ...     return data
    """
    def decorator(func):
        import functools
        import time
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            # Log stage start
            log_with_timestamp(f"[{stage_name}] Starting", "Pipeline", level)
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                
                # Log stage completion with data info
                data_info = ""
                if hasattr(result, 'shape'):  # pandas DataFrame
                    data_info = f" (shape: {result.shape})"
                elif hasattr(result, '__len__'):  # list, dict, etc.
                    data_info = f" (length: {len(result)})"
                
                log_with_timestamp(
                    f"[{stage_name}] Completed in {execution_time:.2f}s{data_info}", 
                    "Pipeline", 
                    level
                )
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                
                # Log stage error
                log_with_timestamp(
                    f"[{stage_name}] Failed after {execution_time:.2f}s: {e}", 
                    "Pipeline", 
                    "error"
                )
                raise
                
        return wrapper
    return decorator


class PerformanceLogger:
    """
    Context manager for performance logging.
    
    Example:
        >>> with PerformanceLogger('DataProcessing', 'Pipeline'):
        ...     # Do some work
        ...     process_data()
        # Logs: [2024-01-01 12:00:00] Pipeline: DataProcessing completed in 1.23s
    """
    
    def __init__(self, operation_name: str, logger_name: str = "Performance", level: str = "info"):
        """
        Initialize performance logger.
        
        Args:
            operation_name: Name of the operation being timed
            logger_name: Logger name for the messages
            level: Log level for the messages
        """
        self.operation_name = operation_name
        self.logger_name = logger_name
        self.level = level
        self.start_time = None
        
    def __enter__(self):
        """Start timing the operation."""
        import time
        self.start_time = time.time()
        log_with_timestamp(f"{self.operation_name} started", self.logger_name, self.level)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End timing and log the result."""
        import time
        execution_time = time.time() - self.start_time
        
        if exc_type is None:
            # Success
            log_with_timestamp(
                f"{self.operation_name} completed in {execution_time:.2f}s", 
                self.logger_name, 
                self.level
            )
        else:
            # Error
            log_with_timestamp(
                f"{self.operation_name} failed after {execution_time:.2f}s: {exc_val}", 
                self.logger_name, 
                "error"
            )


def create_job_logger(job_name: str) -> logging.Logger:
    """
    Create a dedicated logger for a specific job with its own log file.
    
    Args:
        job_name: Name of the job
        
    Returns:
        Logger instance configured for the job
        
    Example:
        >>> logger = create_job_logger('my_pipeline')
        >>> logger.info('Pipeline started')
        # Logs to logs/jobs/my_pipeline.log
    """
    logger = logging.getLogger(f"job.{job_name}")
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Create job-specific file handler
    job_log_path = get_job_log_path(job_name)
    handler = logging.FileHandler(job_log_path)
    handler.setLevel(logging.INFO)
    
    # Use detailed format for job logs
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(name)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger