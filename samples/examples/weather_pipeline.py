# samples/examples/weather_pipeline.py
"""
Weather data pipeline example.
"""
from typing import Dict, Any, List
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.config import config
from core.logging import log_with_timestamp

# Import generic components
from pipelines.tools.extractors.http_extractor import create_http_extractor
from pipelines.tools.loaders.clickhouse_loader import create_clickhouse_loader
from pipelines.tools.transformers.transformers import create_lambda_transformer, add_timestamp_metadata
from pipelines.pipeline_factory import create_el_pipeline

def create_weather_pipeline():
    """Create a weather data pipeline."""
    weather_config = config.get_weather_api_config()
    
    # Weather extractor
    weather_extractor = create_http_extractor(
        url=f"{weather_config['base_url']}/weather",
        params={
            'q': 'Tehran',
            'appid': weather_config['api_key'],
            'units': 'metric'
        },
        name="Weather Extractor"
    )
    
    # Weather transformer
    def transform_weather_data(record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform weather data."""
        return {
            'city': record.get('name'),
            'country': record.get('sys', {}).get('country'),
            'temperature': record.get('main', {}).get('temp'),
            'humidity': record.get('main', {}).get('humidity'),
            'pressure': record.get('main', {}).get('pressure'),
            'description': record.get('weather', [{}])[0].get('description'),
            'wind_speed': record.get('wind', {}).get('speed'),
            'wind_direction': record.get('wind', {}).get('deg'),
            'timestamp': record.get('dt')
        }
    
    weather_transformer = create_lambda_transformer(
        transform_weather_data,
        "Weather Transformer"
    )
    
    # Weather loader
    weather_loader = create_clickhouse_loader(
        table_name="weather_data",
        name="Weather Loader"
    )
    
    # Create EL pipeline
    weather_pipeline = create_el_pipeline(
        weather_extractor,
        weather_loader,
        "Weather Data Pipeline"
    )
    
    return weather_pipeline
