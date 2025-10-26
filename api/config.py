"""Configuration management for the API."""
import os
from configparser import ConfigParser
from typing import Dict, Any


class Config:
    """Centralized configuration management."""
    
    def __init__(self):
        self._config = ConfigParser()
        self._load_config()
    
    def _load_config(self):
        """Load configuration from environment variables or file."""
        # Try to read from config file
        config_file = os.getenv('API_CONFIG_FILE', 'credentials.ini')
        if os.path.exists(config_file):
            self._config.read(config_file)
        
        # Override with environment variables if present
        self.db_config = {
            'host': os.getenv('DB_HOST', self._get_config('database', 'host', 'jupiter')),
            'port': os.getenv('DB_PORT', self._get_config('database', 'port', '5432')),
            'database': os.getenv('DB_NAME', self._get_config('database', 'database', 'reddit')),
            'user': os.getenv('DB_USER', self._get_config('database', 'user')),
            'password': os.getenv('DB_PASSWORD', self._get_config('database', 'password')),
        }
        
        self.elasticsearch_config = {
            'primary': os.getenv('ES_PRIMARY', self._get_config('elasticsearch', 'primary', 'http://mars:9200')),
            'fallback': os.getenv('ES_FALLBACK', self._get_config('elasticsearch', 'fallback', 'http://jupiter:9200')),
        }
    
    def _get_config(self, section: str, key: str, default: Any = None) -> Any:
        """Get configuration value with optional default."""
        try:
            return self._config.get(section, key)
        except (KeyError, AttributeError):
            return default
    
    def get_db_connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return (
            f"dbname='{self.db_config['database']}' "
            f"user='{self.db_config['user']}' "
            f"host='{self.db_config['host']}' "
            f"port='{self.db_config['port']}' "
            f"password='{self.db_config['password']}'"
        )
    
    def get_elasticsearch_urls(self) -> tuple:
        """Get primary and fallback Elasticsearch URLs."""
        return (
            self.elasticsearch_config['primary'],
            self.elasticsearch_config['fallback']
        )


# Global config instance
config = Config()
