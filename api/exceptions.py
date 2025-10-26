"""Custom exceptions for the API."""


class APIError(Exception):
    """Base exception for API errors."""
    pass


class ElasticsearchError(APIError):
    """Raised when Elasticsearch operations fail."""
    pass


class DatabaseError(APIError):
    """Raised when database operations fail."""
    pass


class ValidationError(APIError):
    """Raised when input validation fails."""
    pass


class ConfigurationError(APIError):
    """Raised when configuration is invalid."""
    pass
