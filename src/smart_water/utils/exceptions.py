class SmartWaterException(Exception):
    """Base exception for the Smart Water Data Platform."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class ConfigurationError(SmartWaterException):
    """Raised when application configuration is invalid."""


class IngestionError(SmartWaterException):
    """Raised when data ingestion fails."""


class ProcessingError(SmartWaterException):
    """Raised when data processing fails."""


class DataQualityError(SmartWaterException):
    """Raised when data quality validation fails."""