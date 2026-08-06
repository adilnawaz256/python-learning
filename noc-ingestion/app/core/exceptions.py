class NocIngestionBaseException(Exception):
    """Base exception for all NOC Ingestion domain errors."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class StorageError(NocIngestionBaseException):
    """Raised when MinIO or storage operations fail."""
    pass


class MinIOConnectionError(StorageError):
    """Raised when MinIO client cannot connect to object storage."""
    pass


class FileValidationError(NocIngestionBaseException):
    """Raised when an uploaded file fails type, extension, or corrupt payload validation."""
    pass


class RESTCollectorError(NocIngestionBaseException):
    """Raised when external REST API collection fails or times out."""
    pass


class RESTTimeoutError(RESTCollectorError):
    """Raised when external REST API call times out."""
    pass


class KafkaConsumerError(NocIngestionBaseException):
    """Raised when Kafka connection or message processing fails."""
    pass


class SchemaValidationError(NocIngestionBaseException):
    """Raised when incoming JSON payload fails Pydantic schema validation."""
    pass
