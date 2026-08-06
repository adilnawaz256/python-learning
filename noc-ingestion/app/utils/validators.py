import hashlib
from typing import Tuple
from app.core.exceptions import FileValidationError
from app.schemas.file_schema import SupportedFileType

ALLOWED_EXTENSIONS = {
    ".csv": SupportedFileType.CSV,
    ".json": SupportedFileType.JSON,
    ".xlsx": SupportedFileType.EXCEL,
    ".xls": SupportedFileType.EXCEL,
    ".pdf": SupportedFileType.PDF,
}

CONTENT_TYPES = {
    SupportedFileType.CSV: "text/csv",
    SupportedFileType.JSON: "application/json",
    SupportedFileType.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    SupportedFileType.PDF: "application/pdf",
}


def validate_file_extension(filename: str) -> Tuple[str, SupportedFileType]:
    """Validates filename extension and returns (extension, SupportedFileType)."""
    if not filename or "." not in filename:
        raise FileValidationError(f"Invalid filename '{filename}': Missing file extension.")

    ext = "." + filename.rsplit(".", 1)[-1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise FileValidationError(
            f"Unsupported file format '{ext}'. Allowed extensions: {list(ALLOWED_EXTENSIONS.keys())}"
        )

    return ext, ALLOWED_EXTENSIONS[ext]


def calculate_sha256(content: bytes) -> str:
    """Calculates SHA256 checksum of raw binary content."""
    return hashlib.sha256(content).hexdigest()
