"""
Safe file downloader for medical pricing data from URLs.
Supports HTTPS URLs with validation, size limits, and timeout handling.
"""
import logging
import tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import requests

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Exception raised for download errors."""
    pass


def is_url(path: str) -> bool:
    """
    Check if a string is a URL.

    Args:
        path: String to check

    Returns:
        True if string is a URL, False otherwise
    """
    try:
        result = urlparse(path)
        return result.scheme in ('http', 'https')
    except Exception:
        return False


def validate_url(url: str, require_https: bool = True) -> None:
    """
    Validate URL for security and safety.

    Args:
        url: URL to validate
        require_https: If True, reject HTTP URLs (default: True)

    Raises:
        DownloadError: If URL is invalid or unsafe
    """
    parsed = urlparse(url)

    # Check scheme
    if parsed.scheme not in ('http', 'https'):
        raise DownloadError(f"Invalid URL scheme: {parsed.scheme}. Only HTTP/HTTPS supported.")

    # Require HTTPS for security
    if require_https and parsed.scheme != 'https':
        raise DownloadError("HTTP URLs are not allowed for security. Please use HTTPS.")

    # Check hostname exists
    if not parsed.netloc:
        raise DownloadError("Invalid URL: No hostname found")

    logger.info(f"URL validation passed: {url}")


def get_file_extension_from_url(url: str) -> Optional[str]:
    """
    Extract file extension from URL.

    Args:
        url: URL to extract extension from

    Returns:
        File extension (e.g., '.csv', '.json') or None
    """
    parsed = urlparse(url)
    path = Path(parsed.path)
    return path.suffix.lower()


def download_file(url: str, max_size_mb: int = 5000, require_https: bool = True) -> Path:
    """
    Download a file from a URL to a temporary location.

    Args:
        url: URL to download from
        max_size_mb: Maximum file size in MB (default: 5000 = 5GB)
        require_https: Require HTTPS URLs (default: True)

    Returns:
        Path to downloaded temporary file

    Raises:
        DownloadError: If download fails or validation fails
    """
    logger.info(f"Starting download from URL: {url}")

    # Validate URL
    validate_url(url, require_https=require_https)

    # Get file extension
    extension = get_file_extension_from_url(url)
    if extension not in ('.csv', '.json'):
        logger.warning(f"URL has unexpected extension: {extension}. Will validate after download.")

    try:
        # Make HEAD request to check size and content type
        logger.info("Checking file size and type...")
        head_response = requests.head(url, timeout=30, allow_redirects=True)
        head_response.raise_for_status()

        # Check content length if available
        content_length = head_response.headers.get('content-length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            logger.info(f"File size: {size_mb:.2f} MB")

            if size_mb > max_size_mb:
                raise DownloadError(
                    f"File too large: {size_mb:.2f} MB (max: {max_size_mb} MB). "
                    f"Use a smaller file or increase --max-download-size."
                )
        else:
            logger.warning("Content-Length header not available, cannot check size before download")

        # Create temporary file with appropriate extension
        temp_file = tempfile.NamedTemporaryFile(
            mode='wb',
            suffix=extension or '.tmp',
            delete=False,
            prefix='hospital_data_'
        )
        temp_path = Path(temp_file.name)

        logger.info(f"Downloading to temporary file: {temp_path}")

        # Download file with streaming to handle large files
        response = requests.get(
            url,
            stream=True,
            timeout=(30, 300),  # 30s connect timeout, 5min read timeout
            allow_redirects=True
        )
        response.raise_for_status()

        # Download in chunks and track progress
        downloaded_mb = 0
        chunk_size = 1024 * 1024  # 1 MB chunks

        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                temp_file.write(chunk)
                downloaded_mb += len(chunk) / (1024 * 1024)

                # Log progress every 100 MB
                if int(downloaded_mb) % 100 == 0 and int(downloaded_mb) > 0:
                    logger.info(f"Downloaded {int(downloaded_mb)} MB...")

                # Safety check: enforce max size during download
                if downloaded_mb > max_size_mb:
                    temp_file.close()
                    temp_path.unlink()
                    raise DownloadError(
                        f"File exceeded size limit during download: {downloaded_mb:.2f} MB (max: {max_size_mb} MB)"
                    )

        temp_file.close()

        # Validate downloaded file
        final_size_mb = temp_path.stat().st_size / (1024 * 1024)
        logger.info(f"Download complete: {final_size_mb:.2f} MB")

        # Check file extension of downloaded file
        if not extension or extension not in ('.csv', '.json'):
            logger.warning(
                f"Downloaded file may not be CSV or JSON (extension: {extension}). "
                f"Processing will attempt to detect format."
            )

        return temp_path

    except requests.exceptions.Timeout:
        raise DownloadError("Download timed out. Please try again or use a local file.")
    except requests.exceptions.ConnectionError as e:
        raise DownloadError(f"Connection failed: {e}")
    except requests.exceptions.HTTPError as e:
        raise DownloadError(f"HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        raise DownloadError(f"Download failed: {e}")
    except Exception as e:
        # Clean up temp file on error
        if 'temp_path' in locals() and temp_path.exists():
            temp_path.unlink()
        raise DownloadError(f"Unexpected error during download: {e}")


def cleanup_temp_file(file_path: Path) -> None:
    """
    Clean up a temporary file.

    Args:
        file_path: Path to temporary file to delete
    """
    try:
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Cleaned up temporary file: {file_path}")
    except Exception as e:
        logger.warning(f"Failed to clean up temporary file {file_path}: {e}")
