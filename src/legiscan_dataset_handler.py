# Standard library imports
import io
import json
import time
import random
import logging
import zipfile
import base64
import hashlib
import re
import shutil
from pathlib import Path
from typing import Dict, Optional

# Third-party imports
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

# Local imports
from .config import (
    LEGISCAN_API_KEY,
    LEGISCAN_BASE_URL,
    LEGISCAN_MAX_RETRIES,
    LEGISCAN_DEFAULT_WAIT_SECONDS
)
from .utils import (
    load_json,
    save_json
    # ensure_dir # Not used directly here
)
# Import exceptions from the client module (assuming it defines them)
from .legiscan_client import APIRateLimitError, APIResourceNotFoundError

logger = logging.getLogger(__name__)

# --- LegiScan Bulk Dataset Helpers ---

DATASET_HASH_STORE_FILENAME = "legiscan_dataset_hashes.json"

def _load_dataset_hashes(paths: Dict[str, Path]) -> Dict[int, str]: # Changed key type hint to int
    """Loads the stored dataset hashes from the artifacts directory."""
    hash_file_path = paths.get('artifacts') / DATASET_HASH_STORE_FILENAME
    if hash_file_path.exists():
        hashes = load_json(hash_file_path)
        if isinstance(hashes, dict):
            try:
                # Ensure keys are integers (session IDs)
                return {int(k): str(v) for k, v in hashes.items()} # Ensure value is string too
            except (ValueError, TypeError):
                 logger.warning(f"Invalid keys or values found in {hash_file_path}. Returning empty hashes.")
                 return {}
        else:
            logger.warning(f"Dataset hash file {hash_file_path} is not a valid dictionary. Returning empty hashes.")
            return {}
    return {}

def _save_dataset_hashes(hashes: Dict[int, str], paths: Dict[str, Path]):
    """Saves the dataset hashes (int keys) to the artifacts directory."""
    hash_file_path = paths.get('artifacts') / DATASET_HASH_STORE_FILENAME
    hash_file_path.parent.mkdir(parents=True, exist_ok=True)
    # Ensure keys are strings for JSON compatibility
    save_json({str(k): v for k, v in hashes.items()}, hash_file_path)


@retry(
    stop=stop_after_attempt(LEGISCAN_MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    retry=retry_if_exception_type((requests.exceptions.RequestException, APIRateLimitError)),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING)
)
def download_and_extract_dataset(
    session_id: int,
    access_key: str,
    extract_base_path: Path,
    expected_hash: Optional[str] = None
) -> Optional[Path]:
    """
    Downloads the dataset ZIP for a session using getDataset, verifies it (optional MD5 hash),
    and extracts its 'bill/' subdirectory contents, returning the path to the 'bill' subdirectory.
    Handles both direct application/zip responses and application/json responses
    where the dataset is base64-encoded within the JSON payload.
    Uses a temporary file to handle large datasets and calculate hashes reliably.
    """
    if not LEGISCAN_API_KEY:
        logger.error("Cannot download dataset: LEGISCAN_API_KEY is not set.")
        return None

    params = {
        'key': LEGISCAN_API_KEY,
        'op': 'getDataset',
        'id': session_id,
        'access_key': access_key
    }
    request_id_log = params.get('id', 'N/A')

    base_wait = LEGISCAN_DEFAULT_WAIT_SECONDS + 1.0
    sleep_duration = max(0.1, base_wait + random.uniform(-0.2, 0.4))
    logger.debug(f"Sleeping for {sleep_duration:.2f}s before LegiScan API request (op: getDataset, id: {request_id_log})")
    time.sleep(sleep_duration)

    session_extract_path = extract_base_path / f"session_{session_id}"
    bill_extract_path = session_extract_path / "bill"

    response = None
    zip_data_stream = None
    temp_zip_path = None

    try:
        logger.info(f"Downloading LegiScan dataset: session_id={session_id}, access_key={access_key[:5]}...")
        log_params = {k: v for k, v in params.items() if k != 'key'}
        logger.debug(f"Request params (key omitted): {log_params}")

        response = requests.get(LEGISCAN_BASE_URL, params=params, timeout=300, stream=True)

        if response.status_code == 429:
            logger.warning(f"LegiScan Rate limit hit (HTTP 429) for op=getDataset, id={session_id}. Backing off...")
            raise APIRateLimitError("Rate limit exceeded")

        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '').lower()

        if 'application/zip' in content_type:
            logger.info(f"Processing dataset for session {session_id} received as application/zip.")
            response.raw.decode_content = True
            zip_data_stream = response.raw
        elif 'application/json' in content_type:
            logger.info(f"Processing dataset for session {session_id} received as application/json.")
            try:
                response_content = response.content
                if hasattr(response, 'connection') and response.connection and not response.connection.isclosed():
                    response.close()
                data = json.loads(response_content)
                if data.get('status') == 'OK' and 'dataset' in data:
                    dataset_payload = data['dataset']
                    raw_data_str = None
                    
                    # Check if the payload is the string itself or a dict containing the string
                    if isinstance(dataset_payload, str):
                        raw_data_str = dataset_payload
                    elif isinstance(dataset_payload, dict) and 'zip' in dataset_payload:
                        raw_data_str = dataset_payload.get('zip')
                        logger.info("Extracted base64 dataset string from nested 'zip' key in JSON payload.")
                    else:
                        logger.error(f"Unexpected format for 'dataset' payload: Type {type(dataset_payload)}. Session {session_id}.")
                        return None
                        
                    if not isinstance(raw_data_str, str):
                         logger.error(f"Expected base64 string in JSON dataset (key 'zip'?), got {type(raw_data_str)}. Session {session_id}.")
                         return None
                    try:
                        zip_content_bytes = base64.b64decode(raw_data_str)
                        zip_data_stream = io.BytesIO(zip_content_bytes)
                        logger.info(f"Successfully decoded base64 dataset ({len(zip_content_bytes)} bytes) from JSON for session {session_id}.")
                    except (base64.binascii.Error, ValueError) as b64_error:
                        logger.error(f"Failed to decode base64 dataset from JSON for session {session_id}: {b64_error}")
                        return None
                    except Exception as decode_err:
                         logger.error(f"Error creating BytesIO stream from decoded JSON for session {session_id}: {decode_err}", exc_info=True)
                         return None
                else:
                    status = data.get('status', 'N/A')
                    error_msg = data.get('alert', {}).get('message', f'Unknown API error in JSON (status: {status})')
                    logger.error(f"LegiScan API error in JSON response for getDataset (session {session_id}): {error_msg}")
                    return None
            except json.JSONDecodeError:
                 try: error_preview = response_content[:500].decode('utf-8', errors='replace')
                 except: error_preview = "[Could not decode preview]"
                 logger.error(f"Failed to decode JSON response for getDataset (session {session_id}). Preview: {error_preview}...")
                 return None
            except Exception as e:
                 logger.error(f"Error processing JSON response body for session {session_id}: {e}", exc_info=True)
                 return None
        else:
            logger.warning(f"Unexpected Content-Type '{content_type}' for getDataset (session {session_id}).")
            try:
                response_content = response.content
                error_preview = response_content[:500].decode('utf-8', errors='replace')
                logger.error(f"Cannot process unexpected Content-Type '{content_type}'. Preview: {error_preview}...")
            except Exception as preview_err: logger.error(f"Cannot process unexpected Content-Type '{content_type}'. Could not read preview: {preview_err}")
            finally: # Ensure response is closed if we are erroring out here
                if response and hasattr(response, 'connection') and response.connection and not response.connection.isclosed(): response.close()
            return None

        if zip_data_stream is None:
            logger.error(f"Could not obtain valid data stream for session {session_id}. Aborting extraction.")
            if response and hasattr(response, 'connection') and response.connection and not response.connection.isclosed(): response.close()
            return None

        logger.info(f"Preparing to process dataset stream for session {session_id} into {session_extract_path}")
        session_extract_path.mkdir(parents=True, exist_ok=True)
        temp_zip_path = session_extract_path / f"session_{session_id}_dataset.zip"

        sha256_hasher = hashlib.sha256(); md5_hasher = hashlib.md5(); bytes_written = 0
        try:
            with open(temp_zip_path, 'wb') as f_zip:
                if hasattr(zip_data_stream, 'read'): # BytesIO
                    while True:
                        chunk = zip_data_stream.read(8192)
                        if not chunk: break
                        f_zip.write(chunk); sha256_hasher.update(chunk); md5_hasher.update(chunk); bytes_written += len(chunk)
                elif response and zip_data_stream is response.raw: # requests raw stream
                     for chunk in response.iter_content(chunk_size=8192):
                         if chunk: f_zip.write(chunk); sha256_hasher.update(chunk); md5_hasher.update(chunk); bytes_written += len(chunk)
                else: raise IOError("Invalid zip stream type")
            actual_sha256_hash = sha256_hasher.hexdigest(); actual_md5_hash = md5_hasher.hexdigest()
            logger.info(f"Temporarily saved {bytes_written} bytes to {temp_zip_path}.")
            logger.info(f"Calculated SHA256: {actual_sha256_hash}, MD5: {actual_md5_hash}")

            if isinstance(zip_data_stream, io.BytesIO): zip_data_stream.close()
            elif response and zip_data_stream is response.raw and hasattr(response, 'connection') and response.connection and not response.connection.isclosed():
                 response.close(); logger.debug(f"Closed response stream for session {session_id} after writing.")

            if expected_hash and actual_md5_hash != expected_hash:
                logger.warning(f"MD5 hash mismatch session {session_id}! Expected: {expected_hash}, Got: {actual_md5_hash}.")
            elif expected_hash: logger.info(f"MD5 hash matches expected for session {session_id}.")

            logger.info(f"Extracting files from {temp_zip_path}...")
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                test_result = zip_ref.testzip()
                if test_result is not None: logger.error(f"Corrupted ZIP: {temp_zip_path}. Bad file: {test_result}"); return None
                
                # Find members ending with '/bill/' + filename.json (flexible path)
                bill_file_pattern = re.compile(r"/bill/[^/]+\.json$", re.IGNORECASE)
                members_to_extract = [m for m in zip_ref.namelist() if bill_file_pattern.search(m) and not m.endswith('/')]
                
                # Define the target extraction path for bill files specifically
                actual_bill_extract_path = session_extract_path / "bill"
                actual_bill_extract_path.mkdir(parents=True, exist_ok=True)
                
                if not members_to_extract:
                     logger.warning(f"ZIP {temp_zip_path} lacks files matching '/bill/*.json' pattern. Contents: {zip_ref.namelist()[:10]}")
                     # Allow proceeding if other data might exist, but log clearly
                     # Return None here if bill data is absolutely critical
                     # return None 
                else:
                    logger.info(f"Extracting {len(members_to_extract)} files matching '/bill/*.json' to {actual_bill_extract_path}...")
                    for member in members_to_extract:
                        # Extract each file individually, stripping the leading path components
                        # so it lands directly in the target 'bill' directory.
                        try:
                            # Calculate target path within the 'bill' directory
                            target_filename = Path(member).name
                            target_path = actual_bill_extract_path / target_filename
                            with zip_ref.open(member) as source, open(target_path, 'wb') as target:
                                shutil.copyfileobj(source, target)
                            # logger.debug(f"Extracted '{member}' to '{target_path}'")
                        except Exception as extract_err:
                             logger.error(f"Error extracting individual file '{member}' from {temp_zip_path}: {extract_err}", exc_info=True)
                             # Decide if one error should stop all extraction
                             # return None

                # Check if the target directory was created and has *some* content
                # Note: This check might pass even if individual file extractions failed above
                if not actual_bill_extract_path.is_dir() or not any(actual_bill_extract_path.iterdir()):
                    logger.error(f"Extraction target {actual_bill_extract_path} missing/empty after attempted extraction.")
                    # Consider returning None if extraction failure is critical
                    # return None
            logger.info(f"Successfully extracted bill data for session {session_id} to {actual_bill_extract_path}")
            try: temp_zip_path.unlink(); logger.debug(f"Removed temp zip: {temp_zip_path}"); temp_zip_path = None
            except OSError as e: logger.warning(f"Could not remove temp zip {temp_zip_path}: {e}")
            return actual_bill_extract_path # Return the path to the *extracted* bill directory
        except zipfile.BadZipFile: logger.error(f"Invalid ZIP file {temp_zip_path}.", exc_info=True); return None
        except (IOError, OSError) as e: logger.error(f"File system error for session {session_id}: {e}", exc_info=True); return None
        except Exception as e: logger.error(f"Unexpected error processing stream/file for session {session_id}: {e}", exc_info=True); return None

    except requests.exceptions.HTTPError as err:
        status_code = err.response.status_code if err.response else 'Unknown'
        logger.error(f"HTTP error {status_code} during getDataset session {session_id}: {err}", exc_info=False)
        if status_code == 404: logger.error(f"HTTP 404 suggests invalid Session ID ({session_id}) or Access Key.")
        return None
    except APIRateLimitError: logger.error(f"APIRateLimitError getDataset session {session_id}. Retrying..."); raise
    except requests.exceptions.RequestException as e: logger.error(f"Network error getDataset session {session_id}: {e}", exc_info=False); raise e
    except Exception as e: logger.error(f"Unhandled exception getDataset setup session {session_id}: {e}", exc_info=True); return None
    finally:
         if response and hasattr(response, 'connection') and response.connection and not response.connection.isclosed():
             logger.debug(f"Closing potentially open response in outer finally (session {session_id}).")
             response.close()
         if isinstance(zip_data_stream, io.BytesIO) and not zip_data_stream.closed:
             logger.debug(f"Closing potentially open BytesIO stream in outer finally (session {session_id}).")
             zip_data_stream.close()
