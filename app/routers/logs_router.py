
import os
from datetime import datetime, timedelta
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Query
import re
from fastapi.responses import FileResponse
from starlette import status
from app.core.logger import get_current_log_level, update_log_level, setup_logger
from app.schema.logSchema import LogLevelRequest
router = APIRouter(prefix="/logs", tags=["Logs"])

LOG_DIR = "logs"

SPECIFIC_LOG_PATTERN = re.compile(r"^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+-\d{8}-\d{6}\.log$")
logger = setup_logger("app_logger")
def extract_date_from_filename(filename: str) -> datetime:
    """
    Extract date from filename formats:
    1. log-YYYY-MM-DD.log
    2. clientname-systemid-YYYYMMDD-HHMMSS.log
    Falls back to file modification time if parsing fails.
    """
    # Regex to find YYYY-MM-DD format
    match_ymd = re.search(r'(\d{4}-\d{2}-\d{2})', filename)

    # Regex to find YYYYMMDD-HHMMSS format
    match_full = re.search(r'(\d{8}-\d{6})', filename)

    try:
        if match_ymd:
            date_str = match_ymd.group(1)
            return datetime.strptime(date_str, "%Y-%m-%d")
        elif match_full:
            date_str = match_full.group(1)
            return datetime.strptime(date_str, "%Y%m%d-%H%M%S")
        else:
            # Fallback to file modification time if no pattern matches
            logger.warning(f"Could not extract date from filename {filename} using known patterns. Falling back to file modification time.")
            file_path = os.path.join(LOG_DIR, filename)
            if os.path.exists(file_path):
                return datetime.fromtimestamp(os.path.getmtime(file_path))

    except (ValueError, IndexError) as e:
        logger.warning(f"Error parsing date from filename {filename}: {e}. Falling back to file modification time.")
        file_path = os.path.join(LOG_DIR, filename)
        if os.path.exists(file_path):
            return datetime.fromtimestamp(os.path.getmtime(file_path))

    logger.warning(f"Could not extract date from filename {filename}. Returning current time.")
    return datetime.now()  # Final fallback



@router.get("/")
def get_logs(filename: str = Query(None)):
    """
    Get log files or content of a specific log file.
    If filename is provided, returns the content of that file.
    If no filename is provided, returns list of all log files in the directory.
    """
    logger.info(f"Received request to get logs. Filename provided: {filename}")

    # Ensure log directory exists
    if not os.path.exists(LOG_DIR):
        logger.warning(f"Log directory '{LOG_DIR}' does not exist. Attempting to create it.")
        os.makedirs(LOG_DIR)

    if filename:
        # Return content of specific file
        filename = filename.strip()
        file_path = os.path.join(LOG_DIR, filename)

        if not os.path.isfile(file_path):
            logger.error(f"Attempted to read non-existent log file: {file_path}")
            raise HTTPException(status_code=404, detail="Log file not found")

        try:
            with open(file_path, "r", encoding='utf-8') as f:
                content = f.read()

            # Get file stats
            file_stats = os.stat(file_path)
            file_size = file_stats.st_size
            modified_time = datetime.fromtimestamp(file_stats.st_mtime)
            logger.info(f"Successfully read content of log file: {filename}")

            return {
                "filename": filename,
                "content": content,
                "size_bytes": file_size,
                "modified_at": modified_time.isoformat(),
                "line_count": len(content.splitlines()) if content else 0
            }
        except Exception as e:
            logger.exception(f"Error reading log file {filename}.")
            raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")

    else:
        # Return list of all log files
        try:
            if not os.path.exists(LOG_DIR):
                logger.warning(f"Log directory '{LOG_DIR}' still does not exist.")
                return {"message": "Log directory does not exist.", "files": []}

            files = os.listdir(LOG_DIR)
            if not files:
                logger.warning(f"No log files available.")
                return {"message": "No log files available.", "files": []}

            # Filter files matching the pattern
            filtered_files = [f for f in files if SPECIFIC_LOG_PATTERN.match(f)]

            if not filtered_files:
                # Also include any .log files that might not match the strict pattern
                log_files = [f for f in files if f.endswith('.log')]
                if log_files:
                    filtered_files = log_files
                else:
                    return {
                        "message": "No log files found in the directory.",
                        "files": [],
                        "total_files": 0
                    }

            # Get detailed file information
            file_details = []
            for filename in filtered_files:
                file_path = os.path.join(LOG_DIR, filename)
                try:
                    file_stats = os.stat(file_path)
                    file_size = file_stats.st_size
                    modified_time = datetime.fromtimestamp(file_stats.st_mtime)
                    created_time = extract_date_from_filename(filename)

                    file_details.append({
                        "filename": filename,
                        "size_bytes": file_size,
                        "size_human": format_file_size(file_size),
                        "modified_at": modified_time.isoformat(),
                        "created_at": created_time.isoformat(),
                        "age_days": (datetime.now() - created_time).days
                    })
                except Exception as e:
                    logger.error(f"Error getting stats for {filename}: {e}")
                    print(f"Error getting stats for {filename}: {e}")
                    # Include file even if we can't get stats
                    file_details.append({
                        "filename": filename,
                        "size_bytes": 0,
                        "size_human": "Unknown",
                        "modified_at": None,
                        "created_at": None,
                        "age_days": 0
                    })

            # Sort by creation time (newest first)
            file_details.sort(key=lambda x: x["created_at"] or "1970-01-01", reverse=True)
            logger.info(f"Returning details for {len(file_details)} log files.")

            return {
                "message": f"Found {len(file_details)} log files.",
                "files": file_details,
                "total_files": len(file_details),
                "total_size_bytes": sum(f["size_bytes"] for f in file_details),
                "total_size_human": format_file_size(sum(f["size_bytes"] for f in file_details))
            }

        except Exception as e:
            logger.exception("Error listing log files.")
            raise HTTPException(status_code=500, detail=f"Error listing log files: {str(e)}")


@router.get("/download/{filename}")
async def download_log_file(filename: str):
    """Download a specific log file"""
    logger.info(f"Received request to download file: {filename}")

    sanitized_filename = filename.strip()

    # Security checks
    if not sanitized_filename or '/' in sanitized_filename or '\\' in sanitized_filename or '..' in sanitized_filename:
        logger.warning(f"Download request for invalid filename detected: {sanitized_filename}")
        raise HTTPException(status_code=400, detail="Invalid filename characters detected.")

    # Allow any .log file, not just the strict pattern
    if not sanitized_filename.endswith('.log'):
        logger.warning(f"Download request for non-.log file detected: {sanitized_filename}")
        raise HTTPException(status_code=400, detail="Only .log files can be downloaded.")

    file_path = os.path.join(LOG_DIR, sanitized_filename)

    if not os.path.isfile(file_path):
        logger.error(f"Download request for non-existent file: {file_path}")
        raise HTTPException(status_code=404, detail="Log file not found.")

    try:
        logger.info(f"Serving file for download: {sanitized_filename}")
        return FileResponse(
            path=file_path,
            filename=sanitized_filename,
            media_type="text/plain"
        )
    except Exception as e:
        logger.exception(f"Error serving file {sanitized_filename} for download.")
        raise HTTPException(status_code=500, detail=f"Error serving file: {str(e)}")


@router.delete("/delete-old-logs/")
def delete_old_log_files(
        days: int = Query(..., ge=1,
                          description="Number of days to keep log files. Files older than this will be deleted from the logs directory.")
):
    """
    Delete log files older than specified number of days from the logs directory.
    This deletes the actual log files, not database records.
    """
    logger.info(f"Received request to delete logs older than {days} days.")
    if days <= 0:
        logger.warning(f"Invalid days parameter provided for deletion: {days}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Number of days must be a positive integer."
        )

    if not os.path.exists(LOG_DIR):
        logger.info(f"Log directory '{LOG_DIR}' does not exist. No files to delete.")
        return {"message": "Log directory does not exist.", "deleted_files": [], "deleted_count": 0}

    cutoff_datetime = datetime.now() - timedelta(days=days)
    deleted_files = []
    errors = []

    try:
        # Get all log files
        all_files = os.listdir(LOG_DIR)
        log_files = [f for f in all_files if f.endswith('.log')]
        logger.info(f"Found {len(log_files)} potential log files to check.")


        if not log_files:
            logger.info("No log files found in directory to check for deletion.")
            return {
                "message": f"No log files found in the directory.",
                "deleted_files": [],
                "deleted_count": 0
            }

        for filename in log_files:
            file_path = os.path.join(LOG_DIR, filename)

            try:
                # Get file creation date from filename first, then fall back to file stats
                file_date = extract_date_from_filename(filename)

                if file_date < cutoff_datetime:
                    # Get file size before deletion for reporting
                    file_size = os.path.getsize(file_path)

                    # Delete the file
                    os.remove(file_path)
                    logger.info(f"Deleted old log file: {filename} (created: {file_date.isoformat()})")

                    deleted_files.append({
                        "filename": filename,
                        "size_bytes": file_size,
                        "size_human": format_file_size(file_size),
                        "date_created": file_date.isoformat(),
                        "age_days": (datetime.now() - file_date).days
                    })

            except Exception as e:
                error_msg = f"Error deleting {filename}: {str(e)}"
                errors.append(error_msg)
                print(error_msg)
                logger.error(error_msg)# Log to console

        # Prepare response
        deleted_count = len(deleted_files)
        total_size_freed = sum(f["size_bytes"] for f in deleted_files)

        response = {
            "message": f"Successfully deleted {deleted_count} log files older than {days} days.",
            "deleted_files": deleted_files,
            "deleted_count": deleted_count,
            "total_size_freed_bytes": total_size_freed,
            "total_size_freed_human": format_file_size(total_size_freed),
            "cutoff_date": cutoff_datetime.isoformat()
        }

        if errors:
            response["errors"] = errors
            response["message"] += f" {len(errors)} files could not be deleted due to errors."

        if deleted_count == 0 and not errors:
            response["message"] = f"No log files older than {days} days found for deletion."
        logger.info(f"Log deletion process complete. Deleted {deleted_count} files.")
        return response

    except Exception as e:
        logger.exception("Unexpected error during log file deletion.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during log file deletion: {str(e)}"
        )


def format_file_size(size_bytes: int) -> str:
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"


@router.get("/stats/")
def get_log_directory_stats():
    """Get statistics about the log directory"""
    try:
        if not os.path.exists(LOG_DIR):
            return {
                "directory_exists": False,
                "message": "Log directory does not exist"
            }

        all_files = os.listdir(LOG_DIR)
        log_files = [f for f in all_files if f.endswith('.log')]

        if not log_files:
            return {
                "directory_exists": True,
                "total_files": 0,
                "total_size_bytes": 0,
                "total_size_human": "0 B",
                "oldest_file": None,
                "newest_file": None,
                "files_by_age": {}
            }

        total_size = 0
        file_ages = []
        oldest_file = None
        newest_file = None
        oldest_date = datetime.now()
        newest_date = datetime.min

        for filename in log_files:
            file_path = os.path.join(LOG_DIR, filename)
            file_size = os.path.getsize(file_path)
            total_size += file_size

            file_date = extract_date_from_filename(filename)
            age_days = (datetime.now() - file_date).days
            file_ages.append(age_days)

            if file_date < oldest_date:
                oldest_date = file_date
                oldest_file = filename

            if file_date > newest_date:
                newest_date = file_date
                newest_file = filename

        # Group files by age ranges
        files_by_age = {
            "today": len([age for age in file_ages if age == 0]),
            "1-7_days": len([age for age in file_ages if 1 <= age <= 7]),
            "8-30_days": len([age for age in file_ages if 8 <= age <= 30]),
            "31-90_days": len([age for age in file_ages if 31 <= age <= 90]),
            "over_90_days": len([age for age in file_ages if age > 90])
        }

        return {
            "directory_exists": True,
            "directory_path": os.path.abspath(LOG_DIR),
            "total_files": len(log_files),
            "total_size_bytes": total_size,
            "total_size_human": format_file_size(total_size),
            "oldest_file": {
                "filename": oldest_file,
                "date": oldest_date.isoformat() if oldest_file else None,
                "age_days": (datetime.now() - oldest_date).days if oldest_file else None
            },
            "newest_file": {
                "filename": newest_file,
                "date": newest_date.isoformat() if newest_file else None,
                "age_days": (datetime.now() - newest_date).days if newest_file else None
            },
            "files_by_age": files_by_age,
            "average_age_days": round(sum(file_ages) / len(file_ages), 1) if file_ages else 0
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting directory stats: {str(e)}")
@router.get("/level", response_model=Dict[str, Any])
async def get_log_level():
    """Get the current log level"""
    try:
        result = get_current_log_level()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/level", response_model=Dict[str, Any])
async def set_log_level(request: LogLevelRequest):
    """
    Set the log level - shows ONLY logs of the selected level.

    Examples:
    - If you select WARNING: Only WARNING level logs will appear
    - If you select DEBUG: Only DEBUG level logs will appear
    - If you select INFO: Only INFO level logs will appear
    - If you select ERROR: Only ERROR level logs will appear
    - If you select CRITICAL: Only CRITICAL level logs will appear
    """
    try:
        # Validate log level
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if request.log_level.upper() not in valid_levels:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid log level. Must be one of: {', '.join(valid_levels)}"
            )

        result = update_log_level(request.log_level.upper())
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

