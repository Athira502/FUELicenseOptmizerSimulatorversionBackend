import os
from fastapi import APIRouter, HTTPException, Query
import re # Import the regular expression module
from fastapi.responses import FileResponse
router = APIRouter(prefix="/logs", tags=["Logs"])

LOG_DIR = "logs"


SPECIFIC_LOG_PATTERN = re.compile(r"^[a-zA-Z0-9_]+-[a-zA-Z0-9_]+-\d{8}-\d{6}\.log$")


@router.get("/")
def get_logs(filename: str = Query(None)):
    if filename:

        filename = filename.strip()
        file_path = os.path.join(LOG_DIR, filename)
        if not os.path.isfile(file_path):
            raise HTTPException(status_code=404, detail="Log file not found")
        try:
            with open(file_path, "r") as f:
                content = f.read()
            return {"filename": filename, "content": content}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")
    else:
        try:
            files = os.listdir(LOG_DIR)
            if not files:
                return {"message": "No log files available."}

            filtered_files = [f for f in files if SPECIFIC_LOG_PATTERN.match(f)]

            if not filtered_files:
                return {"message": "No log files matching 'clientname-systemid-timestamp.log' pattern available."}

            filtered_files.sort(key=lambda f: os.path.getmtime(os.path.join(LOG_DIR, f)), reverse=True)
            return {"files": filtered_files}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error listing log files: {str(e)}")

@router.get("/download/{filename}")
async def download_log_file(filename: str):
    """
    Download the content of a specific log file.
    """
    # Sanitize filename to prevent directory traversal attacks
    sanitized_filename = filename.strip()
    # Basic validation against common path traversal indicators
    if not sanitized_filename or '/' in sanitized_filename or '\\' in sanitized_filename or '..' in sanitized_filename:
        raise HTTPException(status_code=400, detail="Invalid filename characters detected.")

    # Optional: Re-verify if the filename matches your expected pattern for extra security
    if not SPECIFIC_LOG_PATTERN.match(sanitized_filename):
        raise HTTPException(status_code=400, detail="Invalid log file format or filename.")

    file_path = os.path.join(LOG_DIR, sanitized_filename)

    # Check if the file exists and is actually a file (not a directory)
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Log file not found.")

    try:
        # Use FileResponse to directly stream the file content
        # It handles setting Content-Disposition header (for download) and Content-Type automatically.
        return FileResponse(
            path=file_path,
            filename=sanitized_filename, # This is crucial for the browser to suggest the correct filename
            media_type="text/plain"      # Explicitly set the media type for text files
        )

    except Exception as e:
        # Catch any other unexpected errors during file serving
        raise HTTPException(status_code=500, detail=f"Error serving file: {str(e)}")
