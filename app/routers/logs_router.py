import os
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Depends
import re
from fastapi.responses import FileResponse
from starlette import status
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.log_data import logData

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

    sanitized_filename = filename.strip()
    if not sanitized_filename or '/' in sanitized_filename or '\\' in sanitized_filename or '..' in sanitized_filename:
        raise HTTPException(status_code=400, detail="Invalid filename characters detected.")

    if not SPECIFIC_LOG_PATTERN.match(sanitized_filename):
        raise HTTPException(status_code=400, detail="Invalid log file format or filename.")

    file_path = os.path.join(LOG_DIR, sanitized_filename)

    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Log file not found.")

    try:

        return FileResponse(
            path=file_path,
            filename=sanitized_filename,
            media_type="text/plain"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error serving file: {str(e)}")



@router.delete("/delete-old-db-logs/")
def delete_old_db_logs(
    days: int = Query(..., ge=1, description="Number of days to keep database logs. Records older than this will be deleted."),
    db: Session = Depends(get_db)
):
    if days <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Number of days must be a positive integer."
        )



    cutoff_datetime = datetime.now() - timedelta(days=days)

    try:
        deleted_count = db.query(logData).filter(
            logData.TIMESTAMP < cutoff_datetime
        ).delete(synchronize_session=False)

        db.commit()

        if deleted_count == 0:
            return {"message": f"No database log records older than {days} days found."}
        else:
            return {"message": f"Successfully deleted {deleted_count} database log records older than {days} days ."}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error deleting database log records: {str(e)}")
