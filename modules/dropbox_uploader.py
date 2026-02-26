import os
import logging
from pathlib import Path
import dropbox
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import WriteMode, UploadSessionCursor, CommitInfo

logger = logging.getLogger(__name__)

DROPBOX_ROOT       = "/MediaArchive"
CHUNK_SIZE         = 8 * 1024 * 1024
SIMPLE_UPLOAD_LIMIT = 150 * 1024 * 1024


def _get_client() -> dropbox.Dropbox:
    return dropbox.Dropbox(
        app_key=os.environ["DROPBOX_APP_KEY"],
        app_secret=os.environ["DROPBOX_APP_SECRET"],
        oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
    )


def upload_file(local_path: str, platform: str, media_type: str) -> tuple[bool, str]:
    if not local_path or not Path(local_path).exists():
        return False, f"Local file not found: {local_path}"

    filename     = Path(local_path).name
    dropbox_path = f"{DROPBOX_ROOT}/{platform.capitalize()}/{media_type}/{filename}"

    try:
        dbx       = _get_client()
        file_size = os.path.getsize(local_path)
        logger.info(f"Uploading {filename} ({file_size/(1024*1024):.1f}MB) -> {dropbox_path}")

        if file_size <= SIMPLE_UPLOAD_LIMIT:
            _simple_upload(dbx, local_path, dropbox_path)
        else:
            _chunked_upload(dbx, local_path, dropbox_path, file_size)

        logger.info(f"Upload complete: {dropbox_path}")
        return True, dropbox_path

    except AuthError as e:
        msg = f"Dropbox auth error (check DROPBOX_REFRESH_TOKEN): {e}"
        logger.error(msg)
        return False, msg
    except ApiError as e:
        msg = f"Dropbox API error: {e}"
        logger.error(msg)
        return False, msg
    except Exception as e:
        msg = f"Dropbox upload error: {e}"
        logger.error(msg)
        return False, msg


def _simple_upload(dbx, local_path, dropbox_path):
    with open(local_path, "rb") as f:
        dbx.files_upload(f.read(), dropbox_path, mode=WriteMode.overwrite)


def _chunked_upload(dbx, local_path, dropbox_path, file_size):
    with open(local_path, "rb") as f:
        first_chunk = f.read(CHUNK_SIZE)
        session     = dbx.files_upload_session_start(first_chunk)
        cursor      = UploadSessionCursor(session_id=session.session_id, offset=f.tell())
        commit      = CommitInfo(path=dropbox_path, mode=WriteMode.overwrite)

        while f.tell() < file_size:
            remaining = file_size - f.tell()
            if remaining <= CHUNK_SIZE:
                dbx.files_upload_session_finish(f.read(remaining), cursor, commit)
            else:
                chunk = f.read(CHUNK_SIZE)
                dbx.files_upload_session_append_v2(chunk, cursor)
                cursor.offset = f.tell()
