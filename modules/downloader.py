import os
import uuid
import logging
import tempfile
import json
import base64
import yt_dlp
from pathlib import Path

logger = logging.getLogger(__name__)

_DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent.parent / "data")))
TEMP_DIR  = _DATA_DIR / "tmp"

_COOKIE_FILE_PATH = None


def _get_cookie_file():
    global _COOKIE_FILE_PATH
    if _COOKIE_FILE_PATH and os.path.exists(_COOKIE_FILE_PATH):
        return _COOKIE_FILE_PATH

    # Option 1: raw Netscape cookie text
    cookies_content = os.environ.get("INSTAGRAM_COOKIES", "").strip()
    if cookies_content:
        try:
            f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, prefix="yt_cookies_")
            f.write(cookies_content)
            f.flush()
            f.close()
            _COOKIE_FILE_PATH = f.name
            logger.info(f"Cookie file written from INSTAGRAM_COOKIES.")
            return _COOKIE_FILE_PATH
        except Exception as e:
            logger.warning(f"Could not write INSTAGRAM_COOKIES: {e}")

    # Option 2: base64 instagrapi session -> convert to Netscape
    b64 = os.environ.get("INSTAGRAM_SESSION_B64", "").strip()
    if b64:
        try:
            session = json.loads(base64.b64decode(b64).decode())
            raw_cookies = session.get("cookies", {})
            if raw_cookies:
                lines = ["# Netscape HTTP Cookie File"]
                for name, value in raw_cookies.items():
                    lines.append(f".instagram.com\tTRUE\t/\tTRUE\t2147483647\t{name}\t{value}")
                f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, prefix="yt_ig_session_")
                f.write("\n".join(lines))
                f.flush()
                f.close()
                _COOKIE_FILE_PATH = f.name
                logger.info("Cookie file written from INSTAGRAM_SESSION_B64.")
                return _COOKIE_FILE_PATH
        except Exception as e:
            logger.warning(f"Could not convert INSTAGRAM_SESSION_B64: {e}")

    return None


def _build_opts(output_template: str) -> dict:
    opts = {
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": False,
        "noprogress": True,
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best",
        "merge_output_format": "mp4",
        "socket_timeout": 60,
        "retries": 3,
        "fragment_retries": 5,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.instagram.com/",
        },
        "max_filesize": 500 * 1024 * 1024,
        "geo_bypass": True,
        "skip_unavailable_fragments": True,
    }
    cookie_file = _get_cookie_file()
    if cookie_file:
        opts["cookiefile"] = cookie_file
    return opts


def _build_fallback_opts(output_template: str) -> dict:
    opts = _build_opts(output_template)
    opts["format"] = "best"
    return opts


class DownloadResult:
    def __init__(self, success: bool, file_path: str = None, media_type: str = "Videos", error: str = None):
        self.success    = success
        self.file_path  = file_path
        self.media_type = media_type
        self.error      = error


def download(url: str, platform: str) -> DownloadResult:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    unique_id       = uuid.uuid4().hex
    file_prefix     = str(TEMP_DIR / f"{platform}_{unique_id}")
    output_template = file_prefix + ".%(ext)s"

    result = _attempt(url, output_template, file_prefix, _build_opts(output_template))
    if result.success:
        return result

    err_lower = (result.error or "").lower()
    if _is_permanent(err_lower):
        return result

    logger.info(f"Primary failed, trying fallback format: {url}")
    fb_prefix   = file_prefix + "_fb"
    fb_template = fb_prefix + ".%(ext)s"
    result2 = _attempt(url, fb_template, fb_prefix, _build_fallback_opts(fb_template))
    return result2 if result2.success else result


def _attempt(url, output_template, file_prefix, opts) -> DownloadResult:
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info is None:
                return DownloadResult(False, error="yt-dlp returned no info")
            if "entries" in info:
                entries = [e for e in (info["entries"] or []) if e]
                if not entries:
                    return DownloadResult(False, error="Playlist returned no entries")
                info = entries[0]
            downloaded = _find_file(file_prefix)
            if not downloaded:
                return DownloadResult(False, error="Downloaded file not found on disk")
            vcodec = (info.get("vcodec") or "none").lower()
            ext    = (info.get("ext") or "").lower()
            if vcodec not in ("none", "") or ext in ("mp4", "mkv", "webm", "mov", "avi"):
                media_type = "Videos"
            elif ext in ("jpg", "jpeg", "png", "webp", "gif"):
                media_type = "Images"
            else:
                media_type = "Videos"
            size_mb = Path(downloaded).stat().st_size / (1024 * 1024)
            logger.info(f"Downloaded: {Path(downloaded).name} [{media_type}] [{size_mb:.1f}MB]")
            return DownloadResult(True, file_path=downloaded, media_type=media_type)
    except yt_dlp.utils.DownloadError as e:
        return DownloadResult(False, error=str(e))
    except Exception as e:
        return DownloadResult(False, error=str(e))


def _find_file(file_prefix: str):
    prefix_name = Path(file_prefix).name
    try:
        candidates = [
            str(f) for f in TEMP_DIR.iterdir()
            if f.name.startswith(prefix_name) and f.is_file() and f.stat().st_size > 0
        ]
    except Exception:
        return None
    if not candidates:
        return None
    return max(candidates, key=lambda p: Path(p).stat().st_size)


def _is_permanent(err_lower: str) -> bool:
    signals = [
        "unsupported url", "no video formats found", "playlist returned no entries",
        "cannot parse data", "private video", "login required", "not available",
        "has been removed", "page not found", "404", "does not exist",
    ]
    return any(s in err_lower for s in signals)
