import os
import re
import uuid
import logging
import tempfile
import json
import base64
import requests
import yt_dlp
from pathlib import Path

logger = logging.getLogger(__name__)

_DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent.parent / "data")))
TEMP_DIR  = _DATA_DIR / "tmp"

_COOKIE_FILE_PATH  = None
_INSTAGRAPI_CLIENT = None


# ─────────────────────────────────────────────
# RESULT CLASS  (defined first — used everywhere)
# ─────────────────────────────────────────────

class DownloadResult:
    def __init__(self, success: bool, file_path: str = None, media_type: str = "Videos", error: str = None):
        self.success    = success
        self.file_path  = file_path
        self.media_type = media_type
        self.error      = error

    def __repr__(self):
        if self.success:
            return f"<DownloadResult OK {self.media_type} {self.file_path}>"
        return f"<DownloadResult FAIL {self.error}>"


# ─────────────────────────────────────────────
# COOKIE HANDLING
# ─────────────────────────────────────────────

def _get_cookie_file():
    global _COOKIE_FILE_PATH
    if _COOKIE_FILE_PATH and os.path.exists(_COOKIE_FILE_PATH):
        return _COOKIE_FILE_PATH

    cookies_content = os.environ.get("INSTAGRAM_COOKIES", "").strip()
    if cookies_content:
        try:
            f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, prefix="yt_cookies_")
            f.write(cookies_content)
            f.flush()
            f.close()
            _COOKIE_FILE_PATH = f.name
            logger.info("Cookie file written from INSTAGRAM_COOKIES.")
            return _COOKIE_FILE_PATH
        except Exception as e:
            logger.warning(f"Could not write INSTAGRAM_COOKIES: {e}")

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


# ─────────────────────────────────────────────
# YT-DLP OPTIONS
# ─────────────────────────────────────────────

def _common_opts(output_template: str) -> dict:
    opts = {
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": False,
        "noprogress": True,
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


def _video_opts(output_template: str) -> dict:
    opts = _common_opts(output_template)
    opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best"
    opts["merge_output_format"] = "mp4"
    return opts


def _universal_opts(output_template: str) -> dict:
    opts = _common_opts(output_template)
    opts["format"] = "best"
    return opts


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _detect_media_type_from_entry(entry: dict) -> str:
    ext    = (entry.get("ext") or "").lower()
    vcodec = (entry.get("vcodec") or "none").lower()
    acodec = (entry.get("acodec") or "none").lower()
    if ext in ("jpg", "jpeg", "png", "webp", "gif"):
        return "Images"
    if vcodec not in ("none", "") or ext in ("mp4", "mkv", "webm", "mov", "avi"):
        return "Videos"
    if vcodec in ("none", "") and acodec in ("none", ""):
        return "Images"
    return "Videos"


def _detect_media_type_from_file(file_path: str) -> str:
    ext = Path(file_path).suffix.lower().lstrip(".")
    if ext in ("jpg", "jpeg", "png", "webp", "gif"):
        return "Images"
    if ext in ("mp4", "mkv", "webm", "mov", "avi"):
        return "Videos"
    return "Videos"


def _find_files_by_prefix(prefix: str) -> list:
    prefix_name = Path(prefix).name
    try:
        return sorted([
            str(f) for f in TEMP_DIR.iterdir()
            if f.name.startswith(prefix_name) and f.is_file() and f.stat().st_size > 0
        ])
    except Exception:
        return []


def _find_one_file(prefix: str):
    files = _find_files_by_prefix(prefix)
    if not files:
        return None
    return max(files, key=lambda p: Path(p).stat().st_size)


def _log_item(index: int, total: int, file_path: str, media_type: str):
    size_mb = Path(file_path).stat().st_size / (1024 * 1024)
    logger.info(f"  [{index}/{total}] {Path(file_path).name} [{media_type}] [{size_mb:.2f}MB]")


def _extract_shortcode(url: str) -> str:
    match = re.search(r"/p/([A-Za-z0-9_\-]+)", url)
    return match.group(1) if match else None


# ─────────────────────────────────────────────
# DIRECT URL DOWNLOAD (requests, no yt-dlp)
# ─────────────────────────────────────────────

def _download_direct_url(url: str, file_prefix: str, ext: str) -> DownloadResult:
    """Download a direct image/video URL using requests."""
    file_path = file_prefix + f".{ext}"
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
            "Referer": "https://www.instagram.com/",
        }
        resp = requests.get(url, headers=headers, stream=True, timeout=60)
        resp.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return DownloadResult(False, error="Downloaded file is empty")

        media_type = _detect_media_type_from_file(file_path)
        return DownloadResult(True, file_path=file_path, media_type=media_type)

    except Exception as e:
        return DownloadResult(False, error=f"Direct download failed: {e}")


# ─────────────────────────────────────────────
# INFO EXTRACTION (no download)
# ─────────────────────────────────────────────

def _extract_info_only(url: str, opts: dict):
    probe_opts = dict(opts)
    probe_opts["quiet"]       = True
    probe_opts["no_warnings"] = True
    try:
        with yt_dlp.YoutubeDL(probe_opts) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception:
        return None


# ─────────────────────────────────────────────
# SINGLE ENTRY DOWNLOAD (yt-dlp)
# ─────────────────────────────────────────────

def _download_entry(url: str, file_prefix: str, opts: dict) -> DownloadResult:
    output_template = file_prefix + ".%(ext)s"
    opts = dict(opts)
    opts["outtmpl"] = output_template

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info is None:
                return DownloadResult(False, error="yt-dlp returned no info")
            if info.get("entries"):
                entries = [e for e in info["entries"] if e]
                if not entries:
                    return DownloadResult(False, error="Empty playlist")
                info = entries[0]
            downloaded = _find_one_file(file_prefix)
            if not downloaded:
                return DownloadResult(False, error="File not found on disk after download")
            media_type = _detect_media_type_from_file(downloaded)
            return DownloadResult(True, file_path=downloaded, media_type=media_type)
    except yt_dlp.utils.DownloadError as e:
        return DownloadResult(False, error=str(e))
    except Exception as e:
        return DownloadResult(False, error=str(e))


# ─────────────────────────────────────────────
# INSTAGRAPI CLIENT (for photo downloads)
# ─────────────────────────────────────────────

def _get_instagrapi_client():
    global _INSTAGRAPI_CLIENT
    if _INSTAGRAPI_CLIENT is not None:
        return _INSTAGRAPI_CLIENT

    b64 = os.environ.get("INSTAGRAM_SESSION_B64", "").strip()
    if not b64:
        logger.warning("INSTAGRAM_SESSION_B64 not set — cannot use instagrapi for photos.")
        return None

    try:
        from instagrapi import Client
        session_data = json.loads(base64.b64decode(b64).decode())
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, prefix="ig_session_")
        json.dump(session_data, tmp)
        tmp.close()

        cl = Client()
        cl.delay_range = [1, 3]
        cl.load_settings(tmp.name)
        cl.get_timeline_feed()
        _INSTAGRAPI_CLIENT = cl
        logger.info("instagrapi client initialized from SESSION_B64.")
        return cl
    except Exception as e:
        logger.error(f"Failed to initialize instagrapi client: {e}")
        return None


def _best_photo_url(media_obj) -> str:
    """Get the highest resolution photo URL from an instagrapi media object."""
    candidates = []

    if hasattr(media_obj, "image_versions2") and media_obj.image_versions2:
        try:
            versions = media_obj.image_versions2.get("candidates", [])
            if versions:
                best = sorted(versions, key=lambda v: v.get("width", 0) * v.get("height", 0), reverse=True)
                if best:
                    candidates.append(best[0].get("url"))
        except Exception:
            pass

    if hasattr(media_obj, "thumbnail_url") and media_obj.thumbnail_url:
        candidates.append(str(media_obj.thumbnail_url))

    for url in candidates:
        if url:
            return url
    return None


# ─────────────────────────────────────────────
# INSTAGRAPI PHOTO DOWNLOADER
# ─────────────────────────────────────────────

def _download_photos_instagrapi(url: str, base_prefix: str) -> list:
    """
    Download photos (single or carousel) using instagrapi.
    Handles: single photo, carousel of photos, mixed photo+video carousel.
    """
    cl = _get_instagrapi_client()
    if cl is None:
        return [DownloadResult(False, error="instagrapi not available — cannot download photos")]

    shortcode = _extract_shortcode(url)
    if not shortcode:
        return [DownloadResult(False, error=f"Could not extract shortcode from URL: {url}")]

    try:
        media_pk = cl.media_pk_from_code(shortcode)
        media    = cl.media_info(media_pk)
    except Exception as e:
        return [DownloadResult(False, error=f"instagrapi media_info failed: {e}")]

    media_type_id = media.media_type  # 1=photo, 2=video, 8=carousel
    logger.info(f"instagrapi: media_type={media_type_id}, shortcode={shortcode}")

    results = []

    # ── CAROUSEL (type 8) ─────────────────────────────────────────────
    if media_type_id == 8:
        resources = media.resources or []
        total     = len(resources)
        logger.info(f"instagrapi carousel: {total} item(s)")

        for i, resource in enumerate(resources):
            item_num   = i + 1
            item_type  = resource.media_type  # 1=photo, 2=video
            item_prefix = f"{base_prefix}_ig_item{item_num:03d}"

            if item_type == 2:
                # Video inside carousel — use yt-dlp
                video_url = str(resource.video_url) if resource.video_url else None
                if video_url:
                    logger.info(f"  [{item_num}/{total}] Carousel video — downloading via yt-dlp")
                    opts   = _video_opts(item_prefix + ".%(ext)s")
                    result = _download_entry(video_url, item_prefix, opts)
                    if not result.success:
                        opts2  = _universal_opts(item_prefix + "_fb.%(ext)s")
                        result = _download_entry(video_url, item_prefix + "_fb", opts2)
                else:
                    result = DownloadResult(False, error=f"No video URL for carousel item {item_num}")
            else:
                # Photo inside carousel — direct download
                photo_url = _best_photo_url(resource)
                if photo_url:
                    logger.info(f"  [{item_num}/{total}] Carousel photo — direct download")
                    result = _download_direct_url(photo_url, item_prefix, "jpg")
                else:
                    result = DownloadResult(False, error=f"No photo URL for carousel item {item_num}")

            if result.success:
                size_mb = Path(result.file_path).stat().st_size / (1024 * 1024)
                logger.info(f"  [{item_num}/{total}] {Path(result.file_path).name} [{result.media_type}] [{size_mb:.2f}MB]")
            else:
                logger.warning(f"  [{item_num}/{total}] FAILED: {result.error}")

            results.append(result)

    # ── SINGLE VIDEO (type 2) ─────────────────────────────────────────
    elif media_type_id == 2:
        video_url = str(media.video_url) if media.video_url else None
        if video_url:
            logger.info("instagrapi: single video — downloading via yt-dlp")
            item_prefix = base_prefix + "_ig_video"
            opts        = _video_opts(item_prefix + ".%(ext)s")
            result      = _download_entry(video_url, item_prefix, opts)
            if not result.success:
                opts2  = _universal_opts(item_prefix + "_fb.%(ext)s")
                result = _download_entry(video_url, item_prefix + "_fb", opts2)
            results.append(result)
        else:
            results.append(DownloadResult(False, error="No video URL from instagrapi"))

    # ── SINGLE PHOTO (type 1) ─────────────────────────────────────────
    else:
        photo_url = _best_photo_url(media)
        if photo_url:
            logger.info("instagrapi: single photo — direct download")
            item_prefix = base_prefix + "_ig_photo"
            result      = _download_direct_url(photo_url, item_prefix, "jpg")
            results.append(result)
        else:
            results.append(DownloadResult(False, error="No photo URL from instagrapi"))

    success_count = sum(1 for r in results if r.success)
    logger.info(f"instagrapi complete: {success_count}/{len(results)} succeeded")
    return results


# ─────────────────────────────────────────────
# CAROUSEL: INCREMENTAL ONE-BY-ONE (yt-dlp)
# ─────────────────────────────────────────────

def _download_carousel(entries: list, base_prefix: str) -> list:
    total   = len(entries)
    results = []
    logger.info(f"Starting carousel download: {total} item(s)")

    for i, entry in enumerate(entries):
        item_num    = i + 1
        item_url    = entry.get("webpage_url") or entry.get("url")
        item_id     = entry.get("id") or f"item{item_num:03d}"
        item_prefix = f"{base_prefix}_item{item_num:03d}_{item_id}"

        if not item_url:
            logger.warning(f"  [{item_num}/{total}] No URL found for entry, skipping")
            results.append(DownloadResult(False, error=f"No URL for item {item_num}"))
            continue

        expected_type = _detect_media_type_from_entry(entry)
        logger.info(f"  [{item_num}/{total}] Downloading [{expected_type}]: {item_id}")

        if expected_type == "Videos":
            opts   = _video_opts(item_prefix + ".%(ext)s")
            result = _download_entry(item_url, item_prefix, opts)
            if not result.success:
                logger.info(f"  [{item_num}/{total}] Video opts failed, retrying universal")
                opts2  = _universal_opts(item_prefix + "_retry.%(ext)s")
                result = _download_entry(item_url, item_prefix + "_retry", opts2)
        else:
            opts   = _universal_opts(item_prefix + ".%(ext)s")
            result = _download_entry(item_url, item_prefix, opts)

        if result.success:
            _log_item(item_num, total, result.file_path, result.media_type)
        else:
            logger.warning(f"  [{item_num}/{total}] FAILED: {result.error[:100]}")

        results.append(result)

    success_count = sum(1 for r in results if r.success)
    logger.info(f"Carousel done: {success_count}/{total} succeeded")
    return results


# ─────────────────────────────────────────────
# MAIN PUBLIC FUNCTIONS
# ─────────────────────────────────────────────

def download(url: str, platform: str) -> DownloadResult:
    results = download_all(url, platform)
    for r in results:
        if r.success:
            return r
    return results[0] if results else DownloadResult(False, error="No results returned")


def download_all(url: str, platform: str) -> list:
    """
    Download ALL items from a URL, one by one (incremental).

    Strategy:
      - Reels / Facebook videos  → yt-dlp directly (fast)
      - /p/ posts                → yt-dlp probe first,
                                   fallback to instagrapi if yt-dlp fails
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    unique_id   = uuid.uuid4().hex
    base_prefix = str(TEMP_DIR / f"{platform}_{unique_id}")
    url_lower   = url.lower()

    logger.info(f"Processing URL: {url[:80]}")

    # ── REELS / VIDEOS: go straight to yt-dlp ─────────────────────────
    if "/reel/" in url_lower or "/reels/" in url_lower or "/tv/" in url_lower \
            or "fb.watch" in url_lower or "/videos/" in url_lower or "/watch" in url_lower:
        logger.info("Reel/video URL — yt-dlp direct download")
        item_prefix = base_prefix + "_single"
        opts        = _video_opts(item_prefix + ".%(ext)s")
        result      = _download_entry(url, item_prefix, opts)
        if not result.success:
            opts2  = _universal_opts(item_prefix + "_fb.%(ext)s")
            result = _download_entry(url, item_prefix + "_fb", opts2)
        return [result]

    # ── /p/ POSTS: try yt-dlp probe first, fallback to instagrapi ─────
    if "/p/" in url_lower:
        probe_opts = _universal_opts(base_prefix + "_probe.%(ext)s")
        info       = _extract_info_only(url, probe_opts)

        if info is None:
            probe_opts2 = _video_opts(base_prefix + "_probe2.%(ext)s")
            info        = _extract_info_only(url, probe_opts2)

        if info is None:
            logger.info(f"yt-dlp probe failed — switching to instagrapi: {url[:80]}")
            return _download_photos_instagrapi(url, base_prefix)

        entries = info.get("entries")

        if entries:
            entries = [e for e in entries if e]
            total   = len(entries)

            if total == 0:
                logger.info("Empty entries — switching to instagrapi")
                return _download_photos_instagrapi(url, base_prefix)

            types       = [_detect_media_type_from_entry(e) for e in entries]
            photo_count = types.count("Images")
            video_count = types.count("Videos")

            if photo_count > 0 and video_count > 0:
                logger.info(f"Mixed carousel: {photo_count} photo(s) + {video_count} video(s)")
            elif photo_count > 0:
                logger.info(f"Photo carousel: {photo_count} photo(s)")
            else:
                logger.info(f"Video carousel: {video_count} video(s)")

            return _download_carousel(entries, base_prefix)

        else:
            media_type  = _detect_media_type_from_entry(info)
            item_prefix = base_prefix + "_single"
            logger.info(f"Single item [{media_type}]: {url[:80]}")

            if media_type == "Videos":
                opts   = _video_opts(item_prefix + ".%(ext)s")
                result = _download_entry(url, item_prefix, opts)
                if not result.success:
                    err_lower = (result.error or "").lower()
                    if "no video formats found" in err_lower:
                        logger.info("No video — switching to instagrapi")
                        return _download_photos_instagrapi(url, base_prefix)
                    opts2  = _universal_opts(item_prefix + "_fb.%(ext)s")
                    result = _download_entry(url, item_prefix + "_fb", opts2)
                return [result]
            else:
                opts   = _universal_opts(item_prefix + ".%(ext)s")
                result = _download_entry(url, item_prefix, opts)
                if not result.success:
                    logger.info("yt-dlp photo failed — switching to instagrapi")
                    return _download_photos_instagrapi(url, base_prefix)
                return [result]

    # ── UNKNOWN URL FORMAT ─────────────────────────────────────────────
    logger.warning(f"Unknown URL format — attempting generic yt-dlp: {url[:80]}")
    item_prefix = base_prefix + "_generic"
    opts        = _universal_opts(item_prefix + ".%(ext)s")
    result      = _download_entry(url, item_prefix, opts)
    return [result]


# ─────────────────────────────────────────────
# PERMANENT ERROR CHECK
# ─────────────────────────────────────────────

def _is_permanent(err_lower: str) -> bool:
    signals = [
        "unsupported url", "playlist returned no entries",
        "cannot parse data", "private video", "login required",
        "has been removed", "page not found", "404", "does not exist",
    ]
    return any(s in err_lower for s in signals)
