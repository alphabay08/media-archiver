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
    """Works for both photos and videos — no format restriction."""
    opts = _common_opts(output_template)
    opts["format"] = "best"
    return opts


# ─────────────────────────────────────────────
# RESULT CLASS
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
# HELPERS
# ─────────────────────────────────────────────

def _detect_media_type_from_entry(entry: dict) -> str:
    """Detect if a yt-dlp entry is a photo or video."""
    ext    = (entry.get("ext") or "").lower()
    vcodec = (entry.get("vcodec") or "none").lower()
    acodec = (entry.get("acodec") or "none").lower()

    if ext in ("jpg", "jpeg", "png", "webp", "gif"):
        return "Images"
    if vcodec not in ("none", "") or ext in ("mp4", "mkv", "webm", "mov", "avi"):
        return "Videos"
    # No video codec and no image ext — treat as image
    if vcodec in ("none", "") and acodec in ("none", ""):
        return "Images"
    return "Videos"


def _detect_media_type_from_file(file_path: str) -> str:
    """Detect media type from actual file extension."""
    ext = Path(file_path).suffix.lower().lstrip(".")
    if ext in ("jpg", "jpeg", "png", "webp", "gif"):
        return "Images"
    if ext in ("mp4", "mkv", "webm", "mov", "avi"):
        return "Videos"
    return "Videos"


def _find_files_by_prefix(prefix: str) -> list:
    """Find all downloaded files matching a prefix, sorted by name."""
    prefix_name = Path(prefix).name
    try:
        files = sorted([
            str(f) for f in TEMP_DIR.iterdir()
            if f.name.startswith(prefix_name) and f.is_file() and f.stat().st_size > 0
        ])
        return files
    except Exception:
        return []


def _find_one_file(prefix: str):
    """Find the largest file matching a prefix."""
    files = _find_files_by_prefix(prefix)
    if not files:
        return None
    return max(files, key=lambda p: Path(p).stat().st_size)


def _log_item(index: int, total: int, file_path: str, media_type: str):
    size_mb = Path(file_path).stat().st_size / (1024 * 1024)
    logger.info(f"  [{index}/{total}] {Path(file_path).name} [{media_type}] [{size_mb:.2f}MB]")


# ─────────────────────────────────────────────
# INFO EXTRACTION (no download)
# ─────────────────────────────────────────────

def _extract_info_only(url: str, opts: dict):
    """Extract metadata without downloading. Returns info dict or None."""
    probe_opts = dict(opts)
    probe_opts["quiet"]       = True
    probe_opts["no_warnings"] = True
    try:
        with yt_dlp.YoutubeDL(probe_opts) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception:
        return None


# ─────────────────────────────────────────────
# SINGLE ENTRY DOWNLOAD
# ─────────────────────────────────────────────

def _download_entry(url: str, file_prefix: str, opts: dict) -> DownloadResult:
    """
    Download a single URL with given opts.
    Returns DownloadResult with correct media_type.
    """
    output_template = file_prefix + ".%(ext)s"
    opts = dict(opts)
    opts["outtmpl"] = output_template

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info is None:
                return DownloadResult(False, error="yt-dlp returned no info")

            # If it returned a playlist unexpectedly, take first entry
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
# CAROUSEL: INCREMENTAL ONE-BY-ONE DOWNLOAD
# ─────────────────────────────────────────────

def _download_carousel(entries: list, base_prefix: str) -> list:
    """
    Download each carousel item one by one.
    Works for:
      - Multiple photos (20+ supported)
      - Mixed photos + videos
      - Any combination
    """
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

        # Detect expected media type from entry metadata
        expected_type = _detect_media_type_from_entry(entry)
        logger.info(f"  [{item_num}/{total}] Downloading [{expected_type}]: {item_id}")

        # Choose opts based on expected type
        if expected_type == "Videos":
            opts   = _video_opts(item_prefix + ".%(ext)s")
            result = _download_entry(item_url, item_prefix, opts)

            # If video opts fail, retry with universal (might actually be a photo)
            if not result.success:
                logger.info(f"  [{item_num}/{total}] Video opts failed, retrying with universal opts")
                opts2  = _universal_opts(item_prefix + "_retry.%(ext)s")
                result = _download_entry(item_url, item_prefix + "_retry", opts2)

        else:
            # Photo — use universal opts
            opts   = _universal_opts(item_prefix + ".%(ext)s")
            result = _download_entry(item_url, item_prefix, opts)

        if result.success:
            _log_item(item_num, total, result.file_path, result.media_type)
        else:
            logger.warning(f"  [{item_num}/{total}] FAILED: {result.error[:100]}")

        results.append(result)

    success_count = sum(1 for r in results if r.success)
    fail_count    = total - success_count
    logger.info(f"Carousel done: {success_count}/{total} succeeded, {fail_count}/{total} failed")

    return results


# ─────────────────────────────────────────────
# MAIN PUBLIC FUNCTIONS
# ─────────────────────────────────────────────

def download(url: str, platform: str) -> DownloadResult:
    """Download single item. Returns first successful result."""
    results = download_all(url, platform)
    for r in results:
        if r.success:
            return r
    return results[0] if results else DownloadResult(False, error="No results returned")


def download_all(url: str, platform: str) -> list:
    """
    Download ALL items from a URL, one by one (incremental).

    Handles all 3 cases:
      Case 1 — Single photo     → downloads 1 image
      Case 2 — Multiple photos  → downloads each photo one by one (20+ supported)
      Case 3 — Mixed (photo+video) carousel → downloads each item one by one

    Returns list of DownloadResult objects.
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    unique_id   = uuid.uuid4().hex
    base_prefix = str(TEMP_DIR / f"{platform}_{unique_id}")

    logger.info(f"Probing URL: {url[:80]}")

    # ── STEP 1: Probe the URL to understand content type ──
    probe_opts = _universal_opts(base_prefix + "_probe.%(ext)s")
    info       = _extract_info_only(url, probe_opts)

    if info is None:
        # Try video opts as second probe attempt
        probe_opts2 = _video_opts(base_prefix + "_probe2.%(ext)s")
        info        = _extract_info_only(url, probe_opts2)

    if info is None:
        # Probe failed — if it's a /p/ URL it's likely a photo, attempt direct download
        if "/p/" in url.lower():
            logger.info(f"Probe failed for /p/ URL — attempting direct photo download: {url[:80]}")
            item_prefix = base_prefix + "_direct"
            opts        = _universal_opts(item_prefix + ".%(ext)s")
            result      = _download_entry(url, item_prefix, opts)
            if result.success:
                return [result]
            # If direct also failed, try as carousel (some /p/ posts are albums)
            logger.info(f"Direct download failed too — trying carousel extraction: {url[:80]}")
            carousel_opts = _universal_opts(base_prefix + "_carousel_probe.%(ext)s")
            info2 = None
            try:
                carousel_opts_dl = dict(carousel_opts)
                carousel_opts_dl["extract_flat"] = False
                with yt_dlp.YoutubeDL(carousel_opts_dl) as ydl:
                    info2 = ydl.extract_info(url, download=False)
            except Exception:
                pass
            if info2 and info2.get("entries"):
                entries2 = [e for e in info2["entries"] if e]
                if entries2:
                    return _download_carousel(entries2, base_prefix)
            return [result]  # Return original failure
        logger.error(f"Could not probe URL: {url[:80]}")
        return [DownloadResult(False, error="Could not extract info from URL — may be private or unavailable")]

    entries = info.get("entries")

    # ── CASE: CAROUSEL (multiple items) ───────────────────────────────
    if entries:
        entries = [e for e in entries if e]
        total   = len(entries)

        if total == 0:
            return [DownloadResult(False, error="Carousel had 0 valid entries")]

        # Identify content mix for logging
        types = [_detect_media_type_from_entry(e) for e in entries]
        photo_count = types.count("Images")
        video_count = types.count("Videos")

        if photo_count > 0 and video_count > 0:
            logger.info(f"Mixed carousel: {photo_count} photo(s) + {video_count} video(s) = {total} total")
        elif photo_count > 0:
            logger.info(f"Photo carousel: {photo_count} photo(s)")
        else:
            logger.info(f"Video carousel: {video_count} video(s)")

        return _download_carousel(entries, base_prefix)

    # ── CASE: SINGLE ITEM ─────────────────────────────────────────────
    else:
        media_type = _detect_media_type_from_entry(info)
        logger.info(f"Single item [{media_type}]: {url[:80]}")

        item_prefix = base_prefix + "_single"

        if media_type == "Videos":
            # Try best video quality first
            opts   = _video_opts(item_prefix + ".%(ext)s")
            result = _download_entry(url, item_prefix, opts)

            if not result.success:
                err_lower = (result.error or "").lower()
                # If no video found, it might actually be a photo post
                if "no video formats found" in err_lower or "no video" in err_lower:
                    logger.info("No video formats found — retrying as photo")
                    opts2      = _universal_opts(item_prefix + "_ph.%(ext)s")
                    result     = _download_entry(url, item_prefix + "_ph", opts2)
                else:
                    # Generic fallback
                    logger.info("Video download failed — trying universal fallback")
                    opts2  = _universal_opts(item_prefix + "_fb.%(ext)s")
                    result = _download_entry(url, item_prefix + "_fb", opts2)

            return [result]

        else:
            # Photo
            opts   = _universal_opts(item_prefix + ".%(ext)s")
            result = _download_entry(url, item_prefix, opts)
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
