import os
import re
import time
import uuid
import random
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

_INSTAGRAPI_CLIENT    = None
_RATE_LIMITED_UNTIL   = 0   # epoch seconds — don't attempt until this time
RATE_LIMIT_COOLDOWN   = int(os.environ.get("RATE_LIMIT_COOLDOWN", 300))  # 5 min default


# ─────────────────────────────────────────────
# RESULT CLASS (defined first)
# ─────────────────────────────────────────────

class DownloadResult:
    def __init__(self, success: bool, file_path: str = None, media_type: str = "Videos",
                 error: str = None, rate_limited: bool = False):
        self.success      = success
        self.file_path    = file_path
        self.media_type   = media_type
        self.error        = error
        self.rate_limited = rate_limited   # True = Instagram is blocking us temporarily

    def __repr__(self):
        if self.success:
            return f"<DownloadResult OK {self.media_type} {self.file_path}>"
        tag = " [RATE_LIMITED]" if self.rate_limited else ""
        return f"<DownloadResult FAIL{tag} {self.error}>"


# ─────────────────────────────────────────────
# INSTAGRAPI CLIENT
# ─────────────────────────────────────────────

def _get_instagrapi_client():
    """
    Get instagrapi client from INSTAGRAM_SESSION_B64.
    This session lasts weeks — no browser cookies needed ever.
    """
    global _INSTAGRAPI_CLIENT
    if _INSTAGRAPI_CLIENT is not None:
        return _INSTAGRAPI_CLIENT

    b64 = os.environ.get("INSTAGRAM_SESSION_B64", "").strip()
    if not b64:
        logger.error("INSTAGRAM_SESSION_B64 not set.")
        return None

    try:
        from instagrapi import Client
        session_data = json.loads(base64.b64decode(b64).decode())
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, prefix="ig_session_")
        json.dump(session_data, tmp)
        tmp.close()

        cl = Client()
        cl.delay_range = [2, 4]   # polite delay between requests
        cl.load_settings(tmp.name)
        cl.get_timeline_feed()
        _INSTAGRAPI_CLIENT = cl
        logger.info("instagrapi client ready.")
        return cl
    except Exception as e:
        logger.error(f"instagrapi init failed: {e}")
        _INSTAGRAPI_CLIENT = None
        return None


def _reset_instagrapi_client():
    global _INSTAGRAPI_CLIENT
    _INSTAGRAPI_CLIENT = None


def _set_rate_limited():
    """Mark Instagram as rate-limited for RATE_LIMIT_COOLDOWN seconds."""
    global _RATE_LIMITED_UNTIL
    _RATE_LIMITED_UNTIL = time.time() + RATE_LIMIT_COOLDOWN
    logger.warning(f"Instagram rate limit detected — cooling down for {RATE_LIMIT_COOLDOWN}s")


def _is_rate_limited() -> bool:
    return time.time() < _RATE_LIMITED_UNTIL


def _is_rate_limit_error(err: str) -> bool:
    err = err.lower()
    return any(s in err for s in [
        "login_required", "403", "401", "rate", "spam",
        "checkpoint", "challenge", "please wait", "blocked",
        "feedback_required", "useragent mismatch",
    ])


def _is_truly_permanent(err: str) -> bool:
    """Errors that will NEVER succeed regardless of retries."""
    err = err.lower()
    return any(s in err for s in [
        "media not found", "no media", "does not exist",
        "has been removed", "page not found", "unsupported url",
        "private", "not available",
    ])


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _extract_shortcode(url: str) -> str:
    match = re.search(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_\-]+)", url)
    return match.group(1) if match else None


def _detect_media_type_from_file(file_path: str) -> str:
    ext = Path(file_path).suffix.lower().lstrip(".")
    if ext in ("jpg", "jpeg", "png", "webp", "gif"):
        return "Images"
    return "Videos"


def _find_one_file(prefix: str):
    prefix_name = Path(prefix).name
    try:
        candidates = [
            str(f) for f in TEMP_DIR.iterdir()
            if f.name.startswith(prefix_name) and f.is_file() and f.stat().st_size > 0
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda p: Path(p).stat().st_size)
    except Exception:
        return None


def _log_item(index: int, total: int, file_path: str, media_type: str):
    size_mb = Path(file_path).stat().st_size / (1024 * 1024)
    logger.info(f"  [{index}/{total}] {Path(file_path).name} [{media_type}] [{size_mb:.2f}MB]")


def _best_photo_url(media_obj) -> str:
    if hasattr(media_obj, "image_versions2") and media_obj.image_versions2:
        try:
            versions = media_obj.image_versions2.get("candidates", [])
            if versions:
                best = sorted(versions, key=lambda v: v.get("width", 0) * v.get("height", 0), reverse=True)
                if best and best[0].get("url"):
                    return best[0]["url"]
        except Exception:
            pass
    if hasattr(media_obj, "thumbnail_url") and media_obj.thumbnail_url:
        return str(media_obj.thumbnail_url)
    return None


# ─────────────────────────────────────────────
# DIRECT URL DOWNLOAD (requests)
# ─────────────────────────────────────────────

def _download_direct_url(url: str, file_prefix: str, ext: str) -> DownloadResult:
    """Download a direct CDN URL using requests — no auth needed for CDN links."""
    file_path = file_prefix + f".{ext}"
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
            "Referer": "https://www.instagram.com/",
        }
        resp = requests.get(url, headers=headers, stream=True, timeout=120)
        resp.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return DownloadResult(False, error="Downloaded file is empty")

        media_type = _detect_media_type_from_file(file_path)
        size_mb    = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"Downloaded: {Path(file_path).name} [{media_type}] [{size_mb:.2f}MB]")
        return DownloadResult(True, file_path=file_path, media_type=media_type)

    except Exception as e:
        return DownloadResult(False, error=f"Direct download failed: {e}")


# ─────────────────────────────────────────────
# INSTAGRAM DOWNLOADER (instagrapi)
# ─────────────────────────────────────────────

def _download_instagram(url: str, base_prefix: str) -> list:
    """
    Download Instagram content using instagrapi only.

    Rate limit handling:
      - If Instagram blocks us → mark rate_limited=True
      - Worker will see this and pause itself with a cooldown
      - Links are NOT marked permanent — they will retry after cooldown
      - Session itself stays valid — we just need to slow down

    Handles: reels, photos, carousels (20+ items), mixed carousels.
    """

    # Check if we're still in cooldown from a previous rate limit
    if _is_rate_limited():
        remaining = int(_RATE_LIMITED_UNTIL - time.time())
        logger.warning(f"Still rate limited — {remaining}s remaining, skipping")
        return [DownloadResult(False, error="instagram_rate_limited", rate_limited=True)]

    cl = _get_instagrapi_client()
    if cl is None:
        return [DownloadResult(False, error="instagrapi session not available — refresh INSTAGRAM_SESSION_B64")]

    shortcode = _extract_shortcode(url)
    if not shortcode:
        return [DownloadResult(False, error=f"Could not extract shortcode from: {url}")]

    # ── GET MEDIA INFO ─────────────────────────────────────────────────
    try:
        # Human-like random delay before each API call
        delay = random.uniform(3, 8)
        logger.info(f"Human delay: {delay:.1f}s")
        time.sleep(delay)

        media_pk = cl.media_pk_from_code(shortcode)
        media    = cl.media_info(media_pk)
    except Exception as e:
        err = str(e)
        if _is_rate_limit_error(err):
            _set_rate_limited()
            _reset_instagrapi_client()
            return [DownloadResult(False, error=f"instagram_rate_limited: {err}", rate_limited=True)]
        if _is_truly_permanent(err):
            return [DownloadResult(False, error=f"Permanent error: {err}")]
        return [DownloadResult(False, error=f"instagrapi failed: {err}")]

    media_type_id = media.media_type  # 1=photo, 2=video/reel, 8=carousel
    logger.info(f"Instagram media_type={media_type_id} shortcode={shortcode}")

    results = []

    # ── CAROUSEL (type 8) — download one by one ────────────────────────
    if media_type_id == 8:
        resources = media.resources or []
        total     = len(resources)
        logger.info(f"Carousel: {total} item(s) — downloading one by one")

        for i, resource in enumerate(resources):
            item_num    = i + 1
            item_type   = resource.media_type
            item_prefix = f"{base_prefix}_item{item_num:03d}"

            logger.info(f"  [{item_num}/{total}] {'video' if item_type == 2 else 'photo'}")

            if item_type == 2:
                video_url = str(resource.video_url) if resource.video_url else None
                result    = _download_direct_url(video_url, item_prefix, "mp4") if video_url \
                            else DownloadResult(False, error=f"No video URL for item {item_num}")
            else:
                photo_url = _best_photo_url(resource)
                result    = _download_direct_url(photo_url, item_prefix, "jpg") if photo_url \
                            else DownloadResult(False, error=f"No photo URL for item {item_num}")

            if result.success:
                _log_item(item_num, total, result.file_path, result.media_type)
            else:
                logger.warning(f"  [{item_num}/{total}] FAILED: {result.error}")

            results.append(result)

    # ── SINGLE VIDEO / REEL (type 2) ──────────────────────────────────
    elif media_type_id == 2:
        video_url = str(media.video_url) if media.video_url else None
        if video_url:
            logger.info("Single reel/video — direct CDN download")
            results.append(_download_direct_url(video_url, base_prefix + "_reel", "mp4"))
        else:
            results.append(DownloadResult(False, error="No video URL from instagrapi"))

    # ── SINGLE PHOTO (type 1) ─────────────────────────────────────────
    else:
        photo_url = _best_photo_url(media)
        if photo_url:
            logger.info("Single photo — direct CDN download")
            results.append(_download_direct_url(photo_url, base_prefix + "_photo", "jpg"))
        else:
            results.append(DownloadResult(False, error="No photo URL from instagrapi"))

    success_count = sum(1 for r in results if r.success)
    logger.info(f"Instagram complete: {success_count}/{len(results)} succeeded")
    return results


# ─────────────────────────────────────────────
# FACEBOOK DOWNLOADER (yt-dlp, no login needed)
# ─────────────────────────────────────────────

def _download_facebook(url: str, base_prefix: str) -> list:
    item_prefix     = base_prefix + "_fb"
    output_template = item_prefix + ".%(ext)s"

    opts = {
        "outtmpl": output_template,
        "quiet": True,
        "noprogress": True,
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best",
        "merge_output_format": "mp4",
        "socket_timeout": 60,
        "retries": 3,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        },
        "geo_bypass": True,
    }

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info is None:
                return [DownloadResult(False, error="yt-dlp returned no info")]
            downloaded = _find_one_file(item_prefix)
            if not downloaded:
                return [DownloadResult(False, error="Facebook file not found after download")]
            media_type = _detect_media_type_from_file(downloaded)
            size_mb    = Path(downloaded).stat().st_size / (1024 * 1024)
            logger.info(f"Facebook: {Path(downloaded).name} [{size_mb:.2f}MB]")
            return [DownloadResult(True, file_path=downloaded, media_type=media_type)]
    except Exception as e:
        return [DownloadResult(False, error=str(e))]


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
    Download ALL items from a URL.
    Instagram → instagrapi (no cookies, session lasts weeks)
    Facebook  → yt-dlp (public content, no login needed)
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    unique_id   = uuid.uuid4().hex
    base_prefix = str(TEMP_DIR / f"{platform}_{unique_id}")
    url_lower   = url.lower()

    logger.info(f"Processing: {url[:80]}")

    if "instagram.com" in url_lower or "instagr.am" in url_lower:
        return _download_instagram(url, base_prefix)

    if "facebook.com" in url_lower or "fb.watch" in url_lower or "fb.com" in url_lower:
        return _download_facebook(url, base_prefix)

    logger.warning(f"Unknown platform: {url[:80]}")
    return [DownloadResult(False, error="Unsupported URL")]


# ─────────────────────────────────────────────
# PERMANENT ERROR CHECK
# ─────────────────────────────────────────────

def _is_permanent(err_lower: str) -> bool:
    signals = [
        "unsupported url", "playlist returned no entries",
        "cannot parse data", "private",
        "has been removed", "page not found", "does not exist",
        "media not found", "no media",
    ]
    return any(s in err_lower for s in signals)
