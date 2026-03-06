"""
modules/downloader.py — V1 Hybrid + Cookies
─────────────────────────────────────────────
3-layer fallback download chain:

  Layer 1 → yt-dlp     (with Instagram cookies — authenticated)
  Layer 2 → gallery-dl (with Instagram cookies — authenticated)
  Layer 3 → instagrapi (session login — absolute last resort)

Cookie auth vs instagrapi:
  - Cookies = browser-based auth (less suspicious to Instagram)
  - instagrapi = private API calls (more suspicious, triggers challenges)
  - Both use same account but cookies are much safer from Render IP

Setup:
  Add INSTAGRAM_COOKIES_B64 to Render environment variables.
  Generate with: base64 encode your instagram.com_cookies.txt file
"""

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

_INSTAGRAPI_CLIENT  = None
_RATE_LIMITED_UNTIL = 0
_COOKIE_FILE_PATH   = None   # cached cookie file path
RATE_LIMIT_COOLDOWN = int(os.environ.get("RATE_LIMIT_COOLDOWN", 300))


# ─────────────────────────────────────────────
# RESULT CLASS
# ─────────────────────────────────────────────

class DownloadResult:
    def __init__(self, success: bool, file_path: str = None,
                 media_type: str = "Videos", error: str = None,
                 rate_limited: bool = False):
        self.success      = success
        self.file_path    = file_path
        self.media_type   = media_type
        self.error        = error
        self.rate_limited = rate_limited

    def __repr__(self):
        if self.success:
            return f"<DownloadResult OK {self.media_type} {self.file_path}>"
        return f"<DownloadResult FAIL {self.error}>"


# ─────────────────────────────────────────────
# COOKIE HELPER
# ─────────────────────────────────────────────

def _get_cookie_file() -> str:
    """
    Decode INSTAGRAM_COOKIES_B64 env var into a temp cookie file.
    Returns file path or None if not set.
    Caches the file path so we only decode once.
    """
    global _COOKIE_FILE_PATH

    # Return cached path if already created and file still exists
    if _COOKIE_FILE_PATH and Path(_COOKIE_FILE_PATH).exists():
        return _COOKIE_FILE_PATH

    b64 = os.environ.get("INSTAGRAM_COOKIES_B64", "").strip()
    if not b64:
        logger.debug("INSTAGRAM_COOKIES_B64 not set — yt-dlp running without cookies")
        return None

    try:
        cookie_data = base64.b64decode(b64).decode("utf-8")
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="ig_cookies_"
        )
        tmp.write(cookie_data)
        tmp.close()
        _COOKIE_FILE_PATH = tmp.name
        logger.info(f"Instagram cookies loaded into: {tmp.name}")
        return _COOKIE_FILE_PATH
    except Exception as e:
        logger.warning(f"Failed to decode INSTAGRAM_COOKIES_B64: {e}")
        return None


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _detect_media_type(file_path: str) -> str:
    ext = Path(file_path).suffix.lower().lstrip(".")
    return "Images" if ext in ("jpg", "jpeg", "png", "webp", "gif") else "Videos"


def _extract_shortcode(url: str) -> str:
    match = re.search(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_\-]+)", url)
    return match.group(1) if match else None


def _find_downloaded_files(prefix: str) -> list:
    prefix_name = Path(prefix).name
    try:
        files = [
            str(f) for f in TEMP_DIR.iterdir()
            if f.name.startswith(prefix_name) and f.is_file() and f.stat().st_size > 0
        ]
        return sorted(files)
    except Exception:
        return []


def _find_one_file(prefix: str) -> str:
    files = _find_downloaded_files(prefix)
    if not files:
        return None
    return max(files, key=lambda p: Path(p).stat().st_size)


def _download_direct_url(url: str, file_prefix: str, ext: str) -> DownloadResult:
    file_path = file_prefix + f".{ext}"
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
            "Referer":    "https://www.instagram.com/",
        }
        resp = requests.get(url, headers=headers, stream=True, timeout=120)
        resp.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return DownloadResult(False, error="Downloaded file is empty")
        media_type = _detect_media_type(file_path)
        size_mb    = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"  CDN: {Path(file_path).name} [{media_type}] [{size_mb:.2f}MB]")
        return DownloadResult(True, file_path=file_path, media_type=media_type)
    except Exception as e:
        return DownloadResult(False, error=f"CDN download failed: {e}")


# ─────────────────────────────────────────────
# INSTAGRAPI CLIENT (last resort only)
# ─────────────────────────────────────────────

def _get_instagrapi_client():
    global _INSTAGRAPI_CLIENT
    if _INSTAGRAPI_CLIENT is not None:
        return _INSTAGRAPI_CLIENT
    b64 = os.environ.get("INSTAGRAM_SESSION_B64", "").strip()
    if not b64:
        return None
    try:
        from instagrapi import Client
        session_data = json.loads(base64.b64decode(b64).decode())
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, prefix="ig_session_"
        )
        json.dump(session_data, tmp)
        tmp.close()
        cl = Client()
        cl.delay_range = [2, 4]
        cl.load_settings(tmp.name)
        cl.get_timeline_feed()
        _INSTAGRAPI_CLIENT = cl
        logger.info("instagrapi ready (last resort layer).")
        return cl
    except Exception as e:
        logger.error(f"instagrapi init failed: {e}")
        _INSTAGRAPI_CLIENT = None
        return None


def _reset_instagrapi():
    global _INSTAGRAPI_CLIENT
    _INSTAGRAPI_CLIENT = None


def _set_rate_limited():
    global _RATE_LIMITED_UNTIL
    _RATE_LIMITED_UNTIL = time.time() + RATE_LIMIT_COOLDOWN
    logger.warning(f"Rate limited — cooldown {RATE_LIMIT_COOLDOWN}s")


def _is_rate_limited() -> bool:
    return time.time() < _RATE_LIMITED_UNTIL


def _is_rate_limit_error(err: str) -> bool:
    return any(s in err.lower() for s in [
        "login_required", "403", "401", "rate", "spam",
        "checkpoint", "challenge", "please wait", "blocked",
        "feedback_required", "expecting value",
    ])


def _is_permanent_error(err: str) -> bool:
    return any(s in err.lower() for s in [
        "media not found", "no media", "does not exist",
        "has been removed", "page not found", "unsupported url",
    ])


# ─────────────────────────────────────────────
# LAYER 1 — yt-dlp (with cookies)
# ─────────────────────────────────────────────

def _try_ytdlp(url: str, base_prefix: str) -> list:
    cookie_file = _get_cookie_file()
    has_cookies = cookie_file is not None
    logger.info(f"  Layer 1: yt-dlp ({'with cookies ✓' if has_cookies else 'no cookies'})")

    user_agents = [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
        "Mozilla/5.0 (Android 13; Mobile; rv:109.0) Gecko/113.0 Firefox/113.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
        "Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; OnePlus; ONEPLUS A3010)",
    ]

    item_prefix = base_prefix + "_ytdlp"

    # FIX 1: outtmpl must use full absolute TEMP_DIR path
    # Without this yt-dlp saves relative to current working directory
    opts = {
        "outtmpl":             str(TEMP_DIR / (Path(item_prefix).name + "_%(autonumber)s.%(ext)s")),
        "quiet":               True,
        "noprogress":          True,
        # FIX 2: simpler format selector — Instagram doesn't always have
        # separate video+audio streams, so "best" is safer fallback
        "format":              "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best/best",
        "merge_output_format": "mp4",
        "socket_timeout":      30,
        "retries":             2,
        "geo_bypass":          True,
        "noplaylist":          False,
        "http_headers": {
            "User-Agent":      random.choice(user_agents),
            "Accept-Language": "en-US,en;q=0.9",
        },
        # FIX 3: extractor args help yt-dlp find formats Instagram hides
        "extractor_args": {
            "instagram": {
                "app_id": ["936619743392459"],
            }
        },
    }

    # Add cookies if available
    if cookie_file:
        opts["cookiefile"] = cookie_file

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)

        files = _find_downloaded_files(item_prefix)
        if not files:
            logger.info("  yt-dlp: no files found in TEMP_DIR after download")
            return []

        results = []
        for f in files:
            media_type = _detect_media_type(f)
            size_mb    = Path(f).stat().st_size / (1024 * 1024)
            logger.info(f"  yt-dlp: {Path(f).name} [{media_type}] [{size_mb:.2f}MB]")
            results.append(DownloadResult(True, file_path=f, media_type=media_type))
        return results

    except Exception as e:
        logger.info(f"  yt-dlp failed: {str(e)[:120]}")
        return []


# ─────────────────────────────────────────────
# LAYER 2 — gallery-dl (with cookies)
# ─────────────────────────────────────────────

def _try_gallery_dl(url: str, base_prefix: str) -> list:
    cookie_file = _get_cookie_file()
    has_cookies = cookie_file is not None
    logger.info(f"  Layer 2: gallery-dl ({'with cookies ✓' if has_cookies else 'no cookies'})")

    try:
        import gallery_dl
        import gallery_dl.job
        import gallery_dl.config
    except ImportError:
        logger.info("  gallery-dl not installed — skipping")
        return []

    item_prefix = base_prefix + "_gdl"

    try:
        gallery_dl.config.clear()
        # FIX: set base-directory to absolute TEMP_DIR
        # Without this gallery-dl creates ./gallery-dl/ subfolder
        # and files are not found by _find_downloaded_files()
        gallery_dl.config.set((), "base-directory", str(TEMP_DIR))
        gallery_dl.config.set((), "directory",      [])
        gallery_dl.config.set((), "filename",       Path(item_prefix).name + "_{num}.{extension}")
        gallery_dl.config.set((), "retries",   2)
        gallery_dl.config.set((), "timeout",   30)
        gallery_dl.config.set(
            ("extractor",), "user-agent",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"
        )

        # Add cookies if available
        if cookie_file:
            gallery_dl.config.set(("extractor",), "cookies", cookie_file)
            logger.debug(f"  gallery-dl using cookie file: {cookie_file}")

        job = gallery_dl.job.DownloadJob(url)
        job.run()

        files = _find_downloaded_files(item_prefix)
        if not files:
            return []

        results = []
        for f in files:
            media_type = _detect_media_type(f)
            size_mb    = Path(f).stat().st_size / (1024 * 1024)
            logger.info(f"  gallery-dl: {Path(f).name} [{media_type}] [{size_mb:.2f}MB]")
            results.append(DownloadResult(True, file_path=f, media_type=media_type))
        return results

    except Exception as e:
        logger.info(f"  gallery-dl failed: {str(e)[:120]}")
        return []


# ─────────────────────────────────────────────
# LAYER 3 — instagrapi (last resort)
# ─────────────────────────────────────────────

def _try_instagrapi(url: str, base_prefix: str) -> list:
    logger.info("  Layer 3: instagrapi (last resort)")

    if _is_rate_limited():
        remaining = int(_RATE_LIMITED_UNTIL - time.time())
        logger.warning(f"  Still rate limited — {remaining}s remaining")
        return [DownloadResult(False, error="instagram_rate_limited", rate_limited=True)]

    cl = _get_instagrapi_client()
    if cl is None:
        return [DownloadResult(False,
            error="instagrapi session not available — refresh INSTAGRAM_SESSION_B64")]

    shortcode = _extract_shortcode(url)
    if not shortcode:
        return [DownloadResult(False, error=f"Could not extract shortcode: {url}")]

    delay = random.uniform(3, 8)
    logger.info(f"  Human delay: {delay:.1f}s")
    time.sleep(delay)

    try:
        media_pk = cl.media_pk_from_code(shortcode)
        media    = cl.media_info(media_pk)
    except Exception as e:
        err = str(e)
        if _is_rate_limit_error(err):
            _set_rate_limited()
            _reset_instagrapi()
            return [DownloadResult(False,
                error=f"instagram_rate_limited: {err}", rate_limited=True)]
        if _is_permanent_error(err):
            return [DownloadResult(False, error=f"Permanent: {err}")]
        return [DownloadResult(False, error=f"instagrapi failed: {err}")]

    media_type_id = media.media_type
    results       = []

    def _best_photo_url(obj):
        if hasattr(obj, "image_versions2") and obj.image_versions2:
            try:
                versions = obj.image_versions2.get("candidates", [])
                if versions:
                    best = sorted(versions,
                        key=lambda v: v.get("width", 0) * v.get("height", 0), reverse=True)
                    if best and best[0].get("url"):
                        return best[0]["url"]
            except Exception:
                pass
        if hasattr(obj, "thumbnail_url") and obj.thumbnail_url:
            return str(obj.thumbnail_url)
        return None

    # Carousel
    if media_type_id == 8:
        resources = media.resources or []
        logger.info(f"  instagrapi carousel: {len(resources)} item(s)")
        for i, resource in enumerate(resources):
            item_prefix = f"{base_prefix}_ig_item{i+1:03d}"
            if resource.media_type == 2:
                video_url = str(resource.video_url) if resource.video_url else None
                result    = _download_direct_url(video_url, item_prefix, "mp4") \
                            if video_url else DownloadResult(False, error="No video URL")
            else:
                photo_url = _best_photo_url(resource)
                result    = _download_direct_url(photo_url, item_prefix, "jpg") \
                            if photo_url else DownloadResult(False, error="No photo URL")
            results.append(result)

    # Single video
    elif media_type_id == 2:
        video_url = str(media.video_url) if media.video_url else None
        results.append(
            _download_direct_url(video_url, base_prefix + "_ig_reel", "mp4")
            if video_url else DownloadResult(False, error="No video URL")
        )

    # Single photo
    else:
        photo_url = _best_photo_url(media)
        results.append(
            _download_direct_url(photo_url, base_prefix + "_ig_photo", "jpg")
            if photo_url else DownloadResult(False, error="No photo URL")
        )

    return results


# ─────────────────────────────────────────────
# FACEBOOK
# ─────────────────────────────────────────────

def _download_facebook(url: str, base_prefix: str) -> list:
    logger.info("  Facebook: yt-dlp")
    item_prefix = base_prefix + "_fb"
    opts = {
        "outtmpl":             item_prefix + ".%(ext)s",
        "quiet":               True,
        "noprogress":          True,
        "format":              "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "merge_output_format": "mp4",
        "socket_timeout":      60,
        "retries":             3,
        "geo_bypass":          True,
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
        },
    }
    cookie_file = _get_cookie_file()
    if cookie_file:
        opts["cookiefile"] = cookie_file

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)
        downloaded = _find_one_file(item_prefix)
        if not downloaded:
            return [DownloadResult(False, error="File not found after yt-dlp")]
        return [DownloadResult(True, file_path=downloaded,
                               media_type=_detect_media_type(downloaded))]
    except Exception as e:
        return [DownloadResult(False, error=str(e))]


# ─────────────────────────────────────────────
# MAIN ENTRY — 3-LAYER FALLBACK CHAIN
# ─────────────────────────────────────────────

def download_all(url: str, platform: str) -> list:
    """
    Layer 1: yt-dlp     (with cookies — handles ~80% of links)
    Layer 2: gallery-dl (with cookies — handles photos/carousels)
    Layer 3: instagrapi (private API  — absolute last resort ~5%)
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    unique_id   = uuid.uuid4().hex
    base_prefix = str(TEMP_DIR / f"{platform}_{unique_id}")
    url_lower   = url.lower()

    logger.info(f"Processing: {url[:80]}")

    # Facebook
    if any(x in url_lower for x in ["facebook.com", "fb.watch", "fb.com"]):
        return _download_facebook(url, base_prefix)

    # Instagram — 3-layer fallback
    if "instagram.com" in url_lower or "instagr.am" in url_lower:

        # Layer 1 — yt-dlp
        results = _try_ytdlp(url, base_prefix)
        if results and any(r.success for r in results):
            logger.info(f"✓ yt-dlp succeeded [{sum(1 for r in results if r.success)} item(s)]")
            return results
        logger.info("yt-dlp failed — trying gallery-dl...")

        # Layer 2 — gallery-dl
        results = _try_gallery_dl(url, base_prefix)
        if results and any(r.success for r in results):
            logger.info(f"✓ gallery-dl succeeded [{sum(1 for r in results if r.success)} item(s)]")
            return results
        logger.info("gallery-dl failed — trying instagrapi (last resort)...")

        # Layer 3 — instagrapi
        results = _try_instagrapi(url, base_prefix)
        if results and any(r.success for r in results):
            logger.info(f"✓ instagrapi succeeded [{sum(1 for r in results if r.success)} item(s)]")
        else:
            logger.error(f"✗ All 3 layers failed: {url[:60]}")
        return results

    return [DownloadResult(False, error="Unsupported URL")]


def download(url: str, platform: str) -> DownloadResult:
    results = download_all(url, platform)
    return results[0] if results else DownloadResult(False, error="No results")
