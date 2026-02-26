import re

INSTAGRAM_PATTERNS = [
    r"instagram\.com/p/[A-Za-z0-9_\-]+",
    r"instagram\.com/reels?/[A-Za-z0-9_\-]+",
    r"instagram\.com/tv/[A-Za-z0-9_\-]+",
    r"instagr\.am/[A-Za-z0-9_\-/]+",
    r"instagram\.com/[^/\s]+/p/[A-Za-z0-9_\-]+",
    r"instagram\.com/[^/\s]+/reels?/[A-Za-z0-9_\-]+",
    r"instagram\.com/clips/[A-Za-z0-9_\-]+",
    r"instagram\.com/share/[A-Za-z0-9_\-/]+",
    r"instagram\.com/explore/[A-Za-z0-9_\-/]+",
]

FACEBOOK_PATTERNS = [
    r"facebook\.com/reels?/[0-9]+",
    r"facebook\.com/[^/\s]+/reels?/[0-9]+",
    r"facebook\.com/[^/\s]+/videos/[0-9]+",
    r"m\.facebook\.com/[^/\s]+/videos/[0-9]+",
    r"mbasic\.facebook\.com/[^/\s]+/videos/[0-9]+",
    r"web\.facebook\.com/[^/\s]+/videos/[0-9]+",
    r"facebook\.com/video\.php\?v=[0-9]+",
    r"facebook\.com/watch/?[\?&]v=[0-9]+",
    r"facebook\.com/watch/[0-9]+",
    r"facebook\.com/watch$",
    r"facebook\.com/share/v/[A-Za-z0-9_\-]+",
    r"facebook\.com/share/p/[A-Za-z0-9_\-]+",
    r"facebook\.com/share/r/[A-Za-z0-9_\-]+",
    r"facebook\.com/share/[A-Za-z0-9_\-]+",
    r"fb\.watch/[A-Za-z0-9_\-]+",
    r"fb\.com/[A-Za-z0-9_\-/]+",
    r"facebook\.com/story\.php\?story_fbid=[0-9]+",
    r"facebook\.com/permalink\.php\?story_fbid=[0-9]+",
    r"facebook\.com/[^/\s]+/posts/[0-9A-Za-z_\-]+",
    r"facebook\.com/photo/?[\?&]fbid=[0-9]+",
    r"facebook\.com/photo\.php\?fbid=[0-9]+",
    r"facebook\.com/[^/\s]+/photos/[0-9A-Za-z_\-/]+",
    r"facebook\.com/groups/[^/\s]+/posts/[0-9A-Za-z_\-]+",
    r"facebook\.com/groups/[^/\s]+/permalink/[0-9]+",
    r"facebook\.com/groups/[^/\s]+/videos/[0-9]+",
    r"facebook\.com/events/[0-9]+",
    r"facebook\.com/[0-9]+/videos/[0-9]+",
    r"facebook\.com/[0-9]+/posts/[0-9]+",
    r"m\.facebook\.com/story\.php",
]

PRIVATE_PATTERNS = [
    r"instagram\.com/stories/",
    r"instagram\.com/highlights/",
    r"instagram\.com/direct/",
    r"facebook\.com/messages/",
    r"messenger\.com/",
]

_KNOWN_DOMAINS = (
    "instagram.com", "instagr.am",
    "facebook.com", "m.facebook.com",
    "mbasic.facebook.com", "web.facebook.com",
    "fb.watch", "fb.com",
)


def detect_platform(url: str) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        if any(url.startswith(d) for d in _KNOWN_DOMAINS):
            url = "https://" + url
        else:
            return "unknown"
    for pat in PRIVATE_PATTERNS:
        if re.search(pat, url, re.IGNORECASE):
            return "private"
    for pat in INSTAGRAM_PATTERNS:
        if re.search(pat, url, re.IGNORECASE):
            return "instagram"
    for pat in FACEBOOK_PATTERNS:
        if re.search(pat, url, re.IGNORECASE):
            return "facebook"
    return "unknown"


def is_valid_url(url: str) -> bool:
    url = url.strip()
    if url.startswith(("http://", "https://")):
        return True
    if any(url.startswith(d) for d in _KNOWN_DOMAINS):
        return True
    return False


def guess_media_type(url: str) -> str:
    url_lower = url.lower()
    if any(x in url_lower for x in [
        "/reel/", "/reels/", "/tv/", "/video", "/watch",
        "fb.watch", "video.php", "/clips/", "v=",
    ]):
        return "Videos"
    if any(x in url_lower for x in ["/photo", "photo.php", "fbid=", "/photos/"]):
        return "Images"
    return "Videos"
