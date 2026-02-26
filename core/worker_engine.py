import os
import time
import logging

from core.state_manager import StateManager
from core.retry_controller import is_retryable, backoff_delay, inter_download_delay
from modules.downloader import download, download_all
from modules.dropbox_uploader import upload_file
from modules.platform_detector import detect_platform, guess_media_type

logger = logging.getLogger(__name__)

MAX_RETRIES            = int(os.environ.get("MAX_RETRIES", 3))
QUEUE_THRESHOLD        = int(os.environ.get("QUEUE_TRIGGER_THRESHOLD", 3))
CONSECUTIVE_FAIL_LIMIT = 10
RATE_LIMIT_PAUSE       = int(os.environ.get("RATE_LIMIT_COOLDOWN", 300))

# BUG 3 FIX: Removed "private" and "not available" — these match instagrapi
# log lines like "private_request" causing valid links to be permanently failed.
_PERMANENT_ERRORS = [
    "unsupported url",
    "cannot parse data",
    "playlist returned no entries",
    "has been removed",
    "page not found",
    "this reel can't be played",
    "video unavailable",
    "does not exist",
    "media not found",
    "no media",
    "the link you followed may be broken",
]


def _is_permanent(error: str) -> bool:
    err = error.lower()
    if "rate_limited" in err or "rate limit" in err:
        return False
    # Challenge/auth errors are retryable — never permanent
    if any(s in err for s in ["challenge", "login_required", "400", "401", "403",
                               "expecting value", "feedback_required"]):
        return False
    return any(s in err for s in _PERMANENT_ERRORS)


def _is_rate_limited(error: str) -> bool:
    return "instagram_rate_limited" in error.lower() or "rate_limited" in error.lower()


def run_worker(force: bool = False):
    sm     = StateManager()
    worker = sm.get_worker_status()

    if worker["status"] == "processing":
        logger.warning("Worker already running.")
        return {"status": "already_running"}
    if worker["status"] == "paused":
        logger.warning(f"Worker is paused: {worker.get('paused_reason')}")
        return {"status": "paused", "reason": worker.get("paused_reason")}

    initial_pending = sm.get_pending_links()
    if not force and len(initial_pending) < QUEUE_THRESHOLD:
        logger.info(f"Only {len(initial_pending)} pending — below threshold {QUEUE_THRESHOLD}. Skipping.")
        return {"status": "below_threshold", "pending": len(initial_pending)}

    sm.set_worker_status("processing")
    logger.info(f"Worker started. {len(initial_pending)} link(s) in queue.")

    consecutive_failures = 0
    processed_count      = 0
    success_count        = 0

    try:
        # BUG 1 FIX: Re-fetch pending on every iteration — never use frozen snapshot
        while True:
            pending = sm.get_pending_links()
            if not pending:
                break

            link_record = pending[0]
            url         = link_record["url"]
            retry_count = link_record["retry_count"]

            processed_count += 1  # BUG 2 FIX: count every attempt

            if retry_count >= MAX_RETRIES:
                sm.mark_failed(url, "Max retries exceeded", permanent=True)
                logger.warning(f"Skipping {url[:60]} — max retries ({MAX_RETRIES}) reached.")
                continue

            logger.info(f"[{processed_count}] {url[:80]} (attempt #{retry_count + 1}/{MAX_RETRIES})")
            sm.mark_processing(url)

            platform = detect_platform(url)
            if platform == "private":
                sm.mark_failed(url, "Private content", permanent=True)
                logger.warning(f"Skipping private: {url[:60]}")
                continue
            if platform == "unknown":
                sm.mark_failed(url, "Unrecognized platform", permanent=True)
                logger.warning(f"Skipping unknown: {url[:60]}")
                continue

            # ── DOWNLOAD ALL ITEMS ──────────────────────────────────
            results = download_all(url, platform)

            # ── ALL ITEMS FAILED ────────────────────────────────────
            if not any(r.success for r in results):
                error        = results[0].error if results else "Unknown error"
                rate_limited = any(getattr(r, "rate_limited", False) for r in results)

                logger.error(f"Download failed [{url[:60]}]: {error[:120]}")

                # ── RATE LIMITED ────────────────────────────────────
                if rate_limited or _is_rate_limited(error):
                    sm.mark_failed(url, error, permanent=False)
                    logger.warning(f"Rate limit — pausing {RATE_LIMIT_PAUSE}s. Links preserved in queue.")
                    sm.set_worker_status("idle")
                    time.sleep(RATE_LIMIT_PAUSE)
                    logger.info("Cooldown done — resuming.")
                    sm.set_worker_status("processing")
                    consecutive_failures = 0
                    continue  # BUG 1 FIX: continue while loop, don't return

                # ── PERMANENT ERROR ─────────────────────────────────
                if _is_permanent(error):
                    sm.mark_failed(url, error, permanent=True)
                    consecutive_failures = 0
                    continue

                # ── RETRYABLE ERROR ─────────────────────────────────
                consecutive_failures += 1
                if consecutive_failures >= CONSECUTIVE_FAIL_LIMIT:
                    sm.mark_failed(url, error)
                    sm.set_worker_status("idle")
                    return {"status": "too_many_failures", "processed": processed_count}

                if is_retryable(error):
                    sm.mark_failed(url, error, permanent=False)
                    backoff_delay(retry_count)
                else:
                    # Unknown error — give one retry then permanent
                    sm.mark_failed(url, error, permanent=(retry_count >= 1))
                continue

            # ── UPLOAD EACH ITEM ────────────────────────────────────
            consecutive_failures = 0
            total_items          = len(results)
            last_upload_path     = None
            uploaded_count       = 0
            success_items        = [r for r in results if r.success]

            if total_items > 1:
                logger.info(f"Uploading {len(success_items)}/{total_items} item(s): {url[:60]}")

            for i, result in enumerate(results):
                item_num = i + 1
                if not result.success:
                    logger.warning(f"  Skipping item {item_num}/{total_items}: {result.error[:80]}")
                    continue

                actual_media_type = result.media_type or guess_media_type(url)
                logger.info(f"  Uploading item {item_num}/{total_items} [{actual_media_type}]...")

                ok, path_or_err = upload_file(result.file_path, platform, actual_media_type)

                try:
                    if result.file_path and os.path.exists(result.file_path):
                        os.remove(result.file_path)
                except Exception as e:
                    logger.warning(f"  Could not delete temp file: {e}")

                if ok:
                    last_upload_path = path_or_err
                    uploaded_count  += 1
                    logger.info(f"  ✓ Item {item_num}/{total_items} → {path_or_err}")
                else:
                    logger.error(f"  ✗ Item {item_num}/{total_items} upload failed: {path_or_err[:120]}")

            # ── MARK FINAL STATUS ───────────────────────────────────
            if uploaded_count > 0:
                sm.mark_completed(url, last_upload_path)
                success_count += 1
                logger.info(f"✓ Completed [{url[:60]}]: {uploaded_count}/{total_items} uploaded")
            else:
                sm.mark_failed(url, "All uploads failed", permanent=True)
                logger.error(f"✗ All uploads failed: {url[:60]}")

            if sm.count_pending() > 0:
                inter_download_delay()

    except Exception as e:
        logger.exception(f"Worker crash: {e}")
        sm.set_worker_status("idle")
        raise

    sm.set_worker_status("idle")
    logger.info(f"Worker finished. Success: {success_count}/{processed_count} attempted.")
    return {"status": "completed", "processed": success_count, "attempted": processed_count}
