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

# How long to pause the worker when Instagram rate limits us (seconds)
# Links are NOT lost — they stay in queue and retry after cooldown
RATE_LIMIT_PAUSE       = int(os.environ.get("RATE_LIMIT_COOLDOWN", 300))  # 5 min default

_PERMANENT_ERRORS = [
    "unsupported url", "cannot parse data",
    "playlist returned no entries",
    "has been removed", "page not found",
    "content is not available", "this reel can't be played",
    "video unavailable", "does not exist", "media not found",
    "no media", "private",
]


def _is_permanent(error: str) -> bool:
    err = error.lower()
    # Never treat rate limit errors as permanent
    if "rate_limited" in err or "rate limit" in err:
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

    pending = sm.get_pending_links()
    if not force and len(pending) < QUEUE_THRESHOLD:
        logger.info(f"Only {len(pending)} pending — below threshold {QUEUE_THRESHOLD}. Skipping.")
        return {"status": "below_threshold", "pending": len(pending)}

    sm.set_worker_status("processing")
    logger.info(f"Worker started. Processing {len(pending)} link(s).")

    consecutive_failures = 0
    processed_count      = 0

    try:
        for link_record in pending:
            url         = link_record["url"]
            retry_count = link_record["retry_count"]

            if retry_count >= MAX_RETRIES:
                sm.mark_failed(url, "Max retries exceeded", permanent=True)
                logger.warning(f"Skipping {url} — max retries reached.")
                continue

            logger.info(f"[{processed_count+1}/{len(pending)}] {url[:80]} (retry #{retry_count})")
            sm.mark_processing(url)

            platform = detect_platform(url)
            if platform == "private":
                sm.mark_failed(url, "Private content", permanent=True)
                continue
            if platform == "unknown":
                sm.mark_failed(url, "Unrecognized platform", permanent=True)
                continue

            # ── DOWNLOAD ALL ITEMS ──────────────────────────────────
            results = download_all(url, platform)

            # ── ALL ITEMS FAILED ────────────────────────────────────
            if not any(r.success for r in results):
                error        = results[0].error if results else "Unknown error"
                rate_limited = any(getattr(r, "rate_limited", False) for r in results)

                logger.error(f"All downloads failed [{url[:60]}]: {error[:120]}")

                # ── RATE LIMITED — pause worker, keep link in queue ─
                if rate_limited or _is_rate_limited(error):
                    # Put link back as pending so it retries after cooldown
                    sm.mark_failed(url, error, permanent=False)
                    logger.warning(
                        f"Instagram rate limit hit — pausing worker for {RATE_LIMIT_PAUSE}s. "
                        f"Links remain in queue and will retry automatically."
                    )
                    sm.set_worker_status("idle")  # set idle so next /run trigger works

                    # Sleep here — Render keeps the process alive during /run
                    logger.info(f"Sleeping {RATE_LIMIT_PAUSE}s before releasing worker...")
                    time.sleep(RATE_LIMIT_PAUSE)
                    logger.info("Cooldown complete — worker releasing.")

                    # Return so VM can re-trigger the worker fresh
                    return {
                        "status": "rate_limited_cooldown",
                        "message": f"Paused {RATE_LIMIT_PAUSE}s due to Instagram rate limit. Links preserved.",
                        "processed": processed_count
                    }

                # ── PERMANENT ERROR — mark and skip ────────────────
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
                    sm.mark_failed(url, error)
                    backoff_delay(retry_count)
                else:
                    sm.mark_failed(url, error, permanent=True)
                continue

            # ── UPLOAD EACH ITEM INCREMENTALLY ──────────────────────
            consecutive_failures = 0
            total_items          = len(results)
            success_items        = [r for r in results if r.success]
            last_upload_path     = None
            uploaded_count       = 0

            if total_items > 1:
                logger.info(f"Uploading {len(success_items)}/{total_items} item(s) from: {url[:60]}")

            for i, result in enumerate(results):
                item_num = i + 1

                if not result.success:
                    logger.warning(f"  Skipping item {item_num}/{total_items}: {result.error[:80]}")
                    continue

                actual_media_type = result.media_type or guess_media_type(url)
                logger.info(f"  Uploading item {item_num}/{total_items} [{actual_media_type}]...")

                success, path_or_err = upload_file(result.file_path, platform, actual_media_type)

                # Clean up temp file
                try:
                    if result.file_path and os.path.exists(result.file_path):
                        os.remove(result.file_path)
                except Exception as e:
                    logger.warning(f"  Could not delete temp file: {e}")

                if success:
                    last_upload_path = path_or_err
                    uploaded_count  += 1
                    logger.info(f"  ✓ Item {item_num}/{total_items} → {path_or_err}")
                else:
                    logger.error(f"  ✗ Item {item_num}/{total_items} upload failed: {path_or_err[:120]}")

            # ── MARK FINAL STATUS ───────────────────────────────────
            if uploaded_count > 0:
                sm.mark_completed(url, last_upload_path)
                logger.info(f"Completed [{url[:60]}]: {uploaded_count}/{total_items} item(s) uploaded")
            else:
                sm.mark_failed(url, "All uploads failed", permanent=True)
                logger.error(f"All uploads failed for: {url[:60]}")

            processed_count += 1

            # Polite delay between posts to avoid rate limits
            if sm.count_pending() > 0:
                inter_download_delay()

    except Exception as e:
        logger.exception(f"Worker crash: {e}")
        sm.set_worker_status("idle")
        raise

    sm.set_worker_status("idle")
    logger.info(f"Worker finished. Processed {processed_count}/{len(pending)} link(s).")
    return {"status": "completed", "processed": processed_count}
