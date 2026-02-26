import os
import logging

from core.state_manager import StateManager
from core.retry_controller import is_retryable, should_pause, backoff_delay, inter_download_delay
from modules.downloader import download, download_all
from modules.dropbox_uploader import upload_file
from modules.platform_detector import detect_platform, guess_media_type

logger = logging.getLogger(__name__)

MAX_RETRIES            = int(os.environ.get("MAX_RETRIES", 3))
QUEUE_THRESHOLD        = int(os.environ.get("QUEUE_TRIGGER_THRESHOLD", 3))
CONSECUTIVE_FAIL_LIMIT = 10

_PERMANENT_ERRORS = [
    "unsupported url", "cannot parse data",
    "playlist returned no entries", "private video", "login required",
    "not available", "has been removed", "page not found", "404",
    "content is not available", "this reel can't be played",
    "video unavailable", "does not exist",
]


def _is_permanent(error: str) -> bool:
    return any(s in error.lower() for s in _PERMANENT_ERRORS)


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

            # ── DOWNLOAD ALL ITEMS (handles single/carousel/mixed) ──
            results = download_all(url, platform)

            # ── ALL ITEMS FAILED ────────────────────────────────────
            if not any(r.success for r in results):
                error = results[0].error if results else "Unknown error"
                logger.error(f"All downloads failed [{url[:60]}]: {error[:120]}")

                if _is_permanent(error):
                    sm.mark_failed(url, error, permanent=True)
                    consecutive_failures = 0
                    continue

                if should_pause(error):
                    sm.mark_failed(url, error)
                    reason = f"Platform blocking: {error}"
                    sm.set_worker_status("paused", reason=reason)
                    logger.critical(f"Worker auto-paused: {reason}")
                    return {"status": "paused", "reason": reason}

                consecutive_failures += 1
                if consecutive_failures >= CONSECUTIVE_FAIL_LIMIT:
                    sm.mark_failed(url, error)
                    sm.set_worker_status("paused", reason="consecutive_failure_limit")
                    return {"status": "paused", "reason": "consecutive_failure_limit"}

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
                    logger.warning(f"  Skipping item {item_num}/{total_items} (download failed): {result.error[:80]}")
                    continue

                # Determine media type
                actual_media_type = result.media_type or guess_media_type(url)

                logger.info(f"  Uploading item {item_num}/{total_items} [{actual_media_type}]...")

                # Upload to Dropbox
                success, path_or_err = upload_file(result.file_path, platform, actual_media_type)

                # Always clean up temp file after upload attempt
                try:
                    if result.file_path and os.path.exists(result.file_path):
                        os.remove(result.file_path)
                        logger.debug(f"  Temp file deleted: {result.file_path}")
                except Exception as e:
                    logger.warning(f"  Could not delete temp file: {e}")

                if success:
                    last_upload_path = path_or_err
                    uploaded_count  += 1
                    logger.info(f"  ✓ Item {item_num}/{total_items} uploaded: {path_or_err}")
                else:
                    logger.error(f"  ✗ Item {item_num}/{total_items} upload failed: {path_or_err[:120]}")

            # ── MARK FINAL STATUS ───────────────────────────────────
            if uploaded_count > 0:
                sm.mark_completed(url, last_upload_path)
                logger.info(
                    f"Completed [{url[:60]}]: "
                    f"{uploaded_count}/{total_items} item(s) uploaded"
                )
            else:
                sm.mark_failed(url, "All uploads failed", permanent=True)
                logger.error(f"All uploads failed for: {url[:60]}")

            processed_count += 1

            # Delay between different posts (not between items of same post)
            if sm.count_pending() > 0:
                inter_download_delay()

    except Exception as e:
        logger.exception(f"Worker crash: {e}")
        sm.set_worker_status("idle")
        raise

    sm.set_worker_status("idle")
    logger.info(f"Worker finished. Processed {processed_count}/{len(pending)} link(s).")
    return {"status": "completed", "processed": processed_count}
