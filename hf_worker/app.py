import os
import time
import logging
import logging.handlers
import threading
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv

load_dotenv()

os.environ.setdefault("DATA_DIR", "/app/data")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
TEMP_DIR = DATA_DIR / "tmp"
LOG_DIR  = Path("/app/logs")

DATA_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

import sys
sys.path.insert(0, "/app")

from core.worker_engine import run_worker
from core.state_manager import StateManager

# ── LOG ROTATION ───────────────────────────────────────────────────
# 5MB per file × 3 backups = 15MB max log storage on Render
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            filename    = str(LOG_DIR / "worker.log"),
            maxBytes    = 5 * 1024 * 1024,  # 5MB per file
            backupCount = 3,                 # 3 backups = 15MB max total
            encoding    = "utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

logger.info("=" * 55)
logger.info("  Media Archiver Worker v1.3 — Starting")
logger.info(f"  SESSION_B64 set: {'yes' if os.environ.get('INSTAGRAM_SESSION_B64') else 'no'}")
logger.info(f"  Log rotation:    5MB x 3 = 15MB max")
logger.info("=" * 55)

app           = FastAPI(title="Media Archiver Worker", version="1.3")
WORKER_SECRET = os.environ.get("WORKER_SECRET", "")
_worker_lock  = threading.Lock()

# Cleanup config
TEMP_MAX_AGE      = int(os.environ.get("TEMP_MAX_AGE_SECONDS",  1800))   # 30 min
TEMP_CLEAN_EVERY  = int(os.environ.get("TEMP_CLEANUP_INTERVAL", 3600))   # 1 hour
STATE_PURGE_DAYS  = int(os.environ.get("STATE_PURGE_DAYS",      30))     # 30 days
STATE_PURGE_EVERY = int(os.environ.get("STATE_PURGE_INTERVAL",  86400))  # daily


def _verify(request: Request):
    if not WORKER_SECRET:
        raise HTTPException(status_code=500, detail="WORKER_SECRET not configured")
    if request.headers.get("X-Worker-Secret", "") != WORKER_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")


# ─────────────────────────────────────────────
# RESOURCE CLEANUP
# ─────────────────────────────────────────────

def cleanup_orphan_temps():
    """Delete temp files older than TEMP_MAX_AGE seconds."""
    try:
        now = time.time()
        deleted = freed = 0
        for f in TEMP_DIR.iterdir():
            if not f.is_file():
                continue
            if (now - f.stat().st_mtime) > TEMP_MAX_AGE:
                freed += f.stat().st_size
                f.unlink()
                deleted += 1
        if deleted:
            logger.info(f"Temp cleanup: {deleted} file(s) deleted, {freed/(1024*1024):.1f}MB freed")
    except Exception as e:
        logger.warning(f"Temp cleanup error: {e}")


def purge_old_state():
    """Remove completed links older than STATE_PURGE_DAYS from state.json."""
    try:
        sm     = StateManager()
        purged = sm.purge_old_completed(keep_days=STATE_PURGE_DAYS)
        if purged:
            logger.info(f"State purge: {purged} completed link(s) older than {STATE_PURGE_DAYS}d removed")
    except Exception as e:
        logger.warning(f"State purge error: {e}")


def background_cleanup():
    """Temp cleanup every hour + state purge every day."""
    logger.info(f"Cleanup worker: temp every {TEMP_CLEAN_EVERY//60}min, state every {STATE_PURGE_EVERY//3600}h")
    last_state_purge = time.time()
    while True:
        time.sleep(TEMP_CLEAN_EVERY)
        cleanup_orphan_temps()
        if (time.time() - last_state_purge) >= STATE_PURGE_EVERY:
            purge_old_state()
            last_state_purge = time.time()


# Run cleanup on startup then start background thread
logger.info("Startup cleanup...")
cleanup_orphan_temps()
purge_old_state()
threading.Thread(target=background_cleanup, name="CleanupWorker", daemon=True).start()
logger.info("Background cleanup thread started.")


# ─────────────────────────────────────────────
# ORIGINAL V1 ENDPOINTS — UNCHANGED
# ─────────────────────────────────────────────

@app.get("/")
async def root():
    sm = StateManager()
    return {
        "service":       "Media Archiver Worker",
        "version":       "1.3",
        "worker_status": sm.get_worker_status()["status"],
        "queue":         sm.get_queue_summary(),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/run")
async def trigger_worker(request: Request):
    _verify(request)
    if not _worker_lock.acquire(blocking=False):
        return {"status": "already_running"}
    body  = {}
    try:
        body = await request.json()
    except Exception:
        pass
    force = body.get("force", True)

    def _run():
        try:
            result = run_worker(force=force)
            logger.info(f"Worker finished: {result}")
        except Exception as e:
            logger.exception(f"Worker crashed: {e}")
        finally:
            _worker_lock.release()

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "triggered"}


@app.get("/stats")
async def get_stats(request: Request):
    _verify(request)
    sm = StateManager()
    return {"stats": sm.get_stats(), "queue": sm.get_queue_summary(), "worker": sm.get_worker_status()}


@app.post("/reset-pause")
async def reset_pause(request: Request):
    _verify(request)
    sm     = StateManager()
    worker = sm.get_worker_status()
    if worker["status"] != "paused":
        return {"status": "not_paused", "current": worker["status"]}
    sm.set_worker_status("idle")
    return {"status": "resumed"}


@app.post("/add")
async def add_link(request: Request):
    _verify(request)
    body     = await request.json()
    url      = body.get("url", "").strip()
    platform = body.get("platform", "unknown")
    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    sm    = StateManager()
    added = sm.add_link(url, platform)
    return {"added": added, "queue": sm.get_queue_summary()}


@app.post("/add-link")
async def add_link_legacy(request: Request):
    return await add_link(request)


@app.post("/bulk-add")
async def bulk_add(request: Request):
    _verify(request)
    body  = await request.json()
    links = body.get("links", [])
    if not links:
        raise HTTPException(status_code=400, detail="links array is required")
    sm    = StateManager()
    added = sm.bulk_add_links(links)
    return {"added": added, "total_submitted": len(links), "queue": sm.get_queue_summary()}


@app.get("/link-status")
async def link_status(request: Request, url: str):
    _verify(request)
    sm     = StateManager()
    record = sm.get_link_status(url)
    return {"found": record is not None, "record": record}


@app.post("/purge-completed")
async def purge_completed(request: Request):
    _verify(request)
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    keep_days = int(body.get("keep_days", STATE_PURGE_DAYS))
    sm        = StateManager()
    purged    = sm.purge_old_completed(keep_days=keep_days)
    return {"purged": purged, "queue": sm.get_queue_summary()}


@app.get("/disk-usage")
async def disk_usage(request: Request):
    """Check how much disk space logs, temp, and state are using."""
    _verify(request)
    def dir_size(p: Path) -> int:
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0
    return {
        "logs_mb": round(dir_size(LOG_DIR)  / (1024*1024), 2),
        "temp_mb": round(dir_size(TEMP_DIR) / (1024*1024), 2),
        "data_mb": round(dir_size(DATA_DIR) / (1024*1024), 2),
    }
