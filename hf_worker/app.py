import os
import logging
import threading
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv

load_dotenv()

os.environ.setdefault("DATA_DIR", "/app/data")
Path("/app/data/tmp").mkdir(parents=True, exist_ok=True)
Path("/app/logs").mkdir(parents=True, exist_ok=True)

import sys
sys.path.insert(0, "/app")

from core.worker_engine import run_worker
from core.state_manager import StateManager

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/app/logs/worker.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

logger.info("=" * 55)
logger.info("  Media Archiver Worker v1.3 — Starting")
logger.info(f"  COOKIES set:     {'yes' if os.environ.get('INSTAGRAM_COOKIES') else 'no'}")
logger.info(f"  SESSION_B64 set: {'yes' if os.environ.get('INSTAGRAM_SESSION_B64') else 'no'}")
logger.info("=" * 55)

app           = FastAPI(title="Media Archiver Worker", version="1.3")
WORKER_SECRET = os.environ.get("WORKER_SECRET", "")
_worker_lock  = threading.Lock()


def _verify(request: Request):
    if not WORKER_SECRET:
        raise HTTPException(status_code=500, detail="WORKER_SECRET not configured")
    if request.headers.get("X-Worker-Secret", "") != WORKER_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized")


@app.get("/")
async def root():
    sm = StateManager()
    return {
        "service": "Media Archiver Worker",
        "version": "1.3",
        "worker_status": sm.get_worker_status()["status"],
        "queue": sm.get_queue_summary(),
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
    """Legacy endpoint — kept for Telegram bot compatibility."""
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
    keep_days = int(body.get("keep_days", 30))
    sm        = StateManager()
    purged    = sm.purge_old_completed(keep_days=keep_days)
    return {"purged": purged, "queue": sm.get_queue_summary()}
