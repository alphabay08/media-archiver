import json
import os
import time
from pathlib import Path
from typing import Optional

_DATA_DIR  = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent.parent / "data")))
STATE_FILE = _DATA_DIR / "state.json"

_DEFAULT_STATE = {
    "links": {},
    "stats": {
        "total_received": 0,
        "total_processed": 0,
        "total_success_download": 0,
        "total_success_upload": 0,
        "total_failed": 0,
        "total_retries": 0,
        "by_platform": {
            "instagram": {"received": 0, "success": 0, "failed": 0},
            "facebook":  {"received": 0, "success": 0, "failed": 0},
        },
    },
    "worker": {
        "status": "idle",
        "last_active": None,
        "paused_reason": None,
    },
}


class StateManager:
    def __init__(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not STATE_FILE.exists():
            self._write(json.loads(json.dumps(_DEFAULT_STATE)))
        else:
            self._recover_stuck_processing()

    def _read(self) -> dict:
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
            for k, v in _DEFAULT_STATE.items():
                if k not in data:
                    data[k] = json.loads(json.dumps(v))
            for plat in ("instagram", "facebook"):
                if plat not in data["stats"]["by_platform"]:
                    data["stats"]["by_platform"][plat] = {"received": 0, "success": 0, "failed": 0}
            return data
        except Exception:
            return json.loads(json.dumps(_DEFAULT_STATE))

    def _write(self, state: dict):
        tmp = str(STATE_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)

    def _recover_stuck_processing(self):
        state   = self._read()
        changed = False
        for link in state["links"].values():
            if link["status"] == "processing":
                link["status"]     = "pending"
                link["updated_at"] = time.time()
                link["error"]      = "Recovered from crashed worker"
                changed            = True
        if changed:
            self._write(state)

    def add_link(self, url: str, platform: str) -> bool:
        state = self._read()
        if url in state["links"]:
            return False
        state["links"][url] = {
            "url": url, "status": "pending", "platform": platform,
            "added_at": time.time(), "updated_at": time.time(),
            "retry_count": 0, "error": None, "dropbox_path": None,
        }
        state["stats"]["total_received"] += 1
        plat = platform if platform in ("instagram", "facebook") else "instagram"
        state["stats"]["by_platform"][plat]["received"] += 1
        self._write(state)
        return True

    def bulk_add_links(self, links: list) -> int:
        state = self._read()
        added = 0
        now   = time.time()
        for item in links:
            url      = item.get("url", "").strip()
            platform = item.get("platform", "unknown")
            if not url or url in state["links"]:
                continue
            state["links"][url] = {
                "url": url, "status": "pending", "platform": platform,
                "added_at": now, "updated_at": now,
                "retry_count": 0, "error": None, "dropbox_path": None,
            }
            state["stats"]["total_received"] += 1
            plat = platform if platform in ("instagram", "facebook") else "instagram"
            state["stats"]["by_platform"][plat]["received"] += 1
            added += 1
        if added:
            self._write(state)
        return added

    def get_pending_links(self) -> list:
        state = self._read()
        return sorted(
            [v for v in state["links"].values() if v["status"] == "pending"],
            key=lambda x: x["added_at"]
        )

    def count_pending(self) -> int:
        return len(self.get_pending_links())

    def mark_processing(self, url: str):
        self._update_link(url, {"status": "processing", "updated_at": time.time()})

    def mark_completed(self, url: str, dropbox_path: str):
        state = self._read()
        link  = state["links"].get(url)
        if not link:
            return
        link.update({"status": "completed", "dropbox_path": dropbox_path,
                     "updated_at": time.time(), "error": None})
        state["stats"]["total_processed"]        += 1
        state["stats"]["total_success_download"] += 1
        state["stats"]["total_success_upload"]   += 1
        plat = link["platform"] if link["platform"] in ("instagram", "facebook") else "instagram"
        state["stats"]["by_platform"][plat]["success"] += 1
        self._write(state)

    def mark_failed(self, url: str, error: str, permanent: bool = False):
        state       = self._read()
        link        = state["links"].get(url)
        if not link:
            return
        link["error"]      = error
        link["updated_at"] = time.time()
        max_retries        = int(os.environ.get("MAX_RETRIES", 3))
        if permanent or link["retry_count"] >= max_retries:
            link["status"]      = "failed"
            link["retry_count"] = min(link["retry_count"] + 1, max_retries + 1)
            state["stats"]["total_failed"]    += 1
            state["stats"]["total_processed"] += 1
            plat = link["platform"] if link["platform"] in ("instagram", "facebook") else "instagram"
            state["stats"]["by_platform"][plat]["failed"] += 1
        else:
            link["retry_count"] += 1
            link["status"]       = "pending"
            state["stats"]["total_retries"] += 1
        self._write(state)

    def _update_link(self, url: str, fields: dict):
        state = self._read()
        if url in state["links"]:
            state["links"][url].update(fields)
            self._write(state)

    def purge_old_completed(self, keep_days: int = 30) -> int:
        state   = self._read()
        cutoff  = time.time() - (keep_days * 86400)
        to_del  = [
            url for url, l in state["links"].items()
            if l["status"] == "completed" and l["updated_at"] < cutoff
        ]
        for url in to_del:
            del state["links"][url]
        if to_del:
            self._write(state)
        return len(to_del)

    def set_worker_status(self, status: str, reason: Optional[str] = None):
        state = self._read()
        state["worker"]["status"]        = status
        state["worker"]["last_active"]   = time.time()
        state["worker"]["paused_reason"] = reason
        self._write(state)

    def get_worker_status(self) -> dict:
        return self._read()["worker"]

    def get_stats(self) -> dict:
        return self._read()["stats"]

    def get_queue_summary(self) -> dict:
        state = self._read()
        links = list(state["links"].values())
        return {
            "pending":    sum(1 for l in links if l["status"] == "pending"),
            "processing": sum(1 for l in links if l["status"] == "processing"),
            "completed":  sum(1 for l in links if l["status"] == "completed"),
            "failed":     sum(1 for l in links if l["status"] == "failed"),
            "total":      len(links),
        }

    def get_link_status(self, url: str):
        return self._read()["links"].get(url)
