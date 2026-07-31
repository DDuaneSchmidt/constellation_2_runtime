from datetime import UTC, datetime, timedelta
from pathlib import Path
import json

from constellation_2.common.atlas_v2_research_os.scheduler_locking import cleanup_stale_scheduler_lock, read_scheduler_lock, scheduler_lock, scheduler_lock_path


def test_scheduler_lock_allows_single_active_execution(tmp_path: Path):
    with scheduler_lock(tmp_path, owner_id="one", trigger_id="t1") as first:
        assert first.acquired is True
        with scheduler_lock(tmp_path, owner_id="two", trigger_id="t2") as second:
            assert second.acquired is False
            assert second.reason == "LOCK_CONFLICT"
    assert read_scheduler_lock(tmp_path) == {}


def test_stale_lock_cleanup_recovers_from_crash(tmp_path: Path):
    path = scheduler_lock_path(tmp_path)
    old = (datetime.now(UTC) - timedelta(hours=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    path.write_text(json.dumps({"lock_id": "stale", "created_at": old}) + "\n", encoding="utf-8")
    assert cleanup_stale_scheduler_lock(tmp_path, stale_after_seconds=1) is True
    assert not path.exists()
    with scheduler_lock(tmp_path, owner_id="new", trigger_id="t3", stale_after_seconds=1) as result:
        assert result.acquired is True


def test_fresh_lock_is_not_cleaned(tmp_path: Path):
    path = scheduler_lock_path(tmp_path)
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    path.write_text(json.dumps({"lock_id": "fresh", "created_at": now}) + "\n", encoding="utf-8")
    assert cleanup_stale_scheduler_lock(tmp_path, stale_after_seconds=3600) is False
    assert path.exists()
