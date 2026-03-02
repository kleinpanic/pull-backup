# tests/test_core_deep.py
#
# Deeper unit coverage for pull-backup without actually invoking ssh/rsync/tar.
# Uses monkeypatch to simulate subprocess behavior and remote conditions.
#
# Run: pytest -q

import json
import os
import sys
import types
import datetime as dt
from functools import lru_cache
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "pull-backup"


@lru_cache(maxsize=1)
def load_module():
    """
    Load the extensionless executable 'pull-backup' as a Python module.
    Uses SourceFileLoader to be robust across Python versions / extensionless paths.
    """
    assert SCRIPT_PATH.exists(), f"missing script at {SCRIPT_PATH}"
    assert SCRIPT_PATH.is_file(), f"not a file: {SCRIPT_PATH}"

    name = "pull_backup"
    loader = SourceFileLoader(name, str(SCRIPT_PATH))
    spec = spec_from_loader(name, loader, origin=str(SCRIPT_PATH))
    assert spec and spec.loader, "failed to build import spec via SourceFileLoader"

    mod = module_from_spec(spec)
    sys.modules[name] = mod
    try:
        loader.exec_module(mod)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return mod


@pytest.fixture(scope="session")
def pb():
    # "pb" = pull-backup module
    return load_module()


def _args(**kw):
    """
    Minimal args namespace required by run_one_job and friends.
    Defaults are intentionally "safe" and deterministic for tests.
    """
    d = dict(
        deep_verify=False,
        strict_rsync=False,
        attempts=2,
        retry_sleep=0,
        ssh_probe_timeout=1,
        rsync_timeout=0,
        rsync_stop_after_mins=0,
        bwlimit=0,
        no_compress=True,
        wallclock_timeout_s=0,
        kill_grace_s=0,
        max_load=0.0,
        load_poll=1,
        progress_interval_s=0,   # disable progress logging loop in tests
        stall_timeout_s=0,       # disable stall watchdog in tests
        du_timeout_s=1,
        no_progress=True,
        auto=True,
        log_days=9999,
        auto_ignore_perms=True,
        auto_ignore_max=200,
        notify=False,
        exit_soft_nonzero=False,
    )
    d.update(kw)
    return types.SimpleNamespace(**d)


def _job(pb, **kw):
    Job = pb.Job
    base = dict(
        device="klein",
        ssh="klein",
        path="/home/klein/",
        when="daily",
        description="",
        klass="laptops",
        keep=3,
        action="quarantine",
        exclude=(),
        exclude_file="",
        stall_timeout_s=0,
        progress_interval_s=0,
        du_timeout_s=0,
        self_backup=False,
    )
    base.update(kw)
    return Job(**base)


def _mk_existing_snapshot(snap_dir: Path, tag: str):
    p = snap_dir / tag
    p.mkdir(parents=True, exist_ok=True)
    (p / "dummy.txt").write_text("x", encoding="utf-8")
    return p


def test_when_matches_today_variants(pb):
    assert pb.when_matches_today("daily") is True
    assert pb.when_matches_today("*") is True
    assert pb.when_matches_today("") is True
    iso, name = pb.iso_wd_and_name_local()
    assert pb.when_matches_today(iso) is True
    assert pb.when_matches_today(name) is True
    assert pb.when_matches_today(f"{iso},Wed,Thu") is True


def test_parse_jobs_excludes_normalizes_abs_under_job_path(pb):
    cfg = {
        "job": [
            {
                "device": "klein",
                "ssh": "klein",
                "path": "/home/klein/",
                "exclude": [
                    "/home/klein/.cache/",
                    ".local/share/Trash/**",
                ],
            }
        ]
    }
    defaults = {"class": "laptops", "keep": 3, "action": "quarantine"}
    jobs = pb.parse_jobs(cfg, defaults)
    assert len(jobs) == 1
    j = jobs[0]
    assert "/.cache/" in j.exclude
    assert ".local/share/Trash/**" in j.exclude


def test_parse_jobs_excludes_rejects_traversal(pb):
    cfg = {
        "job": [
            {
                "device": "klein",
                "ssh": "klein",
                "path": "/home/klein/",
                "exclude": ["../nope"],
            }
        ]
    }
    defaults = {"class": "laptops", "keep": 3, "action": "quarantine"}
    with pytest.raises(RuntimeError):
        pb.parse_jobs(cfg, defaults)


def test_make_runtime_exclude_from_perm_denied(pb):
    pat = pb._make_runtime_exclude_for_denied_path("/home/klein/.ssh/id_rsa", "/home/klein/")
    assert pat == "/.ssh/id_rsa"

    pat2 = pb._make_runtime_exclude_for_denied_path("/etc/shadow", "/home/klein/")
    assert pat2 is None

    pat3 = pb._make_runtime_exclude_for_denied_path(".config/secrets", "/home/klein/")
    assert pat3 == "/.config/secrets"


def test_perm_denied_regex_extracts_path(pb):
    line = 'rsync: [sender] send_files failed to open "/home/klein/.ssh/id_rsa": Permission denied (13)'
    assert pb._extract_perm_denied_path(line) == "/home/klein/.ssh/id_rsa"


def test_is_other_error_line_distinguishes_perm_denied(pb):
    perm = 'rsync: [sender] opendir "/home/klein/secret": Permission denied (13)'
    other = "rsync error: some other failure (code 12) at main.c(123) [sender=3.2.7]"
    assert pb._is_other_error_line(perm) is False
    assert pb._is_other_error_line(other) is True


def test_autoexclude_save_and_cap(pb, tmp_path):
    state_dir = tmp_path / "state"
    added, total = pb.save_autoexclude_patterns(state_dir, "klein", [f"/a{i}" for i in range(10)], max_total=5)
    assert added == 5
    assert total == 5

    added2, total2 = pb.save_autoexclude_patterns(state_dir, "klein", ["/a1", "/a9", "/b"], max_total=5)
    assert added2 == 0
    assert total2 == 5

    pats = pb.load_autoexclude_patterns(state_dir, "klein")
    assert len(pats) == 5


def test_ledger_last_success_map(pb, tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    lp = state_dir / "runs.jsonl"
    recs = [
        {"type": "job", "device": "klein", "outcome": "failed", "ts_end": "2026-01-01T00:00:00Z"},
        {"type": "job", "device": "klein", "outcome": "success", "ts_end": "2026-01-02T00:00:00Z"},
        {"type": "job", "device": "klein", "outcome": "success", "ts_end": "2026-01-03T00:00:00Z"},
        {"type": "job", "device": "mt", "outcome": "success", "ts_end": "2026-01-04T00:00:00Z"},
        {"type": "archive", "device": "klein", "outcome": "success", "ts": "2026-01-05T00:00:00Z"},
    ]
    with open(lp, "w", encoding="utf-8") as fp:
        for r in recs:
            fp.write(json.dumps(r) + "\n")

    mp = pb.read_last_success_map(state_dir)
    assert "klein" in mp and "mt" in mp
    assert mp["klein"].isoformat().startswith("2026-01-03")
    assert mp["mt"].isoformat().startswith("2026-01-04")


def test_retention_legacy_moves_old(pb, tmp_path):
    backups_root = tmp_path / "backups"
    snap_dir = backups_root / "laptops" / "klein" / "snapshots"
    legacy_root = backups_root / "legacy" / "laptops" / "klein"
    snap_dir.mkdir(parents=True)

    class _Log:
        def __init__(self):
            self.lines = []
        def state(self, s, msg):
            self.lines.append((s, msg))

    _mk_existing_snapshot(snap_dir, "20260101-000000Z")
    _mk_existing_snapshot(snap_dir, "20260102-000000Z")
    _mk_existing_snapshot(snap_dir, "20260103-000000Z")
    _mk_existing_snapshot(snap_dir, "20260104-000000Z")

    log = _Log()
    pb.retention(snap_dir, legacy_root, keep=2, action="legacy", log=log)

    assert (legacy_root / "20260101-000000Z").exists()
    assert (legacy_root / "20260102-000000Z").exists()
    assert (snap_dir / "20260103-000000Z").exists()
    assert (snap_dir / "20260104-000000Z").exists()


def test_collapse_summaries_preserves_worst_and_sums_attempts(pb):
    JobSummary = pb.JobSummary
    Outcome = pb.Outcome
    j = _job(pb)

    now = dt.datetime.now(dt.timezone.utc)
    s1 = JobSummary(j, Outcome.SUCCESS, "success", "tag", now, now, 1, final_rc=0)
    s2 = JobSummary(j, Outcome.DEFERRED, "ssh_or_rsync_unreachable", "tag", now, now, 2, final_rc=None)

    collapsed = pb.collapse_summaries_by_device([s1, s2])
    assert len(collapsed) == 1
    assert collapsed[0].outcome == Outcome.DEFERRED
    assert collapsed[0].attempts == 3


def test_run_one_job_success_finalizes_and_sets_current(pb, tmp_path, monkeypatch):
    backups_root = tmp_path / "backups"
    state_dir = tmp_path / "state"
    backups_root.mkdir()
    state_dir.mkdir()

    job = _job(pb, device="klein", ssh="klein", path="/home/klein/")
    snap_dir = backups_root / job.klass / job.device / "snapshots"
    snap_dir.mkdir(parents=True)
    _mk_existing_snapshot(snap_dir, "20260101-000000Z")

    monkeypatch.setattr(pb, "rsync_ready_probe", lambda host, cfg, timeout_s=0: (True, "RSYNC_OK\n"))
    monkeypatch.setattr(pb, "utc_tag", lambda: "20260114-000000Z")

    def fake_run_and_tee(cmd, log, **kwargs):
        assert any(str(x).startswith("--link-dest=") for x in cmd)
        return 0, None, "exit", 0

    monkeypatch.setattr(pb, "run_and_tee", fake_run_and_tee)
    monkeypatch.setattr(pb, "du_bytes", lambda p, timeout_s=0: (123, "ok"))

    args = _args(no_progress=True, progress_interval_s=0, stall_timeout_s=0)
    s = pb.run_one_job(job, backups_root=backups_root, state_dir=state_dir, ssh_config=None, args=args)

    assert s.outcome == pb.Outcome.SUCCESS
    final = snap_dir / "20260114-000000Z"
    assert final.exists() and final.is_dir()
    assert not (final / ".run-tag").exists()
    cur = snap_dir / "current"
    assert cur.is_symlink()
    assert cur.resolve() == final.resolve()

    lp = state_dir / "runs.jsonl"
    assert lp.exists()
    lines = lp.read_text(encoding="utf-8").splitlines()
    assert any('"outcome": "success"' in ln for ln in lines)


def test_run_one_job_attempts_exhausted_transient_leaves_inprogress(pb, tmp_path, monkeypatch):
    backups_root = tmp_path / "backups"
    state_dir = tmp_path / "state"
    backups_root.mkdir()
    state_dir.mkdir()

    job = _job(pb, device="klein", ssh="klein", path="/home/klein/")
    monkeypatch.setattr(pb, "rsync_ready_probe", lambda host, cfg, timeout_s=0: (True, "RSYNC_OK\n"))
    monkeypatch.setattr(pb, "utc_tag", lambda: "20260114-000001Z")

    calls = {"n": 0}
    def fake_run_and_tee(cmd, log, **kwargs):
        calls["n"] += 1
        return 255, None, "exit", 0

    monkeypatch.setattr(pb, "run_and_tee", fake_run_and_tee)

    args = _args(attempts=2, retry_sleep=0)
    s = pb.run_one_job(job, backups_root=backups_root, state_dir=state_dir, ssh_config=None, args=args)

    assert calls["n"] == 2
    assert s.outcome == pb.Outcome.FAILED
    assert "attempts_exhausted" in s.reason

    snap_dir = backups_root / job.klass / job.device / "snapshots"
    inprog = snap_dir / ".inprogress-klein"
    assert inprog.exists()
    assert not (snap_dir / "20260114-000001Z").exists()


def test_run_one_job_soft_success_rc23_finalizes_and_marks_failure(pb, tmp_path, monkeypatch):
    backups_root = tmp_path / "backups"
    state_dir = tmp_path / "state"
    backups_root.mkdir()
    state_dir.mkdir()

    job = _job(pb, device="klein", ssh="klein", path="/home/klein/")
    monkeypatch.setattr(pb, "rsync_ready_probe", lambda host, cfg, timeout_s=0: (True, "RSYNC_OK\n"))
    monkeypatch.setattr(pb, "utc_tag", lambda: "20260114-000002Z")

    monkeypatch.setattr(pb, "run_and_tee", lambda cmd, log, **kw: (23, None, "exit", 0))
    monkeypatch.setattr(pb, "du_bytes", lambda p, timeout_s=0: (123, "ok"))

    args = _args(strict_rsync=False)
    s = pb.run_one_job(job, backups_root=backups_root, state_dir=state_dir, ssh_config=None, args=args)

    assert s.outcome == pb.Outcome.SOFT_SUCCESS
    snap_dir = backups_root / job.klass / job.device / "snapshots"
    final = snap_dir / "20260114-000002Z"
    assert final.exists()

    assert (state_dir / "klein.failure").exists()


def test_run_one_job_autoignore_perms_rerun_reaches_zero(pb, tmp_path, monkeypatch):
    backups_root = tmp_path / "backups"
    state_dir = tmp_path / "state"
    backups_root.mkdir()
    state_dir.mkdir()

    job = _job(pb, device="klein", ssh="klein", path="/home/klein/")
    monkeypatch.setattr(pb, "rsync_ready_probe", lambda host, cfg, timeout_s=0: (True, "RSYNC_OK\n"))
    monkeypatch.setattr(pb, "utc_tag", lambda: "20260114-000003Z")
    monkeypatch.setattr(pb, "du_bytes", lambda p, timeout_s=0: (123, "ok"))

    seq = {"i": 0}
    def fake_run_and_tee(cmd, log, on_line=None, **kw):
        seq["i"] += 1
        if seq["i"] == 1:
            if on_line:
                on_line('rsync: [sender] send_files failed to open "/home/klein/.ssh/id_rsa": Permission denied (13)')
                on_line('rsync: [sender] opendir "/home/klein/.gnupg": Permission denied (13)')
            return 23, None, "exit", 0
        if seq["i"] == 2:
            return 0, None, "exit", 0
        raise AssertionError("unexpected extra rsync call")

    monkeypatch.setattr(pb, "run_and_tee", fake_run_and_tee)

    args = _args(strict_rsync=False, auto_ignore_perms=True)
    s = pb.run_one_job(job, backups_root=backups_root, state_dir=state_dir, ssh_config=None, args=args)

    assert s.outcome == pb.Outcome.SUCCESS
    assert seq["i"] == 2

    autoex = state_dir / "klein.autoexclude"
    assert autoex.exists()
    txt = autoex.read_text(encoding="utf-8")
    assert "/.ssh/id_rsa" in txt
    assert "/.gnupg" in txt

    lp = state_dir / "runs.jsonl"
    lines = lp.read_text(encoding="utf-8").splitlines()
    recs = [json.loads(ln) for ln in lines if ln.strip().startswith("{")]
    last_job = [r for r in recs if r.get("type") == "job" and r.get("tag") == "20260114-000003Z"][-1]
    assert last_job.get("autoignore_reran") is True


def test_run_one_job_unreachable_deferred_cleans_new_inprogress(pb, tmp_path, monkeypatch):
    backups_root = tmp_path / "backups"
    state_dir = tmp_path / "state"
    backups_root.mkdir()
    state_dir.mkdir()

    job = _job(pb, device="klein", ssh="klein", path="/home/klein/")
    monkeypatch.setattr(pb, "utc_tag", lambda: "20260114-000004Z")

    monkeypatch.setattr(pb, "rsync_ready_probe", lambda host, cfg, timeout_s=0: (False, "nope\n"))
    monkeypatch.setattr(
        pb,
        "run_and_tee",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not run rsync")),
    )

    args = _args()
    s = pb.run_one_job(job, backups_root=backups_root, state_dir=state_dir, ssh_config=None, args=args)

    assert s.outcome == pb.Outcome.DEFERRED
    snap_dir = backups_root / job.klass / job.device / "snapshots"
    inprog = snap_dir / ".inprogress-klein"
    assert not inprog.exists()


def test_collect_job_meta_and_priority_ordering(pb, tmp_path):
    backups_root = tmp_path / "backups"
    state_dir = tmp_path / "state"
    backups_root.mkdir()
    state_dir.mkdir()

    j1 = _job(pb, device="a", ssh="a", path="/home/a/")
    j2 = _job(pb, device="b", ssh="b", path="/home/b/")

    snap_b = backups_root / j2.klass / j2.device / "snapshots"
    snap_b.mkdir(parents=True)
    inprog = snap_b / ".inprogress-b"
    inprog.mkdir()
    (inprog / "some_payload").write_text("x", encoding="utf-8")

    last_success_map = {
        "a": dt.datetime(2026, 1, 13, tzinfo=dt.timezone.utc),
        "b": dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
    }

    ma = pb.collect_job_meta(backups_root, state_dir, j1, last_success_map)
    mb = pb.collect_job_meta(backups_root, state_dir, j2, last_success_map)

    assert ma.resumable is False
    assert mb.resumable is True

    assert pb.priority_tuple(j2, mb) < pb.priority_tuple(j1, ma)

