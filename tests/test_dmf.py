"""Tests for the DMF (tape) layer.

Nothing here touches the real ``dmls``/``dmget``; see ``conftest.FakeArchive``.
"""

import pytest

from gfdl_utils import dmf


# ---------------------------------------------------------------------------
# batching
# ---------------------------------------------------------------------------


def test_batched_respects_count():
    paths = ["/a/{0}".format(i) for i in range(1001)]
    batches = list(dmf.batched(paths, size=500))
    assert [len(b) for b in batches] == [500, 500, 1]
    assert [p for b in batches for p in b] == paths


def test_batched_respects_command_line_length():
    paths = ["/" + "x" * 100 for _ in range(10)]
    batches = list(dmf.batched(paths, size=500, max_bytes=310))
    assert all(len(b) <= 3 for b in batches)
    assert sum(len(b) for b in batches) == 10


def test_batched_never_yields_empty():
    assert list(dmf.batched([])) == []
    assert list(dmf.batched(["/a"], size=1)) == [["/a"]]


def test_batched_rejects_zero_size():
    with pytest.raises(ValueError):
        list(dmf.batched(["/a"], size=0))


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------


def test_status_parsing_and_sizes(archive):
    a = archive.add("/archive/a.nc", "DUL", 1024 ** 3)
    b = archive.add("/archive/b.nc", "OFL", 2 * 1024 ** 3)
    report = dmf.status_report([a, b])
    assert report[a].state == "DUL" and report[a].online
    assert report[b].state == "OFL" and report[b].offline
    assert report.nbytes() == 3 * 1024 ** 3
    assert report.nbytes_offline == 2 * 1024 ** 3
    assert report.counts() == {"DUL": 1, "OFL": 1}


def test_paths_with_spaces_are_parsed(archive):
    path = archive.add("/archive/some dir/a file.nc", "OFL", 10)
    report = dmf.status_report([path])
    assert report.offline == [path]


def test_states_are_classified():
    for state in ("REG", "DUL", "MIG", "NMG"):
        assert dmf.FileStatus("/p", state).online
    assert dmf.FileStatus("/p", "UNM").inflight
    assert dmf.FileStatus("/p", "UNM").offline
    assert dmf.FileStatus("/p", "OFL").offline
    assert dmf.FileStatus("/p", "PAR").suspect
    assert dmf.FileStatus("/p", dmf.MISSING).missing
    # a missing file is neither online nor "offline waiting for tape"
    assert not dmf.FileStatus("/p", dmf.MISSING).offline


def test_missing_paths_are_reported_not_dropped(archive):
    good = archive.add("/archive/a.nc", "DUL", 1)
    report = dmf.status_report([good, "/archive/nope.nc"])
    assert report.missing == ["/archive/nope.nc"]
    assert not report.complete


def test_query_all_ondisk_is_false_for_missing_files(archive):
    # Regression: `all([])` on empty dmls output used to report success.
    assert dmf.query_all_ondisk(["/archive/nope.nc"]) is False
    assert dmf.query_all_ondisk([]) is False
    archive.add("/archive/a.nc", "DUL", 1)
    assert dmf.query_all_ondisk(["/archive/a.nc"]) is True


def test_query_ondisk_accepts_a_list_and_batches(archive):
    paths = [archive.add("/archive/{0}.nc".format(i), "DUL", 1) for i in range(900)]
    result = dmf.query_ondisk(paths)
    assert result == {p: True for p in paths}
    # 900 files must not cost 900 subprocesses
    assert len(archive.dmls_calls) == 2


def test_status_report_normalises_redundant_separators(archive):
    archive.add("/archive/a.nc", "DUL", 1)
    report = dmf.status_report(["/archive//a.nc"])
    assert report["/archive//a.nc"].online


# ---------------------------------------------------------------------------
# formatting
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("nbytes,expected", [
    (0, "0 B"),
    (2048, "2.00 kB"),
    (int(1.17 * 1024 ** 4), "1.17 TB"),
])
def test_format_bytes(nbytes, expected):
    assert dmf.format_bytes(nbytes) == expected


def test_summary_mentions_counts_and_size(archive):
    for i in range(3):
        archive.add("/archive/{0}.nc".format(i), "OFL", 1024 ** 3)
    archive.add("/archive/on.nc", "DUL", 1024 ** 3)
    summary = dmf.status_report(
        ["/archive/{0}.nc".format(i) for i in range(3)] + ["/archive/on.nc"]
    ).summary()
    assert "3 OFL" in summary and "1 DUL" in summary and "3.00 GB" in summary


# ---------------------------------------------------------------------------
# recall
# ---------------------------------------------------------------------------


def test_issue_dmget_batches_and_reports_success(archive):
    paths = [archive.add("/archive/{0}.nc".format(i), "OFL", 1) for i in range(1200)]
    assert dmf.issue_dmget(paths) == 0
    assert [len(c) for c in archive.dmget_calls] == [500, 500, 200]


def test_issue_dmget_raises_on_failure(archive, monkeypatch):
    class Failing:
        returncode = 1
        stderr = type("S", (), {"read": staticmethod(lambda: "dmget: boom")})()

        def poll(self):
            return 1

    monkeypatch.setattr(dmf.subprocess, "Popen", lambda cmd, **kw: Failing())
    with pytest.raises(dmf.DMFError) as excinfo:
        dmf.issue_dmget(["/archive/a.nc"])
    assert "exit code 1" in str(excinfo.value)
    assert "dmget: boom" in str(excinfo.value)


def test_dmget_recalls_only_offline_files(archive):
    on = archive.add("/archive/on.nc", "DUL", 1)
    off = archive.add("/archive/off.nc", "OFL", 1)
    report = dmf.dmget([on, off], progress=False)
    assert archive.dmget_calls == [[off]]
    assert report.complete


def test_dmget_is_a_noop_when_everything_is_online(archive):
    paths = [archive.add("/archive/{0}.nc".format(i), "DUL", 1) for i in range(5)]
    dmf.dmget(paths, progress=False)
    assert archive.dmget_calls == []


def test_dmget_size_guard(archive):
    paths = [archive.add("/archive/{0}.nc".format(i), "OFL", 10 * 1024 ** 3)
             for i in range(20)]
    with pytest.raises(dmf.DMFError) as excinfo:
        dmf.dmget(paths, max_gb=100, progress=False)
    assert "200.00 GB" in str(excinfo.value)
    assert archive.dmget_calls == []
    # ... and force= gets past it
    dmf.dmget(paths, max_gb=100, force=True, progress=False)
    assert archive.dmget_calls


def test_dmget_raises_on_missing_files(archive):
    with pytest.raises(FileNotFoundError):
        dmf.dmget(["/archive/nope.nc"], progress=False)


def test_dmget_detects_a_lying_recall(archive):
    """dmget exiting 0 while files stay offline must not look like success."""
    archive.add("/archive/off.nc", "OFL", 1)
    archive.recall_state = "OFL"  # the recall silently does nothing
    with pytest.raises(dmf.DMFError) as excinfo:
        dmf.dmget(["/archive/off.nc"], progress=False)
    assert "still not on disk" in str(excinfo.value)


def test_wait_until_ondisk_returns_when_complete(archive):
    paths = [archive.add("/archive/{0}.nc".format(i), "DUL", 1) for i in range(3)]
    report = dmf.wait_until_ondisk(paths, progress=False)
    assert report.complete
    assert len(archive.dmls_calls) == 1


def test_wait_until_ondisk_times_out_with_state_detail(archive):
    archive.add("/archive/a.nc", "UNM", 1024 ** 3)
    with pytest.raises(TimeoutError) as excinfo:
        dmf.wait_until_ondisk(["/archive/a.nc"], dmget_timeout=0, progress=False)
    message = str(excinfo.value)
    assert "1 UNM" in message and "1.00 GB" in message


def test_wait_until_ondisk_gives_up_on_missing_files(archive):
    with pytest.raises(FileNotFoundError):
        dmf.wait_until_ondisk(["/archive/nope.nc"], progress=False)


def test_iter_online_yields_online_files_first(archive):
    on = [archive.add("/archive/on{0}.nc".format(i), "DUL", 1) for i in range(3)]
    off = archive.add("/archive/off.nc", "OFL", 1)
    yielded = list(dmf.iter_online(on + [off], poll=0.0, progress=False))
    assert yielded[:3] == on
    assert yielded[-1] == off


# ---------------------------------------------------------------------------
# the guard
# ---------------------------------------------------------------------------


def test_ensure_ondisk_raises_by_default(archive):
    on = archive.add("/archive/on.nc", "DUL", 1)
    off = archive.add("/archive/off.nc", "OFL", 3 * 1024 ** 3)
    with pytest.raises(dmf.OfflineDataError) as excinfo:
        dmf.ensure_ondisk([on, off])
    message = str(excinfo.value)
    assert "1 of 2 files are offline" in message
    assert "3.00 GB" in message
    assert "dmget=True" in message
    assert off in message
    assert archive.dmget_calls == []


def test_ensure_ondisk_dmget_recalls(archive):
    off = archive.add("/archive/off.nc", "OFL", 1)
    dmf.ensure_ondisk([off], dmget=True, progress=False)
    assert archive.dmget_calls == [[off]]


def test_ensure_ondisk_warn_and_ignore(archive):
    off = archive.add("/archive/off.nc", "OFL", 1)
    with pytest.warns(UserWarning):
        dmf.ensure_ondisk([off], on_offline="warn")
    assert dmf.ensure_ondisk([off], on_offline="ignore") is None
    assert archive.dmget_calls == []


def test_ensure_ondisk_rejects_unknown_policy(archive):
    archive.add("/archive/a.nc", "DUL", 1)
    with pytest.raises(ValueError):
        dmf.ensure_ondisk(["/archive/a.nc"], on_offline="recall-please")


def test_ensure_ondisk_is_a_noop_for_no_paths(archive):
    assert dmf.ensure_ondisk([]) is None
