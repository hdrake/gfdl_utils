"""Interaction with GFDL's DMF tape system (``dmls`` / ``dmget``).

The archive filesystem at GFDL is tape-backed by SGI DMF.  A file may be
resident on disk, resident only on tape, or somewhere in between; reading a
file that is not on disk blocks (for minutes to hours) while DMF silently
recalls it.  This module makes that visible and controllable.

Design goals
------------

**Batched.**  ``dmls`` and ``dmget`` both accept many paths per invocation.
Everything here spawns one process per ``BATCH_SIZE`` (default 500) paths, so
checking the status of 900 files costs 2 subprocesses, not 900.

**Honest.**  Commands run through :mod:`subprocess` with real return codes and
captured stderr.  A failed recall raises; it is never silently indistinguishable
from success (the old ``os.system("dmget ... &")`` always returned the exit
status of the backgrounding shell, i.e. 0).

**Informative.**  Status is a DMF state per file, not a boolean, plus the file
size, so callers can report "3 of 932 files still UNM (unmigrating), 1.17 TB
total" instead of "not ready yet".

**Guarded.**  :func:`dmget` refuses to recall more than ``max_gb`` (default
100 GB, matching GFDL's guidance to confirm before recalling >50 files or
>100 GB) unless explicitly overridden.

DMF states
----------

============  =========================================================
``REG``       regular file, resident on disk only (never migrated)
``DUL``       dual-state: identical copies on disk and tape -> readable
``MIG``       migrating to tape; data still on disk -> readable
``NMG``       not managed by DMF -> readable
``UNM``       unmigrating: a recall is in flight
``OFL``       offline: data is on tape only
``PAR``       partial-state: only part of the file is on disk
``INV``       invalid: DMF state is inconsistent
============  =========================================================

Typical use
-----------

    from gfdl_utils import dmf

    report = dmf.status_report(paths)
    print(report.summary())

    dmf.dmget(paths, max_gb=2000)          # blocking recall, with progress
    dmf.ensure_ondisk(paths)               # raise (loudly) if anything is offline

    for path in dmf.iter_online(paths):    # stream: yields files as they land
        process(path)
"""

from __future__ import annotations

import dataclasses
import getpass
import glob as _glob
import os
import re
import shutil
import subprocess
import time as _time
import warnings

__all__ = [
    "BATCH_SIZE",
    "MAX_CONCURRENT_DMGET",
    "ONLINE_STATES",
    "INFLIGHT_STATES",
    "OFFLINE_STATES",
    "SUSPECT_STATES",
    "MISSING",
    "available",
    "DMFError",
    "OfflineDataError",
    "FileStatus",
    "StatusReport",
    "format_bytes",
    "batched",
    "stat_paths",
    "status_report",
    "query_ondisk",
    "query_all_ondisk",
    "query_dmget",
    "offline_paths",
    "issue_dmget",
    "wait_until_ondisk",
    "dmget",
    "iter_online",
    "ensure_ondisk",
]


# ---------------------------------------------------------------------------
# tunables
# ---------------------------------------------------------------------------

#: Maximum number of paths handed to a single ``dmls``/``dmget`` process.
#: 500 is the batch size GFDL documents as safe.  ``ARG_MAX`` is 2 MB on the
#: analysis nodes, so 500 archive paths (~100 kB of command line) is a
#: comfortable fraction of it.
BATCH_SIZE = 500

#: Maximum number of ``dmget`` processes running at once.  DMF schedules tape
#: mounts itself; more concurrency mostly just queues more work.
MAX_CONCURRENT_DMGET = 4

#: Default guard rail (in GB) on the size of a single recall request.
MAX_GB = 100.0

#: Refuse to build a command line longer than this many bytes, whatever
#: ``BATCH_SIZE`` says.
MAX_CMDLINE_BYTES = 128_000


#: ``N/A`` is what ``dmls`` reports for files on a filesystem DMF does not
#: manage at all (``/vftmp``, ``/work``, ...) -- always readable.
ONLINE_STATES = frozenset({"REG", "DUL", "MIG", "NMG", "N/A"})
INFLIGHT_STATES = frozenset({"UNM"})
OFFLINE_STATES = frozenset({"OFL"})
SUSPECT_STATES = frozenset({"PAR", "INV"})

#: Pseudo-state used for paths ``dmls`` could not stat at all.
MISSING = "MISSING"


class DMFError(RuntimeError):
    """A ``dmls``/``dmget`` command failed, or a recall did not complete."""


class OfflineDataError(DMFError):
    """Data was requested that is on tape, without permission to recall it."""


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------

# `dmls -l -d` prints, per file:
#   -rw-r--r--  1 owner  group  173943936 2026-03-28 00:44 (DUL) /full/path
# Anchoring on the parenthesised state token means the path may contain
# spaces (the old `output.split(' ')[-1]` could not).
_DMLS_RE = re.compile(r"\((?P<state>[A-Z][A-Z/][A-Z])\)\s+(?P<path>.+?)\s*$")
_DMLS_ERR_RE = re.compile(r"^dmls:\s+(?:Cannot access|cannot access)\s+(?P<path>.+?):")


def format_bytes(nbytes):
    """Human-readable byte count, e.g. ``'1.17 TB'``."""
    nbytes = float(nbytes)
    for unit, scale in (("PB", 1024 ** 5), ("TB", 1024 ** 4),
                        ("GB", 1024 ** 3), ("MB", 1024 ** 2), ("kB", 1024)):
        if abs(nbytes) >= scale:
            return "{:.2f} {}".format(nbytes / scale, unit)
    return "{:.0f} B".format(nbytes)


def format_duration(seconds):
    """Human-readable elapsed time, e.g. ``'2m 07s'``."""
    seconds = int(round(seconds))
    if seconds < 60:
        return "{}s".format(seconds)
    if seconds < 3600:
        return "{}m {:02d}s".format(seconds // 60, seconds % 60)
    return "{}h {:02d}m".format(seconds // 3600, (seconds % 3600) // 60)


def batched(paths, size=None, max_bytes=None):
    """Split ``paths`` into command-line-sized batches.

    Batches are bounded both by count (``size``, default :data:`BATCH_SIZE`)
    and by total command-line length (``max_bytes``, default
    :data:`MAX_CMDLINE_BYTES`), so a handful of pathological paths cannot blow
    past ``ARG_MAX``.

    Yields
    ------
    list of str
    """
    size = BATCH_SIZE if size is None else size
    max_bytes = MAX_CMDLINE_BYTES if max_bytes is None else max_bytes
    if size < 1:
        raise ValueError("batch size must be >= 1")
    batch, nbytes = [], 0
    for path in paths:
        cost = len(str(path)) + 1
        if batch and (len(batch) >= size or nbytes + cost > max_bytes):
            yield batch
            batch, nbytes = [], 0
        batch.append(path)
        nbytes += cost
    if batch:
        yield batch


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class FileStatus:
    """DMF state and size of one file."""

    path: str
    state: str
    size: int = 0

    @property
    def online(self):
        """True if the file can be read now without waiting for tape."""
        return self.state in ONLINE_STATES

    @property
    def offline(self):
        """True if reading the file would require (or is awaiting) a recall."""
        return not self.online and self.state != MISSING

    @property
    def inflight(self):
        """True if a recall for this file is already under way."""
        return self.state in INFLIGHT_STATES

    @property
    def suspect(self):
        """True for partial/invalid DMF states."""
        return self.state in SUSPECT_STATES

    @property
    def missing(self):
        """True if ``dmls`` could not stat the path at all."""
        return self.state == MISSING


@dataclasses.dataclass
class StatusReport:
    """Aggregate DMF status of a collection of paths.

    Indexable and iterable like the ``{path: FileStatus}`` mapping it wraps.
    """

    statuses: dict

    def __getitem__(self, path):
        return self.statuses[path]

    def __iter__(self):
        return iter(self.statuses)

    def __len__(self):
        return len(self.statuses)

    def _select(self, attr):
        return [s.path for s in self.statuses.values() if getattr(s, attr)]

    @property
    def paths(self):
        return list(self.statuses)

    @property
    def online(self):
        """Paths readable right now."""
        return self._select("online")

    @property
    def offline(self):
        """Paths that need a recall (including those already unmigrating)."""
        return self._select("offline")

    @property
    def inflight(self):
        """Paths whose recall is already under way (``UNM``)."""
        return self._select("inflight")

    @property
    def suspect(self):
        """Paths in a partial or invalid DMF state."""
        return self._select("suspect")

    @property
    def missing(self):
        """Paths ``dmls`` could not stat."""
        return self._select("missing")

    @property
    def complete(self):
        """True if every path is on disk (and none are missing)."""
        return not self.offline and not self.missing

    def nbytes(self, paths=None):
        """Total size of ``paths`` (default: all of them)."""
        paths = self.statuses if paths is None else paths
        return sum(self.statuses[p].size for p in paths if p in self.statuses)

    @property
    def nbytes_offline(self):
        return self.nbytes(self.offline)

    def counts(self):
        """``{state: count}`` over every path, most common first."""
        tally = {}
        for status in self.statuses.values():
            tally[status.state] = tally.get(status.state, 0) + 1
        return dict(sorted(tally.items(), key=lambda kv: (-kv[1], kv[0])))

    def summary(self):
        """One-line human-readable summary."""
        n = len(self.statuses)
        states = ", ".join("{} {}".format(v, k) for k, v in self.counts().items())
        return "{} files, {} total [{}]; {} offline ({})".format(
            n, format_bytes(self.nbytes()), states or "-",
            len(self.offline), format_bytes(self.nbytes_offline),
        )

    def __str__(self):
        return self.summary()


def _run(cmd, timeout=None):
    """Run ``cmd`` capturing output; returns a CompletedProcess."""
    return subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=timeout,
    )


def available():
    """True if the DMF client tools are installed on this host."""
    return shutil.which("dmls") is not None


def _require(command):
    if shutil.which(command) is None:
        raise DMFError(
            "'{0}' is not on PATH. This machine does not appear to have the DMF "
            "client tools; tape-aware operations are unavailable here.".format(command)
        )


def _stat_without_dmf(paths):
    """Fall back to ``os.stat`` on hosts with no DMF.

    Nothing can be on tape if there is no tape system, so existing files are
    reported as ``NMG`` (not managed by DMF) and the rest as missing.  This is
    what lets the rest of the package work unchanged off the GFDL analysis
    nodes.
    """
    statuses = {}
    for path in paths:
        try:
            statuses[path] = FileStatus(path, "NMG", os.path.getsize(path))
        except OSError:
            statuses[path] = FileStatus(path, MISSING, 0)
    return statuses


def stat_paths(paths, batch_size=None, timeout=None):
    """DMF state and size of every path, via batched ``dmls -l -d``.

    Parameters
    ----------
    paths : str or iterable of str
        Paths to query.  A single string containing glob metacharacters is
        expanded first.  ``-d`` is passed to ``dmls`` so that a directory
        argument reports the directory itself rather than dumping its contents
        (which ``dmls`` prints without their parent path, and which the old
        implementation therefore mis-attributed).
    batch_size : int, optional
        Paths per ``dmls`` process.  Defaults to :data:`BATCH_SIZE`.
    timeout : float, optional
        Per-batch subprocess timeout in seconds.

    Returns
    -------
    dict
        ``{path: FileStatus}``, keyed by the path exactly as it was passed in.
        Paths ``dmls`` could not stat get state :data:`MISSING`.

    Notes
    -----
    On a host without the DMF client tools this falls back to ``os.stat`` and
    reports existing files as ``NMG``, so the rest of the package works
    unchanged away from the GFDL analysis nodes.
    """
    paths = _as_path_list(paths)
    if not paths:
        return {}
    if not available():
        return _stat_without_dmf(paths)

    # dmls echoes the path back as given, but normalise anyway so that
    # '//' or trailing components still map back onto the caller's strings.
    lookup = {os.path.normpath(p): p for p in paths}
    statuses = {}

    for batch in batched(paths, size=batch_size):
        proc = _run(["dmls", "-l", "-d"] + list(batch), timeout=timeout)
        for line in proc.stdout.splitlines():
            match = _DMLS_RE.search(line)
            if match is None:
                continue
            fields = line.split()
            try:
                size = int(fields[4])
            except (IndexError, ValueError):
                size = 0
            reported = match.group("path")
            key = lookup.get(os.path.normpath(reported), reported)
            statuses[key] = FileStatus(key, match.group("state"), size)
        for line in proc.stderr.splitlines():
            match = _DMLS_ERR_RE.match(line.strip())
            if match is None:
                continue
            reported = match.group("path")
            key = lookup.get(os.path.normpath(reported), reported)
            statuses.setdefault(key, FileStatus(key, MISSING, 0))
        if proc.returncode not in (0, 1) and not proc.stdout.strip():
            raise DMFError(
                "dmls failed with exit code {0} for a batch of {1} paths:\n{2}".format(
                    proc.returncode, len(batch), proc.stderr.strip()[:2000]
                )
            )

    # Anything dmls said nothing about at all: treat as missing rather than
    # silently dropping it (the old code's `all([])` bug in reverse).
    for path in paths:
        statuses.setdefault(path, FileStatus(path, MISSING, 0))
    return statuses


def status_report(paths, **kwargs):
    """:class:`StatusReport` for ``paths``.  See :func:`stat_paths`."""
    return StatusReport(stat_paths(paths, **kwargs))


def _as_path_list(paths):
    """Normalise a str/glob/iterable argument into a list of path strings."""
    if paths is None:
        return []
    if isinstance(paths, str):
        if any(c in paths for c in "*?["):
            return sorted(_glob.glob(paths))
        return [paths]
    return [str(p) for p in paths]


def offline_paths(paths, **kwargs):
    """``(offline_paths, total_bytes)`` for the subset still needing a recall."""
    report = status_report(paths, **kwargs)
    return report.offline, report.nbytes_offline


# ---------------------------------------------------------------------------
# backwards-compatible boolean queries
# ---------------------------------------------------------------------------


def query_ondisk(path):
    """``{path: bool}`` — whether each file is resident on disk.

    Accepts a single path, a glob pattern, or an iterable of paths.  Unlike the
    original implementation this issues one ``dmls`` per :data:`BATCH_SIZE`
    paths rather than one per path, and parses paths containing spaces
    correctly.  Prefer :func:`stat_paths` / :func:`status_report`, which also
    give you the DMF state and the file size.
    """
    return {p: s.online for p, s in stat_paths(path).items()}


def query_all_ondisk(paths):
    """True if every path in ``paths`` is resident on disk.

    A path ``dmls`` cannot stat counts as *not* on disk, so callers keep
    waiting rather than proceeding on a vacuously-true result.
    """
    statuses = stat_paths(paths)
    if not statuses:
        return False
    return all(s.online for s in statuses.values())


def query_dmget(user=None, out=False):
    """1 if ``user`` still has entries in the ``dmwho`` queue, else 0."""
    user = getpass.getuser() if user is None else user
    if shutil.which("dmwho") is None:
        return 0
    proc = _run(["dmwho"])
    lines = [l for l in proc.stdout.splitlines() if user in l]
    if not lines:
        return 0
    if out:
        print("\n".join(lines))
    return 1


# ---------------------------------------------------------------------------
# recall
# ---------------------------------------------------------------------------


def _progress_printer(progress):
    """Normalise the ``progress`` argument into a callable or None."""
    if progress is True:
        return lambda message: print(message, flush=True)
    if not progress:
        return None
    return progress


class _DmgetPool:
    """Runs ``dmget`` over batches with bounded concurrency."""

    def __init__(self, batches, max_concurrent):
        self._pending = list(batches)
        self._max = max(1, int(max_concurrent))
        self._running = []          # list of (Popen, batch)
        self.failures = []          # list of (batch, returncode, stderr)
        self.nbatches = len(self._pending)

    @property
    def done(self):
        return not self._pending and not self._running

    def pump(self):
        """Reap finished processes and launch replacements."""
        still = []
        for proc, batch in self._running:
            if proc.poll() is None:
                still.append((proc, batch))
                continue
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            if proc.returncode != 0:
                self.failures.append((batch, proc.returncode, stderr))
        self._running = still
        while self._pending and len(self._running) < self._max:
            batch = self._pending.pop(0)
            proc = subprocess.Popen(
                ["dmget"] + list(batch),
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            self._running.append((proc, batch))

    @property
    def nfinished(self):
        return self.nbatches - len(self._pending) - len(self._running)


def issue_dmget(path, batch_size=None, max_concurrent=None, wait=True,
                check=True, progress=False):
    """Recall ``path`` from tape with ``dmget``.

    Unlike the original ``os.system("dmget ... &")``, this batches the paths,
    bounds concurrency, and reports real exit codes.

    Parameters
    ----------
    path : str or iterable of str
        Path(s) to recall.
    batch_size : int, optional
        Paths per ``dmget`` process (default :data:`BATCH_SIZE`).
    max_concurrent : int, optional
        Maximum simultaneous ``dmget`` processes
        (default :data:`MAX_CONCURRENT_DMGET`).
    wait : bool, default True
        Block until every ``dmget`` process exits.  ``dmget`` itself returns
        once the recall is complete, so ``wait=True`` means the data is on
        disk when this returns.  With ``wait=False`` the running
        :class:`subprocess.Popen` handles are returned instead and it is the
        caller's job to reap them.
    check : bool, default True
        Raise :class:`DMFError` if any ``dmget`` process exits non-zero.
        Ignored when ``wait=False``.
    progress : bool or callable, default False
        Report batch completion.

    Returns
    -------
    int or list
        ``0`` on success when ``wait=True``; the list of running
        ``(Popen, batch)`` pairs when ``wait=False``.
    """
    paths = _as_path_list(path)
    if not paths:
        return 0
    _require("dmget")

    report = _progress_printer(progress)
    batches = list(batched(paths, size=batch_size))
    pool = _DmgetPool(
        batches,
        MAX_CONCURRENT_DMGET if max_concurrent is None else max_concurrent,
    )
    pool.pump()

    if not wait:
        return list(pool._running) + [(None, b) for b in pool._pending]

    while not pool.done:
        _time.sleep(0.2)
        finished_before = pool.nfinished
        pool.pump()
        if report is not None and pool.nfinished != finished_before:
            report("dmget: {}/{} batches complete".format(
                pool.nfinished, pool.nbatches))

    if pool.failures and check:
        batch, rc, stderr = pool.failures[0]
        raise DMFError(
            "dmget failed for {0} of {1} batches (first failure: exit code {2}, "
            "{3} paths, e.g. {4}).\n{5}\n"
            "Check the DMF queues with `dmwho`.".format(
                len(pool.failures), pool.nbatches, rc, len(batch), batch[0],
                stderr.strip()[:2000],
            )
        )
    return 1 if pool.failures else 0


def wait_until_ondisk(paths, dmget_timeout=10800, poll=2.0, max_poll=30.0,
                      progress=True, batch_size=None):
    """Block until every path in ``paths`` is resident on disk.

    Polls with a single batched ``dmls`` per iteration (2 subprocesses for 900
    files, not 900) and exponential backoff, and reports DMF state counts as it
    goes rather than a bare boolean.

    Parameters
    ----------
    paths : iterable of str
    dmget_timeout : float or None, default 10800
        Seconds to wait before raising :class:`TimeoutError`.  ``None`` waits
        indefinitely.
    poll : float, default 2.0
        Initial poll interval in seconds.
    max_poll : float, default 30.0
        Maximum poll interval; the interval grows by 1.5x per iteration.
    progress : bool or callable, default True
        Report progress lines while waiting.
    batch_size : int, optional

    Returns
    -------
    StatusReport
        The final (complete) status.

    Raises
    ------
    TimeoutError
        If the recall does not complete within ``dmget_timeout`` seconds.  The
        message names how many files remain in each DMF state.
    FileNotFoundError
        If any path cannot be stat'ed at all — waiting for it would never
        terminate.
    """
    paths = _as_path_list(paths)
    if not paths:
        return StatusReport({})

    report = _progress_printer(progress)
    start = _time.time()
    delay = poll
    last_message = None

    while True:
        current = status_report(paths, batch_size=batch_size)
        if current.missing:
            raise FileNotFoundError(
                "{0} of {1} paths do not exist (dmls cannot stat them), so they "
                "will never migrate to disk; e.g. {2}".format(
                    len(current.missing), len(paths), current.missing[0])
            )
        if current.complete:
            if report is not None and last_message is not None:
                report("dmget: all {0} files on disk after {1}".format(
                    len(paths), format_duration(_time.time() - start)))
            return current

        elapsed = _time.time() - start
        if report is not None:
            message = "dmget: {0}/{1} on disk ({2} remaining), states [{3}], {4} elapsed".format(
                len(current.online), len(paths),
                format_bytes(current.nbytes_offline),
                ", ".join("{} {}".format(v, k) for k, v in current.counts().items()),
                format_duration(elapsed),
            )
            if message != last_message:
                report(message)
                last_message = message

        if dmget_timeout is not None and elapsed > dmget_timeout:
            raise TimeoutError(
                "Timed out after {0} waiting for DMF migration: {1} of {2} paths "
                "still offline ({3}), states [{4}]. Examples: {5}. "
                "The recall may still be running -- check `dmwho`.".format(
                    format_duration(elapsed), len(current.offline), len(paths),
                    format_bytes(current.nbytes_offline),
                    ", ".join("{} {}".format(v, k) for k, v in current.counts().items()),
                    ", ".join(current.offline[:3]),
                )
            )

        _time.sleep(delay)
        delay = min(delay * 1.5, max_poll)


def dmget(paths, max_gb=None, force=False, dmget_timeout=10800, progress=True,
          batch_size=None, max_concurrent=None, skip_online=True):
    """Recall ``paths`` from tape, blocking until they are on disk.

    This is the recommended entry point.  It

    1. takes a single batched inventory of the request (states + sizes),
    2. refuses requests larger than ``max_gb`` unless ``force=True``,
    3. submits ``dmget`` in bounded-concurrency batches,
    4. reports progress while the recall runs, and
    5. verifies afterwards that the files really did land.

    Parameters
    ----------
    paths : str or iterable of str
    max_gb : float, optional
        Guard rail on the size of the recall (default :data:`MAX_GB`, 100 GB,
        matching GFDL's guidance to confirm before recalling >100 GB).  Pass
        ``None`` or ``force=True`` to disable.
    force : bool, default False
        Bypass the ``max_gb`` guard.
    dmget_timeout : float or None, default 10800
        Seconds to wait for completion.  ``None`` waits indefinitely.
    progress : bool or callable, default True
    skip_online : bool, default True
        Only pass genuinely offline paths to ``dmget``.

    Returns
    -------
    StatusReport
        Final status of ``paths``.

    Raises
    ------
    FileNotFoundError
        If any path does not exist.
    DMFError
        If the request exceeds ``max_gb``, if ``dmget`` fails, or if files are
        still offline after ``dmget`` claims to have finished.
    TimeoutError
        If the recall does not finish within ``dmget_timeout``.
    """
    paths = _as_path_list(paths)
    if not paths:
        return StatusReport({})

    emit = _progress_printer(progress)
    before = status_report(paths, batch_size=batch_size)

    if before.missing:
        raise FileNotFoundError(
            "{0} of {1} requested paths do not exist, e.g.\n  {2}".format(
                len(before.missing), len(paths), "\n  ".join(before.missing[:5]))
        )
    if before.suspect and emit is not None:
        emit("dmget: warning -- {0} files in a partial/invalid DMF state, e.g. {1}".format(
            len(before.suspect), before.suspect[0]))

    todo = before.offline if skip_online else paths
    if not todo:
        if emit is not None:
            emit("dmget: all {0} files ({1}) already on disk.".format(
                len(paths), format_bytes(before.nbytes())))
        return before

    nbytes = before.nbytes(todo)
    limit = MAX_GB if max_gb is None and not force else max_gb
    if limit is not None and not force and nbytes / 1024 ** 3 > limit:
        raise DMFError(
            "Refusing to recall {0} files ({1}) from tape: that exceeds "
            "max_gb={2}. This is a guard rail, not a hard limit -- re-run with a "
            "larger max_gb (or force=True) if you really want it. GFDL asks that "
            "you confirm recalls of more than 50 files or 100 GB.".format(
                len(todo), format_bytes(nbytes), limit)
        )

    if emit is not None:
        emit("dmget: recalling {0} files ({1}) from tape in {2} batches; "
             "{3} of {4} already on disk.".format(
                 len(todo), format_bytes(nbytes),
                 len(list(batched(todo, size=batch_size))),
                 len(before.online), len(paths)))

    start = _time.time()
    pool = _DmgetPool(
        list(batched(todo, size=batch_size)),
        MAX_CONCURRENT_DMGET if max_concurrent is None else max_concurrent,
    )
    pool.pump()

    delay, last_message = 0.5, None
    while not pool.done:
        _time.sleep(min(delay, 5.0))
        pool.pump()
        if emit is not None:
            current = status_report(todo, batch_size=batch_size)
            message = ("dmget: {0}/{1} on disk ({2} remaining), states [{3}], "
                       "{4}/{5} batches done, {6} elapsed".format(
                           len(current.online), len(todo),
                           format_bytes(current.nbytes_offline),
                           ", ".join("{} {}".format(v, k)
                                     for k, v in current.counts().items()),
                           pool.nfinished, pool.nbatches,
                           format_duration(_time.time() - start)))
            if message != last_message:
                emit(message)
                last_message = message
        delay = min(delay * 1.3, 30.0)
        if dmget_timeout is not None and _time.time() - start > dmget_timeout:
            raise TimeoutError(
                "Timed out after {0} waiting for dmget of {1} files ({2}). "
                "The recall is still running in the background -- check `dmwho`.".format(
                    format_duration(_time.time() - start), len(todo),
                    format_bytes(nbytes))
            )

    if pool.failures:
        batch, rc, stderr = pool.failures[0]
        raise DMFError(
            "dmget failed for {0} of {1} batches (first: exit code {2}, {3} paths, "
            "e.g. {4}).\n{5}".format(
                len(pool.failures), pool.nbatches, rc, len(batch), batch[0],
                stderr.strip()[:2000])
        )

    after = status_report(paths, batch_size=batch_size)
    if not after.complete:
        raise DMFError(
            "dmget exited successfully but {0} of {1} files are still not on disk "
            "({2}), states [{3}]. Examples: {4}".format(
                len(after.offline), len(paths), format_bytes(after.nbytes_offline),
                ", ".join("{} {}".format(v, k) for k, v in after.counts().items()),
                ", ".join(after.offline[:3]))
        )
    if emit is not None:
        emit("dmget: complete -- {0} files ({1}) on disk after {2}.".format(
            len(todo), format_bytes(nbytes), format_duration(_time.time() - start)))
    return after


#: Internal alias so :func:`ensure_ondisk` can call :func:`dmget` despite its
#: own ``dmget=`` boolean argument.
_dmget = dmget


def iter_online(paths, submit=True, poll=5.0, max_poll=60.0, dmget_timeout=10800,
                progress=True, **dmget_kwargs):
    """Yield paths as they become resident on disk.

    Lets a caller start work on the first files of a large recall instead of
    waiting for all of them, and doubles as a prefetch primitive: submit a
    chunk, consume it as it lands, submit the next.

    Paths already on disk are yielded immediately (in input order); the rest are
    yielded in the order DMF happens to deliver them.

    Parameters
    ----------
    paths : iterable of str
    submit : bool, default True
        Issue ``dmget`` for the offline paths first.  With ``submit=False``
        this only watches an already-running recall.
    poll : float, default 5.0
        Initial poll interval, growing by 1.5x up to ``max_poll``.
    **dmget_kwargs
        Passed to :func:`issue_dmget` (``max_gb`` is honoured up front).

    Yields
    ------
    str
    """
    paths = _as_path_list(paths)
    if not paths:
        return

    emit = _progress_printer(progress)
    report = status_report(paths)
    if report.missing:
        raise FileNotFoundError(
            "{0} of {1} requested paths do not exist, e.g. {2}".format(
                len(report.missing), len(paths), report.missing[0])
        )

    pending = []
    for path in paths:
        if report[path].online:
            yield path
        else:
            pending.append(path)
    if not pending:
        return

    max_gb = dmget_kwargs.pop("max_gb", MAX_GB)
    force = dmget_kwargs.pop("force", False)
    nbytes = report.nbytes(pending)
    if max_gb is not None and not force and nbytes / 1024 ** 3 > max_gb:
        raise DMFError(
            "Refusing to recall {0} files ({1}) from tape: exceeds max_gb={2}. "
            "Pass a larger max_gb or force=True.".format(
                len(pending), format_bytes(nbytes), max_gb)
        )

    handles = None
    if submit:
        if emit is not None:
            emit("dmget: streaming recall of {0} files ({1}).".format(
                len(pending), format_bytes(nbytes)))
        handles = issue_dmget(pending, wait=False, **dmget_kwargs)

    start, delay = _time.time(), poll
    try:
        while pending:
            _time.sleep(delay)
            delay = min(delay * 1.5, max_poll)
            current = stat_paths(pending)
            landed = [p for p in pending if current[p].online]
            for path in landed:
                yield path
            if landed:
                pending = [p for p in pending if p not in set(landed)]
                delay = poll
                if emit is not None and pending:
                    emit("dmget: {0} files still on tape.".format(len(pending)))
            if pending and dmget_timeout is not None and \
                    _time.time() - start > dmget_timeout:
                raise TimeoutError(
                    "Timed out after {0} with {1} files still offline, e.g. {2}".format(
                        format_duration(_time.time() - start), len(pending), pending[0])
                )
    finally:
        if handles:
            for proc, _batch in handles:
                if proc is not None and proc.poll() is None:
                    # Leave the recall running: killing dmget would waste the
                    # tape mounts already scheduled.  Just stop tracking it.
                    pass


# ---------------------------------------------------------------------------
# the guard
# ---------------------------------------------------------------------------


def ensure_ondisk(paths, dmget=False, max_gb=None, on_offline=None,
                  dmget_timeout=10800, progress=True, **kwargs):
    """Make sure ``paths`` can be read without silently blocking on tape.

    This is the guard that turns "``open_mfdataset`` hangs for an hour with no
    output" into an actionable error.

    Parameters
    ----------
    paths : str or iterable of str
    dmget : bool, default False
        Shorthand for ``on_offline='dmget'``.
    on_offline : {'raise', 'dmget', 'warn', 'ignore'}, optional
        What to do when some paths are on tape.  Defaults to ``'dmget'`` if
        ``dmget=True``, else ``'raise'``.

        ``'raise'``
            Raise :class:`OfflineDataError` naming the count, the size and how
            to fix it.  This is the default because reading offline data is
            almost never what someone meant to do.
        ``'dmget'``
            Recall them (see :func:`dmget`) and block until they land.
        ``'warn'``
            Emit a :class:`UserWarning` and carry on (the read will block).
        ``'ignore'``
            Do nothing — the historical behaviour.

    Returns
    -------
    StatusReport or None
    """
    paths = _as_path_list(paths)
    if not paths:
        return None

    if on_offline is None:
        on_offline = "dmget" if dmget else "raise"
    if on_offline == "ignore":
        return None
    if on_offline not in ("raise", "dmget", "warn"):
        raise ValueError(
            "on_offline must be one of 'raise', 'dmget', 'warn', 'ignore'; "
            "got {0!r}".format(on_offline)
        )

    if on_offline == "dmget":
        return _dmget(
            paths, max_gb=max_gb, dmget_timeout=dmget_timeout,
            progress=progress, **kwargs
        )

    report = status_report(paths)
    if report.missing:
        raise FileNotFoundError(
            "{0} of {1} paths do not exist, e.g.\n  {2}".format(
                len(report.missing), len(paths), "\n  ".join(report.missing[:5]))
        )
    if not report.offline:
        return report

    message = (
        "{0} of {1} files are offline (on tape), {2} total. Reading them would "
        "block -- possibly for hours -- with no output while DMF recalls them.\n"
        "  Pass dmget=True to recall them first (add max_gb=... if the request is "
        "large), or on_offline='ignore' to read them anyway.\n"
        "  States: [{3}]\n"
        "  First offline file: {4}".format(
            len(report.offline), len(paths), format_bytes(report.nbytes_offline),
            ", ".join("{} {}".format(v, k) for k, v in report.counts().items()),
            report.offline[0],
        )
    )
    if on_offline == "warn":
        warnings.warn(message, UserWarning, stacklevel=2)
        return report
    raise OfflineDataError(message)
