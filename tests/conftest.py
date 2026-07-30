"""Shared fixtures.

The DMF tests never invoke the real ``dmls``/``dmget``: a tape recall is
expensive and the test suite has to run on machines that have no DMF at all.
Instead the subprocess layer is stubbed with a fake archive whose ``dmls``
output is byte-for-byte the format the real one produces.
"""

import os
import subprocess

import pytest

from gfdl_utils import dmf


DMLS_LINE = ("-rw-r--r--  1 oar.gfdl.bgrp-account    b {size:>12} "
             "2026-03-30 16:54 ({state}) {path}")


class FakeArchive:
    """A stand-in for DMF: maps paths to (state, size) and records commands."""

    def __init__(self, files=None):
        self.files = dict(files or {})
        self.dmls_calls = []
        self.dmget_calls = []
        #: state files transition to once ``dmget`` has been run on them
        self.recall_state = "DUL"

    # -- construction helpers -------------------------------------------------

    def add(self, path, state="DUL", size=1024):
        self.files[os.path.normpath(path)] = (state, size)
        return path

    def _lookup(self, path):
        return self.files.get(os.path.normpath(path))

    # -- fake subprocess ------------------------------------------------------

    def run(self, cmd, timeout=None):
        assert cmd[0] == "dmls"
        paths = [c for c in cmd[1:] if not c.startswith("-")]
        self.dmls_calls.append(paths)
        out, err, rc = [], [], 0
        for path in paths:
            entry = self._lookup(path)
            if entry is not None:
                state, size = entry
                # the real dmls echoes the path back exactly as given
                out.append(DMLS_LINE.format(size=size, state=state, path=path))
            else:
                rc = 1
                err.append("dmls: Cannot access {0}: No such file or "
                           "directory".format(path))
        return subprocess.CompletedProcess(
            cmd, rc, "\n".join(out) + "\n", "\n".join(err) + "\n")

    def popen(self, cmd, **kwargs):
        assert cmd[0] == "dmget"
        paths = list(cmd[1:])
        self.dmget_calls.append(paths)
        ok = True
        for path in paths:
            entry = self._lookup(path)
            if entry is None:
                ok = False
                continue
            self.files[os.path.normpath(path)] = (self.recall_state, entry[1])
        return FakeProc(0 if ok else 1)


class FakeProc:
    """Minimal :class:`subprocess.Popen` stand-in that has already exited."""

    def __init__(self, returncode=0, stderr=""):
        self.returncode = returncode
        self.stderr = _FakeStream(stderr)

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode


class _FakeStream:
    def __init__(self, text):
        self._text = text

    def read(self):
        return self._text


@pytest.fixture
def archive(monkeypatch):
    """A :class:`FakeArchive` wired into :mod:`gfdl_utils.dmf`."""
    fake = FakeArchive()
    monkeypatch.setattr(dmf, "available", lambda: True)
    monkeypatch.setattr(dmf, "_require", lambda command: None)
    monkeypatch.setattr(dmf, "_run", fake.run)
    monkeypatch.setattr(dmf.subprocess, "Popen", fake.popen)
    return fake


@pytest.fixture
def pptree(tmp_path):
    """A miniature post-processing tree.

    ``ocean_annual`` has 5yr and 20yr annual timeseries, a hole at years
    11-15 in the 5yr tree, monthly output, and a static file.
    """
    root = tmp_path / "pp"

    def touch(relpath):
        path = root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
        return str(path)

    for start in (1, 6, 16, 21):
        span = "{0:04d}-{1:04d}".format(start, start + 4)
        for var in ("tos", "sos"):
            touch("ocean_annual/ts/annual/5yr/ocean_annual.{0}.{1}.nc".format(span, var))
    for start in (1, 21):
        span = "{0:04d}-{1:04d}".format(start, start + 19)
        touch("ocean_annual/ts/annual/20yr/ocean_annual.{0}.tos.nc".format(span))
    touch("ocean_annual/ts/monthly/5yr/ocean_annual.000101-000512.tos.nc")
    touch("ocean_annual/ocean_annual.static.nc")
    touch("atmos/ts/monthly/5yr/atmos.000101-000512.t_ref.nc")
    touch("atmos/atmos.static.nc")
    return str(root)
