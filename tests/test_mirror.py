"""Tests for the ``gcp`` mirroring layer."""

import os
import subprocess

import pytest

from gfdl_utils import mirror


class FakeGcp:
    """Records ``gcp`` invocations and creates the destination files."""

    def __init__(self, returncode=0):
        self.calls = []
        self.returncode = returncode

    def __call__(self, cmd, **kwargs):
        self.calls.append(cmd)
        sources, destdir = cmd[1:-1], cmd[-1]
        if self.returncode == 0:
            for src in sources:
                dst = os.path.join(destdir, os.path.basename(src))
                with open(dst, "wb") as handle:
                    handle.write(b"")
        return subprocess.CompletedProcess(cmd, self.returncode, "", "gcp: boom")


@pytest.fixture
def gcp(monkeypatch):
    fake = FakeGcp()
    monkeypatch.setattr(mirror.shutil, "which", lambda name: "/usr/bin/" + name)
    monkeypatch.setattr(mirror.subprocess, "run", fake)
    return fake


def test_mirrored_path():
    assert mirror.mirrored_path("/archive/a/b.nc", "/vftmp/me") == "/vftmp/me/archive/a/b.nc"


def test_mirror_copies_and_returns_destinations(gcp, tmp_path, pptree):
    sources = [
        os.path.join(pptree, "ocean_annual/ts/annual/5yr/ocean_annual.0001-0005.tos.nc"),
        os.path.join(pptree, "ocean_annual/ocean_annual.static.nc"),
    ]
    prefix = str(tmp_path / "scratch")
    out = mirror.mirror_path(sources, prefix=prefix, verbose=False)
    assert out == [mirror.mirrored_path(s, prefix) for s in sources]
    assert all(os.path.isfile(p) for p in out)


def test_mirror_groups_by_source_directory(gcp, tmp_path, pptree):
    """A list spanning several directories must not be flattened into one.

    The previous implementation derived a single destination from ``path[0]``.
    """
    sources = [
        os.path.join(pptree, "ocean_annual/ts/annual/5yr/ocean_annual.0001-0005.tos.nc"),
        os.path.join(pptree, "atmos/ts/monthly/5yr/atmos.000101-000512.t_ref.nc"),
    ]
    prefix = str(tmp_path / "scratch")
    out = mirror.mirror_path(sources, prefix=prefix, verbose=False)
    assert len(gcp.calls) == 2
    assert {os.path.dirname(p) for p in out} == {c[-1].rstrip("/") for c in gcp.calls}


def test_mirror_skips_files_already_present(gcp, tmp_path, pptree):
    """Regression: ``path_to_copy`` used to be computed and then ignored."""
    sources = [
        os.path.join(pptree, "ocean_annual/ts/annual/5yr/ocean_annual.{0}.tos.nc".format(s))
        for s in ("0001-0005", "0006-0010")
    ]
    prefix = str(tmp_path / "scratch")
    mirror.mirror_path(sources, prefix=prefix, verbose=False)
    assert gcp.calls == [["gcp"] + sources + [os.path.dirname(
        mirror.mirrored_path(sources[0], prefix)) + "/"]]

    gcp.calls.clear()
    mirror.mirror_path(sources, prefix=prefix, verbose=False)
    assert gcp.calls == []                                   # nothing left to do

    os.remove(mirror.mirrored_path(sources[0], prefix))
    gcp.calls.clear()
    mirror.mirror_path(sources, prefix=prefix, verbose=False)
    assert len(gcp.calls) == 1
    assert gcp.calls[0][1:-1] == [sources[0]]                # only the missing one


def test_mirror_overwrite_recopies(gcp, tmp_path, pptree):
    source = os.path.join(
        pptree, "ocean_annual/ts/annual/5yr/ocean_annual.0001-0005.tos.nc")
    prefix = str(tmp_path / "scratch")
    mirror.mirror_path([source], prefix=prefix, verbose=False)
    gcp.calls.clear()
    mirror.mirror_path([source], prefix=prefix, overwrite=True, verbose=False)
    assert len(gcp.calls) == 1


def test_mirror_raises_on_gcp_failure(gcp, tmp_path, pptree):
    gcp.returncode = 1
    source = os.path.join(
        pptree, "ocean_annual/ts/annual/5yr/ocean_annual.0001-0005.tos.nc")
    with pytest.raises(RuntimeError, match="gcp failed"):
        mirror.mirror_path([source], prefix=str(tmp_path / "scratch"), verbose=False)


def test_mirror_raises_when_gcp_is_absent(monkeypatch, tmp_path, pptree):
    monkeypatch.setattr(mirror.shutil, "which", lambda name: None)
    source = os.path.join(
        pptree, "ocean_annual/ts/annual/5yr/ocean_annual.0001-0005.tos.nc")
    with pytest.raises(RuntimeError, match="not on PATH"):
        mirror.mirror_path([source], prefix=str(tmp_path / "scratch"), verbose=False)


def test_mirror_of_nothing(gcp):
    assert mirror.mirror_path([]) == []
    assert mirror.mirror_path(None) == []
