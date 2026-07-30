"""Tests for the xarray opening layer.

These write small real netCDF files, so they need a netCDF backend; they skip
themselves if none is installed.
"""

import os
import warnings

import numpy as np
import pytest

xr = pytest.importorskip("xarray")
cftime = pytest.importorskip("cftime")

from gfdl_utils import dmf, load, paths  # noqa: E402


def _has_netcdf_backend():
    try:
        return bool(set(xr.backends.list_engines()) & {"netcdf4", "h5netcdf", "scipy"})
    except Exception:                                        # pragma: no cover
        return False


pytestmark = pytest.mark.skipif(
    not _has_netcdf_backend(), reason="no netCDF backend installed")


def _dataset(year0, nyears, variables=("tos",)):
    time = [cftime.DatetimeNoLeap(year0 + i, 7, 2, 12) for i in range(nyears)]
    bnds = np.array(
        [[cftime.DatetimeNoLeap(year0 + i, 1, 1),
          cftime.DatetimeNoLeap(year0 + i + 1, 1, 1)] for i in range(nyears)],
        dtype=object,
    )
    data = {
        name: (("time", "y"), np.arange(nyears * 3, dtype="f4").reshape(nyears, 3) + j)
        for j, name in enumerate(variables)
    }
    data["time_bnds"] = (("time", "nv"), bnds)
    return xr.Dataset(data, coords={"time": time, "y": [0, 1, 2]})


@pytest.fixture
def nctree(tmp_path):
    """A pp tree of real 5-year netCDF files, with a hole at years 11-15."""
    root = tmp_path / "pp"
    directory = root / "ocean_annual" / "ts" / "annual" / "5yr"
    directory.mkdir(parents=True)
    for start in (1, 6, 16):
        span = "{0:04d}-{1:04d}".format(start, start + 4)
        _dataset(start, 5, ("tos", "sos")).to_netcdf(
            str(directory / "ocean_annual.{0}.tos.nc".format(span)))
        _dataset(start, 5, ("sos",)).to_netcdf(
            str(directory / "ocean_annual.{0}.sos.nc".format(span)))
    static = xr.Dataset({"areacello": (("y",), np.ones(3))})
    static.to_netcdf(str(root / "ocean_annual" / "ocean_annual.static.nc"))
    paths.clear_cache()
    yield str(root)
    paths.clear_cache()


# ---------------------------------------------------------------------------
# decoding helpers
# ---------------------------------------------------------------------------


def test_cftime_decoding_uses_the_supported_spelling():
    kwargs = load.cftime_decoding()
    if hasattr(getattr(xr, "coders", None), "CFDatetimeCoder"):
        assert "use_cftime" not in kwargs
        assert isinstance(kwargs["decode_times"], xr.coders.CFDatetimeCoder)
    else:                                                    # pragma: no cover
        assert kwargs == {"use_cftime": True}


def test_open_emits_no_future_warning(nctree):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                         years=(1, 10), warn_gaps=False)
    assert not [w for w in caught
                if issubclass(w.category, FutureWarning) and "use_cftime" in str(w.message)]


def test_deprecated_use_cftime_kwarg_is_translated(nctree):
    kwargs = load._open_mfdataset_kwargs({"use_cftime": True})
    assert "use_cftime" not in kwargs or "decode_times" not in kwargs


def test_cftime_variables_are_loaded_eagerly(nctree):
    ds = load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                          years=(1, 10), warn_gaps=False)
    assert ds.time_bnds.dtype == object
    assert ds.time_bnds.chunks is None, "object-dtype cftime must not stay dask-backed"
    # the failure mode this prevents: concatenating dask object arrays
    xr.concat([ds.isel(time=slice(0, 2)), ds.isel(time=slice(2, 4)).load()],
              dim="time")


def test_load_cftime_variables_can_be_disabled(nctree):
    ds = load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                          years=(1, 10), warn_gaps=False, load_cftime=False)
    assert ds.time_bnds.chunks is not None


# ---------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------


def test_years_limits_what_is_opened(nctree):
    ds = load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                          years=(1, 10), warn_gaps=False)
    assert ds.sizes["time"] == 10
    assert ds.time.values[0].year == 1 and ds.time.values[-1].year == 10


def test_years_does_not_open_excluded_files(nctree, monkeypatch):
    opened = []
    real = xr.open_mfdataset

    def spy(paths_, **kwargs):
        opened.extend(paths_)
        return real(paths_, **kwargs)

    monkeypatch.setattr(load.xr, "open_mfdataset", spy)
    load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                     years=(16, 20), warn_gaps=False)
    assert len(opened) == 1 and "0016-0020" in opened[0]


def test_multiple_variables_are_merged(nctree):
    ds = load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*",
                          ["tos", "sos"], years=(1, 10), warn_gaps=False)
    assert {"tos", "sos"} <= set(ds.data_vars)


def test_wildcard_add_expands(nctree):
    ds = load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "*",
                          years=(1, 5), warn_gaps=False)
    assert {"tos", "sos"} <= set(ds.data_vars)


def test_missing_variable_raises_with_the_directory(nctree):
    with pytest.raises(FileNotFoundError) as excinfo:
        load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "nope")
    assert "nope" in str(excinfo.value) and "annual/5yr" in str(excinfo.value)


def test_gaps_are_warned_about(nctree):
    with pytest.warns(UserWarning, match="11-15"):
        load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos")


def test_gaps_warning_can_be_silenced(nctree, recwarn):
    load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                     warn_gaps=False)
    assert not [w for w in recwarn if "no post-processed files" in str(w.message)]


def test_open_static(nctree):
    ds = load.open_static(nctree, "ocean_annual")
    assert "areacello" in ds


# ---------------------------------------------------------------------------
# the tape guard
# ---------------------------------------------------------------------------


def test_open_frompp_refuses_offline_files(nctree, monkeypatch):
    def all_offline(paths_, **kwargs):
        return {p: dmf.FileStatus(p, "OFL", 10 * 1024 ** 3) for p in paths_}

    monkeypatch.setattr(dmf, "stat_paths", all_offline)
    with pytest.raises(dmf.OfflineDataError) as excinfo:
        load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                         years=(1, 10), warn_gaps=False)
    assert "offline" in str(excinfo.value) and "dmget=True" in str(excinfo.value)


def test_open_frompp_ignore_policy_opens_anyway(nctree, monkeypatch):
    def all_offline(paths_, **kwargs):
        return {p: dmf.FileStatus(p, "OFL", 10) for p in paths_}

    monkeypatch.setattr(dmf, "stat_paths", all_offline)
    ds = load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                          years=(1, 10), warn_gaps=False, on_offline="ignore")
    assert ds.sizes["time"] == 10


def test_dmget_and_mirror_are_mutually_exclusive(nctree):
    with pytest.raises(ValueError):
        load.open_frompp(nctree, "ocean_annual", "ts", "annual/5yr", "*", "tos",
                         dmget=True, mirror=True)


# ---------------------------------------------------------------------------
# backwards compatibility
# ---------------------------------------------------------------------------


def test_core_still_exposes_the_old_names():
    from gfdl_utils import core

    for name in ("open_frompp", "open_static", "get_pathspp", "get_pathstatic",
                 "issue_dmget", "query_dmget", "query_ondisk", "query_all_ondisk",
                 "wait_until_ondisk", "mirror_path", "get_ppnames", "get_local",
                 "get_timefrequency", "get_varnames", "get_allvars",
                 "find_variable", "find_unique_variable", "query_is1x1deg"):
        assert callable(getattr(core, name)), name


def test_import_gfdl_utils_exposes_core_lazily():
    import gfdl_utils

    assert gfdl_utils.core.open_frompp is load.open_frompp
    assert "core" in dir(gfdl_utils)
