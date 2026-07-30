"""Tests for post-processing path construction and discovery."""

import os

import pytest

from gfdl_utils import paths


@pytest.fixture(autouse=True)
def _clear_cache():
    paths.clear_cache()
    yield
    paths.clear_cache()


# ---------------------------------------------------------------------------
# filename parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,expected", [
    ("ocean_annual.0001-0005.tos.nc", ("ocean_annual", "0001-0005", "tos")),
    ("ocean_monthly.000101-000512.thetao.nc",
     ("ocean_monthly", "000101-000512", "thetao")),
    ("atmos_level_cmip.00010101-00051231.ta.nc",
     ("atmos_level_cmip", "00010101-00051231", "ta")),
    ("ocean_annual.0001.ann.nc", ("ocean_annual", "0001", "ann")),
])
def test_parse_ppfilename(name, expected):
    assert tuple(paths.parse_ppfilename(name)) == expected


@pytest.mark.parametrize("name", [
    "ocean_annual.static.nc",
    "ocean_annual.0001-0005.tos.nc.md5",
    "README",
])
def test_parse_ppfilename_returns_none_for_non_timeseries(name):
    assert paths.parse_ppfilename(name) is None


@pytest.mark.parametrize("name,years", [
    ("ocean_annual.0001-0005.tos.nc", (1, 5)),
    ("ocean_monthly.018501-201412.tos.nc", (185, 2014)),
    ("ocean_annual.0007.ann.nc", (7, 7)),
])
def test_file_years(name, years):
    assert paths.file_years(name) == years


def test_file_years_raises_on_unparseable():
    with pytest.raises(ValueError):
        paths.file_years("ocean_annual.static.nc")


# ---------------------------------------------------------------------------
# path construction and listing
# ---------------------------------------------------------------------------


def test_get_pathspp_collapses_double_slashes():
    assert paths.get_pathspp("/pp/", "oa", "ts", "annual/5yr", "*", "tos") == \
        "/pp/oa/ts/annual/5yr/oa.*.tos.nc"


def test_get_pathstatic():
    assert paths.get_pathstatic("/pp", "oa") == "/pp/oa/oa.static.nc"


def test_get_pathspp_list_is_sorted_chronologically(pptree):
    found = paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                                   "*", "tos")
    assert [paths.file_years(p) for p in found] == [(1, 5), (6, 10), (16, 20), (21, 25)]


def test_get_pathspp_list_year_filter_drops_whole_files(pptree):
    found = paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                                   "*", "tos", years=(7, 17))
    assert [paths.file_years(p) for p in found] == [(6, 10), (16, 20)]


def test_get_pathspp_list_year_filter_accepts_open_ends(pptree):
    found = paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                                   "*", "tos", years=(None, 10))
    assert [paths.file_years(p) for p in found] == [(1, 5), (6, 10)]


def test_get_pathspp_list_require(pptree):
    with pytest.raises(FileNotFoundError):
        paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                               "*", "nosuchvar", require=True)


def test_get_ppnames(pptree):
    assert paths.get_ppnames(pptree) == ["atmos", "ocean_annual"]


def test_get_locals(pptree):
    assert paths.get_locals(pptree, "ocean_annual", "ts") == [
        "annual/20yr", "annual/5yr", "monthly/5yr"]


# ---------------------------------------------------------------------------
# caching
# ---------------------------------------------------------------------------


def test_listdir_is_cached_and_clearable(pptree):
    directory = os.path.join(pptree, "ocean_annual", "ts", "annual", "5yr")
    first = paths.listdir(directory)
    assert paths.cache_info()["directories"] >= 1
    open(os.path.join(directory, "ocean_annual.0026-0030.tos.nc"), "wb").close()
    assert paths.listdir(directory) == first        # served from cache
    paths.clear_cache()
    assert len(paths.listdir(directory)) == len(first) + 1


def test_listdir_missing_ok(tmp_path):
    with pytest.raises(FileNotFoundError):
        paths.listdir(str(tmp_path / "nope"))
    assert paths.listdir(str(tmp_path / "nope"), missing_ok=True) == []


# ---------------------------------------------------------------------------
# get_local disambiguation
# ---------------------------------------------------------------------------


def test_get_local_uses_the_requested_priorities(pptree):
    assert paths.get_local(pptree, "ocean_annual", "ts",
                           local1priority="annual", local2priority="5yr") == "annual/5yr"


def test_get_local_falls_back_to_frequency_priority(pptree):
    # 'daily' does not exist; monthly outranks annual in FREQUENCY_PRIORITY
    with pytest.warns(UserWarning, match="not available"):
        assert paths.get_local(pptree, "ocean_annual", "ts",
                               local1priority="daily") == "monthly/5yr"


def test_get_local_picks_the_chunk_with_the_most_coverage(pptree):
    # annual/5yr covers 20 years (with a hole), annual/20yr covers 40.
    with pytest.warns(UserWarning, match="Options and coverage"):
        assert paths.get_local(pptree, "ocean_annual", "ts",
                               local1priority="annual",
                               local2priority="1yr") == "annual/20yr"


def test_get_local_strict_raises_and_lists_options(pptree):
    with pytest.raises(ValueError) as excinfo:
        paths.get_local(pptree, "ocean_annual", "ts",
                        local1priority="annual", local2priority="1yr", strict=True)
    assert "20yr" in str(excinfo.value) and "5yr" in str(excinfo.value)


def test_get_local_single_option_does_not_warn(pptree, recwarn):
    assert paths.get_local(pptree, "atmos", "ts") == "monthly/5yr"
    assert not [w for w in recwarn if issubclass(w.category, UserWarning)]


def test_get_timefrequency(pptree):
    assert paths.get_timefrequency(pptree, "atmos") == "monthly"


# ---------------------------------------------------------------------------
# variables
# ---------------------------------------------------------------------------


def test_get_varnames(pptree):
    assert sorted(paths.get_varnames(pptree, "ocean_annual",
                                     local="annual/5yr")) == ["sos", "tos"]


def test_get_varnames_returns_none_without_out_directory(tmp_path):
    (tmp_path / "pp" / "empty").mkdir(parents=True)
    assert paths.get_varnames(str(tmp_path / "pp"), "empty") is None


def test_get_allvars_and_find_variable(pptree):
    allvars = paths.get_allvars(pptree)
    assert set(allvars) == {"atmos", "ocean_annual"}
    assert paths.find_variable(pptree, "t_ref") == ["atmos"]
    assert paths.find_variable(pptree, "nope") is None


def test_find_unique_variable(pptree):
    assert paths.find_unique_variable(pptree, "t_ref") == "atmos"
    with pytest.raises(ValueError):
        paths.find_unique_variable(pptree, "t_ref", require="ocean")


# ---------------------------------------------------------------------------
# gaps
# ---------------------------------------------------------------------------


def test_find_year_gaps(pptree):
    found = paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                                   "*", "tos")
    assert paths.find_year_gaps(found) == [(11, 15)]
    assert paths.format_year_gaps([(11, 15), (20, 20)]) == "11-15, 20"


def test_find_year_gaps_needs_every_variable(pptree):
    tos = paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                                 "*", "tos")
    sos = paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                                 "*", "sos")
    assert paths.find_year_gaps(tos + sos[:1], nrequired=2) == [(6, 25)]


def test_find_year_gaps_of_nothing():
    assert paths.find_year_gaps([]) == []


def test_year_coverage(pptree):
    found = paths.get_pathspp_list(pptree, "ocean_annual", "ts", "annual/5yr",
                                   "*", "tos", years=(1, 10))
    assert paths.year_coverage(found) == set(range(1, 11))


def test_query_is1x1deg():
    assert paths.query_is1x1deg("ocean_monthly_1x1deg")
    assert not paths.query_is1x1deg("ocean_monthly")
