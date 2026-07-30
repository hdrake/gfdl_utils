"""Backwards-compatible facade over the ``gfdl_utils`` subpackages.

``gfdl_utils.core`` used to be the whole package.  It is now a thin re-export
layer so that existing code (``from gfdl_utils.core import open_frompp``,
``gu.core.get_pathspp(...)``) keeps working unchanged, while the
implementation lives in focused modules:

:mod:`gfdl_utils.dmf`
    the DMF/tape layer (``dmls``/``dmget``): batched, honest about failure,
    with size accounting and guard rails.
:mod:`gfdl_utils.paths`
    post-processing path construction and discovery: filename parsing, year
    filtering, cached directory listings, gap detection.
:mod:`gfdl_utils.load`
    opening post-processed output with xarray.
:mod:`gfdl_utils.mirror`
    mirroring archive files onto local scratch with ``gcp``.

Behaviour changes relative to 0.1.x are listed in the package README.
"""

from __future__ import annotations

import getpass  # noqa: F401  (historically importable from here)

import xarray as xr  # noqa: F401

from .dmf import (  # noqa: F401
    BATCH_SIZE,
    DMFError,
    FileStatus,
    MAX_CONCURRENT_DMGET,
    OfflineDataError,
    StatusReport,
    dmget,
    ensure_ondisk,
    format_bytes,
    issue_dmget,
    iter_online,
    offline_paths,
    query_all_ondisk,
    query_dmget,
    query_ondisk,
    stat_paths,
    status_report,
    wait_until_ondisk,
)
from .load import (  # noqa: F401
    cftime_decoding,
    load_cftime_variables,
    open_frompp,
    open_static,
)
from .mirror import mirror_path, mirrored_path  # noqa: F401
from .paths import (  # noqa: F401
    PPFile,
    cache_info,
    clear_cache,
    file_timerange,
    file_years,
    find_unique_variable,
    find_variable,
    find_year_gaps,
    format_year_gaps,
    get_allvars,
    get_local,
    get_locals,
    get_pathspp,
    get_pathspp_list,
    get_pathstatic,
    get_ppnames,
    get_timefrequency,
    get_varnames,
    listdir,
    parse_ppfilename,
    query_is1x1deg,
    year_coverage,
)

__all__ = [
    # opening
    "open_frompp",
    "open_static",
    "cftime_decoding",
    "load_cftime_variables",
    # paths
    "get_pathspp",
    "get_pathspp_list",
    "get_pathstatic",
    "get_ppnames",
    "get_local",
    "get_locals",
    "get_timefrequency",
    "get_varnames",
    "get_allvars",
    "find_variable",
    "find_unique_variable",
    "find_year_gaps",
    "format_year_gaps",
    "year_coverage",
    "file_years",
    "file_timerange",
    "parse_ppfilename",
    "PPFile",
    "listdir",
    "clear_cache",
    "cache_info",
    "query_is1x1deg",
    # tape
    "issue_dmget",
    "dmget",
    "ensure_ondisk",
    "iter_online",
    "wait_until_ondisk",
    "query_ondisk",
    "query_all_ondisk",
    "query_dmget",
    "stat_paths",
    "status_report",
    "offline_paths",
    "format_bytes",
    "FileStatus",
    "StatusReport",
    "DMFError",
    "OfflineDataError",
    "BATCH_SIZE",
    "MAX_CONCURRENT_DMGET",
    # mirroring
    "mirror_path",
    "mirrored_path",
]
