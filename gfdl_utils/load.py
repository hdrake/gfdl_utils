"""Opening GFDL post-processed output with xarray.

The strategy is one :func:`xarray.open_mfdataset` call per variable, merged
afterwards.  That avoids xarray inspecting (and aligning) coordinates across
thousands of files of different variables, which is where most of the wall time
of a naive ``open_mfdataset(glob)`` goes.

What this layer adds on top of :mod:`gfdl_utils.paths` and
:mod:`gfdl_utils.dmf`:

* a **tape guard**: by default, opening files that live on tape raises a clear
  error instead of blocking for hours inside ``open_mfdataset`` with no output;
* **year filtering**: ``years=(first, last)`` drops whole files before any of
  them is opened;
* **gap warnings**: missing post-processing shows up as a warning, not as a
  silently discontinuous time axis;
* **forward-compatible decoding**: ``use_cftime`` is passed the way current
  xarray wants it, and cftime (object-dtype) variables such as ``time_bnds``
  are loaded eagerly so they do not come back as dask object arrays that cannot
  later be concatenated.
"""

from __future__ import annotations

import warnings

import numpy as np
import xarray as xr

from . import dmf
from .mirror import DEFAULT_PREFIX, mirror_path, mirrored_path
from .paths import (
    find_year_gaps,
    format_year_gaps,
    get_pathspp,
    get_pathspp_list,
    get_pathstatic,
    parse_ppfilename,
)

__all__ = [
    "open_frompp",
    "open_static",
    "cftime_decoding",
    "load_cftime_variables",
    "DEFAULT_DECODE_TIMEDELTA",
    "DEFAULT_ON_OFFLINE",
]

#: Value passed to ``decode_timedelta`` when the caller does not specify one.
#: ``False`` matches the default xarray is moving towards, and keeps variables
#: with time-like units (``average_DT`` and friends) as plain numbers.  Set to
#: ``None`` to restore xarray's own (currently deprecated) behaviour.
DEFAULT_DECODE_TIMEDELTA = False

#: What :func:`open_frompp` does about files that are on tape when ``dmget`` is
#: not requested.  One of ``'raise'``, ``'dmget'``, ``'warn'``, ``'ignore'``.
DEFAULT_ON_OFFLINE = "raise"


def cftime_decoding(use_cftime=True):
    """Keyword arguments that make xarray decode times as cftime objects.

    ``use_cftime=True`` as a top-level keyword is deprecated in current xarray
    (it emits a ``FutureWarning`` once per file); the supported spelling is
    ``decode_times=xr.coders.CFDatetimeCoder(use_cftime=True)``.  This helper
    returns whichever the installed xarray supports.
    """
    coders = getattr(xr, "coders", None)
    coder_cls = getattr(coders, "CFDatetimeCoder", None) if coders else None
    if coder_cls is not None:
        return {"decode_times": coder_cls(use_cftime=use_cftime)}
    return {"use_cftime": use_cftime}


def _is_cftime_variable(var, max_size):
    """True for a small object-dtype variable holding cftime datetimes."""
    if var.dtype != object or var.size == 0 or var.size > max_size:
        return False
    try:
        sample = np.asarray(var[(0,) * var.ndim].values).item()
    except Exception:
        return False
    return hasattr(sample, "calendar")


def load_cftime_variables(ds, max_size=1_000_000):
    """Eagerly load object-dtype cftime variables (e.g. ``time_bnds``).

    Dask cannot rechunk object arrays, so a dask-backed ``time_bnds`` blows up
    with ``NotImplementedError: Can not use auto rechunking with object dtype``
    as soon as it meets an in-memory one in a ``concat``.  These variables are
    a handful of kilobytes, so loading them up front removes a whole class of
    downstream failure at no cost.

    Parameters
    ----------
    ds : xarray.Dataset
    max_size : int, default 1e6
        Do not load object variables larger than this many elements.

    Returns
    -------
    xarray.Dataset
    """
    names = [
        name for name, var in ds.variables.items()
        if var.chunks is not None and _is_cftime_variable(var, max_size)
    ]
    if not names:
        return ds
    ds = ds.copy()
    for name in names:
        var = ds.variables[name]
        loaded = xr.Variable(var.dims, np.asarray(var.values), var.attrs)
        loaded.encoding = var.encoding
        if name in ds.coords:
            ds = ds.assign_coords({name: loaded})
        else:
            ds[name] = loaded
    return ds


def _resolve_on_offline(on_offline, dmget):
    """Policy for offline files: explicit > ``dmget=True`` > module default."""
    if on_offline is not None:
        return on_offline
    return "dmget" if dmget else DEFAULT_ON_OFFLINE


def _open_mfdataset_kwargs(kwargs):
    """Merge caller keywords with the defaults ``open_frompp`` relies on."""
    kwargs = dict(kwargs)
    if "use_cftime" in kwargs and "decode_times" not in kwargs:
        # Translate the deprecated spelling rather than passing both.
        kwargs.update(cftime_decoding(kwargs.pop("use_cftime")))
    open_kwargs = dict(cftime_decoding())
    open_kwargs["decode_timedelta"] = DEFAULT_DECODE_TIMEDELTA
    open_kwargs.update(kwargs)
    open_kwargs.setdefault("combine", "nested")
    open_kwargs.setdefault("concat_dim", "time")
    open_kwargs.setdefault("coords", "minimal")
    open_kwargs.setdefault("data_vars", "minimal")
    open_kwargs.setdefault("compat", "override")
    open_kwargs.setdefault("join", "outer")
    if open_kwargs.get("decode_timedelta", False) is None:
        open_kwargs.pop("decode_timedelta")
    return open_kwargs


def _resolve_adds(pp, ppname, out, local, time, add, years, use_cache):
    """Expand ``add`` (a name, a wildcard, or a list of either) to real names.

    Wildcards are resolved from a single cached directory listing rather than
    one ``glob`` per candidate variable.
    """
    if add is None:
        raise TypeError("`add` must be a string or list of strings, not None.")
    entries = [add] if isinstance(add, str) else list(add)
    if not all(isinstance(e, str) for e in entries):
        raise TypeError("`add` must be a string or list of strings.")

    resolved, seen = [], set()
    for entry in entries:
        if not any(c in entry for c in "*?["):
            if entry not in seen:
                seen.add(entry)
                resolved.append(entry)
            continue
        matches = set()
        for path in get_pathspp_list(pp, ppname, out, local, time, entry,
                                     years=years, use_cache=use_cache):
            parsed = parse_ppfilename(path)
            if parsed is not None:
                matches.add(parsed.add)
        if not matches:
            raise FileNotFoundError(
                "No files found for add={0!r} matching time pattern {1!r} in "
                "{2}.".format(entry, time,
                              get_pathspp(pp, ppname, out, local, time, entry))
            )
        for name in sorted(matches):
            if name not in seen:
                seen.add(name)
                resolved.append(name)
    return resolved


def open_frompp(
    pp,
    ppname,
    out,
    local,
    time,
    add,
    dmget=False,
    dmget_timeout=10800,
    mirror=False,
    prefix=DEFAULT_PREFIX,
    years=None,
    on_offline=None,
    max_gb=None,
    warn_gaps=True,
    load_cftime=True,
    use_cache=True,
    progress=True,
    **kwargs
):
    """Open post-processed output as a single dataset.

    Parameters
    ----------
    pp : str
        Path to the post-processing directory.
    ppname : str
        Name of the post-processing component, e.g. ``'ocean_annual'``.
    out : str
        Averaging of the post-process (``'ts'`` or ``'av'``).
    local : str
        Local file structure, commonly ``'annual/5yr'``.
    time : str
        Time glob, usually ``'*'``.  For a year *range*, prefer ``years=``,
        which understands the filenames instead of relying on the caller to
        write a clever glob.
    add : str or list of str
        Variable name(s).  Wildcards are expanded (``'*'`` opens every variable
        present, which is rarely what you want for a big component).
    dmget : bool, default False
        Recall offline files from tape before opening, and block until they
        land.  Equivalent to ``on_offline='dmget'``.
    dmget_timeout : float or None, default 10800
        Seconds to wait for the recall.  ``None`` waits indefinitely.
    mirror : bool, default False
        Copy the files under ``prefix`` with ``gcp`` and open them from there.
    prefix : str, default ``/vftmp/$USER``
    years : tuple of (int or None), optional
        Inclusive ``(first_year, last_year)`` window.  Files that do not
        overlap it are dropped from their filenames alone, so they are never
        opened, never recalled from tape, and never counted.
    on_offline : {'raise', 'dmget', 'warn', 'ignore'}, optional
        What to do about files that live on tape.  Defaults to ``'dmget'`` when
        ``dmget=True`` and otherwise to :data:`DEFAULT_ON_OFFLINE`
        (``'raise'``).  ``'ignore'`` restores the old behaviour of handing
        offline paths straight to ``open_mfdataset``.
    max_gb : float, optional
        Guard rail on the size of a tape recall; see :func:`gfdl_utils.dmf.dmget`.
    warn_gaps : bool, default True
        Warn when the selected files leave whole years uncovered.
    load_cftime : bool, default True
        Eagerly load object-dtype cftime variables; see
        :func:`load_cftime_variables`.
    use_cache : bool, default True
        Use cached directory listings (:func:`gfdl_utils.paths.clear_cache`
        invalidates them).
    progress : bool, default True
        Print tape-recall progress.
    **kwargs
        Passed through to :func:`xarray.open_mfdataset`.  The concatenation
        keywords (``combine``, ``concat_dim``, ``coords``, ``data_vars``,
        ``compat``) are fixed unless overridden here.

    Returns
    -------
    xarray.Dataset

    Raises
    ------
    gfdl_utils.dmf.OfflineDataError
        If any file is on tape and ``on_offline='raise'``.
    FileNotFoundError
        If any requested variable has no matching files.
    """
    if dmget and mirror:
        raise ValueError("Can not set both `dmget=True` and `mirror=True`.")

    variables = _resolve_adds(pp, ppname, out, local, time, add, years, use_cache)

    paths_by_var, all_paths = {}, []
    for var in variables:
        paths = get_pathspp_list(pp, ppname, out, local, time, var,
                                 years=years, use_cache=use_cache)
        if not paths:
            raise FileNotFoundError(
                "No files found for variable '{0}' with time pattern '{1}'{2} "
                "in {3}.".format(
                    var, time,
                    "" if years is None else " and years {0}".format(tuple(years)),
                    "/".join([pp, ppname, out, local]).replace("//", "/"))
            )
        paths_by_var[var] = paths
        all_paths.extend(paths)

    if warn_gaps:
        for var, paths in paths_by_var.items():
            gaps = find_year_gaps(paths)
            if gaps:
                warnings.warn(
                    "{0}/{1}: no post-processed files for years {2}; the "
                    "concatenated time axis will have holes there.".format(
                        ppname, var, format_year_gaps(gaps)),
                    UserWarning, stacklevel=2,
                )

    if mirror:
        mirror_path(all_paths, prefix=prefix, verbose=progress)
        paths_by_var = {
            var: [mirrored_path(p, prefix) for p in paths]
            for var, paths in paths_by_var.items()
        }
    else:
        dmf.ensure_ondisk(
            all_paths,
            on_offline=_resolve_on_offline(on_offline, dmget),
            max_gb=max_gb, dmget_timeout=dmget_timeout, progress=progress,
        )

    open_kwargs = _open_mfdataset_kwargs(kwargs)

    datasets = []
    for var in variables:
        ds_var = xr.open_mfdataset(paths_by_var[var], **open_kwargs)
        if load_cftime:
            ds_var = load_cftime_variables(ds_var)
        datasets.append(ds_var)

    if len(datasets) == 1:
        return datasets[0]
    return xr.merge(datasets, compat="override", join="outer")


def open_static(pp, ppname, dmget=False, dmget_timeout=10800, on_offline=None,
                max_gb=None, progress=True, **kwargs):
    """Open the static grid file associated with a pp component.

    Parameters
    ----------
    pp : str
        Path to the post-processing directory.
    ppname : str
        Name of the post-processing component.
    dmget : bool, default False
        Recall the file from tape first if it is offline.
    on_offline : {'raise', 'dmget', 'warn', 'ignore'}, optional
        See :func:`open_frompp`.
    **kwargs
        Passed to :func:`xarray.open_dataset`.

    Returns
    -------
    xarray.Dataset
    """
    ds_path = get_pathstatic(pp, ppname)
    dmf.ensure_ondisk(
        [ds_path],
        on_offline=_resolve_on_offline(on_offline, dmget),
        max_gb=max_gb, dmget_timeout=dmget_timeout, progress=progress,
    )
    kwargs.setdefault("decode_timedelta", DEFAULT_DECODE_TIMEDELTA)
    if kwargs.get("decode_timedelta", False) is None:
        kwargs.pop("decode_timedelta")
    return xr.open_dataset(ds_path, **kwargs)
