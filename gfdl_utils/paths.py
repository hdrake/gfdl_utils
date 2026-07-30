"""Discovery of GFDL post-processed output on disk.

A post-processing tree looks like::

    <pp>/<ppname>/<out>/<local>/<ppname>.<time>.<add>.nc
    <pp>/<ppname>/<ppname>.static.nc

with ``out`` in ``{ts, av}``, ``local`` something like ``annual/5yr``, ``time``
a ``YYYY[MM[DD]]`` or ``YYYY[MM[DD]]-YYYY[MM[DD]]`` token, and ``add`` the
variable name (for ``ts``) or a climatology label such as ``ann`` (for ``av``).

This module adds three things the original path helpers lacked:

**Year filtering.**  :func:`get_pathspp_list` accepts ``years=(first, last)``
and drops whole files that do not overlap the window, by parsing the filename.
Loading 100 years out of a 1400-year control should not open 280 files.

**Caching.**  These directories routinely hold 15k+ entries and are listed over
and over (once per variable, per component, per call).  Directory listings are
memoised here; call :func:`clear_cache` after writing new post-processed output.

**Gap detection.**  Real runs have holes in their post-processing.  Silently
returning a time axis with missing years invites wrong science, so
:func:`find_year_gaps` reports them and the loader warns.
"""

from __future__ import annotations

import fnmatch
import os
import re
import warnings

__all__ = [
    "clear_cache",
    "cache_info",
    "listdir",
    "PPFile",
    "parse_ppfilename",
    "file_timerange",
    "file_years",
    "get_pathspp",
    "get_pathspp_list",
    "get_pathstatic",
    "get_ppnames",
    "get_locals",
    "get_local",
    "get_timefrequency",
    "get_varnames",
    "get_allvars",
    "find_variable",
    "find_unique_variable",
    "find_year_gaps",
    "format_year_gaps",
    "year_coverage",
    "query_is1x1deg",
]


#: Preference order used to disambiguate the frequency level of ``local`` when
#: the caller's ``local1priority`` is not present.
FREQUENCY_PRIORITY = (
    "monthly", "annual", "daily", "6hr", "3hr", "1hr",
    "8xdaily", "4xdaily", "120hr",
)


# ---------------------------------------------------------------------------
# directory listing cache
# ---------------------------------------------------------------------------

_LISTDIR_CACHE = {}


def clear_cache():
    """Forget every cached directory listing.

    Call this if post-processed output has appeared since the listings were
    taken (e.g. a long-running notebook while ``frepp`` is still writing).
    """
    _LISTDIR_CACHE.clear()


def cache_info():
    """``{'directories': n, 'entries': m}`` describing the listing cache."""
    return {
        "directories": len(_LISTDIR_CACHE),
        "entries": sum(len(v) for v in _LISTDIR_CACHE.values()),
    }


def listdir(path, use_cache=True, missing_ok=False):
    """Sorted ``os.listdir`` of ``path``, memoised.

    Parameters
    ----------
    path : str
    use_cache : bool, default True
        Serve from (and populate) the module-level cache.
    missing_ok : bool, default False
        Return ``[]`` instead of raising when ``path`` does not exist.

    Returns
    -------
    list of str
    """
    key = os.path.normpath(path)
    if use_cache and key in _LISTDIR_CACHE:
        return _LISTDIR_CACHE[key]
    try:
        entries = sorted(os.listdir(path))
    except (FileNotFoundError, NotADirectoryError):
        if missing_ok:
            return []
        raise FileNotFoundError("No such post-processing directory: {0}".format(path))
    except PermissionError:
        if missing_ok:
            return []
        raise
    if use_cache:
        _LISTDIR_CACHE[key] = entries
    return entries


def _listsubdirs(path, use_cache=True, missing_ok=False):
    """Sorted names of the subdirectories of ``path``."""
    return [
        name for name in listdir(path, use_cache=use_cache, missing_ok=missing_ok)
        if os.path.isdir(os.path.join(path, name))
    ]


def _iglob(pattern, use_cache=True):
    """Glob ``pattern``, using the cached listing when only the basename varies.

    Falls back to :func:`glob.glob` when the directory part itself contains
    wildcards.
    """
    directory, name = os.path.split(pattern)
    if any(c in directory for c in "*?["):
        import glob as _glob
        return sorted(_glob.glob(pattern))
    entries = listdir(directory, use_cache=use_cache, missing_ok=True)
    return [os.path.join(directory, e) for e in fnmatch.filter(entries, name)]


# ---------------------------------------------------------------------------
# filename parsing
# ---------------------------------------------------------------------------

# <ppname>.<time>.<add>.nc  -- ppname and add may themselves contain dots,
# so anchor on the time token, which is all digits (with an optional range).
_PPFILE_RE = re.compile(
    r"^(?P<ppname>.+?)\.(?P<time>\d{4,}(?:-\d{4,})?)\.(?P<add>.+)\.nc$"
)


class PPFile(tuple):
    """Parsed post-processed filename: ``(ppname, time, add)``."""

    __slots__ = ()

    def __new__(cls, ppname, time, add):
        return tuple.__new__(cls, (ppname, time, add))

    ppname = property(lambda self: self[0])
    time = property(lambda self: self[1])
    add = property(lambda self: self[2])

    def __repr__(self):
        return "PPFile(ppname={0!r}, time={1!r}, add={2!r})".format(*self)


def parse_ppfilename(path):
    """Parse ``<ppname>.<time>.<add>.nc`` into a :class:`PPFile`.

    Returns ``None`` for anything that does not match (``*.static.nc``,
    ``*.nc.md5``, stray files, ...) rather than raising, so it is safe to map
    over a whole directory listing.
    """
    match = _PPFILE_RE.match(os.path.basename(str(path)))
    if match is None:
        return None
    return PPFile(match.group("ppname"), match.group("time"), match.group("add"))


def file_timerange(path):
    """``(start, end)`` time tokens of a post-processed file, as strings.

    Single-timestamp filenames give ``(t, t)``.

    Raises
    ------
    ValueError
        If the filename has no parseable time token.
    """
    parsed = parse_ppfilename(path)
    if parsed is None:
        raise ValueError("Cannot parse a time range from '{0}'".format(path))
    start, _, end = parsed.time.partition("-")
    return start, (end or start)


def file_years(path):
    """``(first_year, last_year)`` covered by a post-processed file, as ints."""
    start, end = file_timerange(path)
    return int(start[:4]), int(end[:4])


def _sortkey(path):
    """Chronological sort key, robust to mixed YYYY / YYYYMM / YYYYMMDD."""
    try:
        start, end = file_timerange(path)
    except ValueError:
        return ("", "", str(path))
    return (start.ljust(12, "0"), end.ljust(12, "0"), str(path))


def _overlaps(path, years):
    first, last = years
    try:
        y0, y1 = file_years(path)
    except ValueError:
        return True  # unparseable: keep it rather than silently dropping data
    if first is not None and y1 < first:
        return False
    if last is not None and y0 > last:
        return False
    return True


# ---------------------------------------------------------------------------
# path construction
# ---------------------------------------------------------------------------


def get_pathspp(pp, ppname, out, local, time, add):
    """Build the glob pattern for a set of post-processed files.

    Parameters
    ----------
    pp : str
        Path to the post-processing directory.
    ppname : str
        Name of the post-processing component, e.g. ``'ocean_annual'``.
    out : str
        Averaging of the post-process (``'ts'`` or ``'av'``).
    local : str
        Local file structure below ``out``, commonly ``'annual/5yr'``.
    time : str
        Time string; usually the wildcard ``'*'``.
    add : str
        Variable name (for ``ts``) or climatology label (for ``av``).

    Returns
    -------
    str
        Path, possibly including wildcards.  Use :func:`get_pathspp_list` to
        expand it (and to filter it by year).
    """
    filename = ".".join([ppname, time, add, "nc"])
    path = "/".join([pp, ppname, out, local, filename])
    return path.replace("//", "/")


def get_pathspp_list(pp, ppname, out, local, time, add, years=None,
                     use_cache=True, require=False):
    """Expand :func:`get_pathspp` to a chronologically sorted list of files.

    Parameters
    ----------
    pp, ppname, out, local, time, add
        As for :func:`get_pathspp`.
    years : tuple of (int or None), optional
        Inclusive ``(first_year, last_year)`` window.  Files whose filename
        says they do not overlap it are dropped *without being opened* — this
        is the cheap way to load a sub-period of a long control run.  Either
        element may be ``None`` for "unbounded".
    use_cache : bool, default True
        Use the cached directory listing instead of a fresh ``glob``.
    require : bool, default False
        Raise :class:`FileNotFoundError` when nothing matches.

    Returns
    -------
    list of str
    """
    pattern = get_pathspp(pp, ppname, out, local, time, add)
    paths = _iglob(pattern, use_cache=use_cache)
    if years is not None:
        paths = [p for p in paths if _overlaps(p, years)]
    paths = sorted(paths, key=_sortkey)
    if require and not paths:
        raise FileNotFoundError(
            "No files match '{0}'{1}.".format(
                pattern,
                "" if years is None else " within years {0}".format(tuple(years)),
            )
        )
    return paths


def get_pathstatic(pp, ppname):
    """Path to the static grid file associated with a pp component."""
    static = ".".join([ppname, "static", "nc"])
    return "/".join([pp, ppname, static])


# ---------------------------------------------------------------------------
# structure discovery
# ---------------------------------------------------------------------------


def get_ppnames(pp, use_cache=True):
    """List the post-processing components (subdirectories) of ``pp``."""
    return _listsubdirs(pp, use_cache=use_cache)


def get_locals(pp, ppname, out, use_cache=True):
    """Every available ``local`` (``<frequency>/<chunk>``) for a component.

    Returns
    -------
    list of str
        e.g. ``['annual/5yr', 'monthly/5yr', 'monthly/20yr']``.
    """
    root = "/".join([pp, ppname, out])
    combos = []
    for freq in _listsubdirs(root, use_cache=use_cache, missing_ok=True):
        for chunk in _listsubdirs("/".join([root, freq]),
                                  use_cache=use_cache, missing_ok=True):
            combos.append("/".join([freq, chunk]))
    return combos


def _chunk_length(chunk):
    """Leading integer of a chunk directory name (``'20yr'`` -> 20)."""
    match = re.match(r"^(\d+)", str(chunk))
    return int(match.group(1)) if match else 0


def year_coverage(paths):
    """Set of calendar years covered by ``paths``, from their filenames."""
    years = set()
    for path in paths:
        try:
            y0, y1 = file_years(path)
        except ValueError:
            continue
        years.update(range(y0, y1 + 1))
    return years


def _chunk_coverage(pp, ppname, out, freq, chunk, add, use_cache=True):
    """(#years covered, -chunk length) for a candidate chunk directory."""
    adds = [add] if isinstance(add, str) else list(add or ["*"])
    years = set()
    for one in adds:
        years |= year_coverage(
            get_pathspp_list(pp, ppname, out, "/".join([freq, chunk]),
                             "*", one, use_cache=use_cache)
        )
    return (len(years), -_chunk_length(chunk))


def get_local(pp, ppname, out, local1priority="monthly", local2priority="5yr",
              add=None, strict=False, use_cache=True, quiet=False):
    """Resolve the ``<frequency>/<chunk>`` subpath of a pp component.

    The original implementation fell back to ``os.listdir(...)[-1]`` whenever
    the requested priority was absent — i.e. to whatever the filesystem
    happened to return last, which could silently pick the wrong chunk length
    (a ``20yr`` tree instead of the ``5yr`` one, or ``daily`` instead of
    ``annual``).  This version resolves ambiguity deterministically and tells
    you when it had to.

    Frequency is chosen as: the requested ``local1priority`` if present; else
    the only option; else the first match in :data:`FREQUENCY_PRIORITY`; else
    the alphabetically first, with a warning.

    Chunk length is chosen as: the requested ``local2priority`` if present;
    else the only option; else whichever chunk directory actually covers the
    most calendar years (ties broken towards the shorter chunk), with a
    warning naming the alternatives.

    Parameters
    ----------
    pp, ppname, out : str
    local1priority : str, default 'monthly'
    local2priority : str, default '5yr'
    add : str or list of str, optional
        Variable(s) to consider when scoring chunk coverage.  Defaults to all
        files in the directory.
    strict : bool, default False
        Raise :class:`ValueError` (listing the options) instead of guessing
        when the priority is absent and more than one option exists.
    quiet : bool, default False
        Suppress the ambiguity warnings.

    Returns
    -------
    str
        e.g. ``'monthly/5yr'``.
    """
    root = "/".join([pp, ppname, out])
    freqs = _listsubdirs(root, use_cache=use_cache)
    if not freqs:
        raise FileNotFoundError("No frequency subdirectories under '{0}'".format(root))

    if local1priority in freqs:
        freq = local1priority
    elif len(freqs) == 1:
        freq = freqs[0]
    elif strict:
        raise ValueError(
            "Ambiguous frequency for {0}/{1}: '{2}' is not available. "
            "Options: {3}".format(ppname, out, local1priority, freqs)
        )
    else:
        freq = next((f for f in FREQUENCY_PRIORITY if f in freqs), freqs[0])
        if not quiet:
            warnings.warn(
                "'{0}' not available under {1}; using '{2}'. Options: {3}. "
                "Pass local1priority= to choose explicitly.".format(
                    local1priority, root, freq, freqs),
                UserWarning, stacklevel=2,
            )

    freqdir = "/".join([root, freq])
    chunks = _listsubdirs(freqdir, use_cache=use_cache)
    if not chunks:
        raise FileNotFoundError("No chunk subdirectories under '{0}'".format(freqdir))

    if local2priority in chunks:
        chunk = local2priority
    elif len(chunks) == 1:
        chunk = chunks[0]
    elif strict:
        raise ValueError(
            "Ambiguous chunk length for {0}/{1}/{2}: '{3}' is not available. "
            "Options: {4}".format(ppname, out, freq, local2priority, chunks)
        )
    else:
        scores = {
            c: _chunk_coverage(pp, ppname, out, freq, c, add, use_cache=use_cache)
            for c in chunks
        }
        chunk = max(chunks, key=lambda c: scores[c])
        if not quiet:
            warnings.warn(
                "'{0}' not available under {1}; using '{2}' (covers {3} years). "
                "Options and coverage: {4}. Pass local2priority= to choose "
                "explicitly.".format(
                    local2priority, freqdir, chunk, scores[chunk][0],
                    {c: scores[c][0] for c in chunks}),
                UserWarning, stacklevel=2,
            )

    return "/".join([freq, chunk])


def get_timefrequency(pp, ppname, **kwargs):
    """Time frequency of a pp component, inferred from its ``ts`` structure."""
    return get_local(pp, ppname, "ts", **kwargs).split("/")[0]


def get_varnames(pp, ppname, verbose=False, out="ts", local=None, use_cache=True):
    """List the variables with timeseries files in a pp component.

    Returns ``None`` (as before) when the component has no ``<out>``
    directory.
    """
    root = "/".join([pp, ppname, out])
    if not _listsubdirs(root, use_cache=use_cache, missing_ok=True):
        if verbose:
            print("No {0} directory in {1}. Can't retrieve variables.".format(out, ppname))
        return None

    if local is None:
        local = get_local(pp, ppname, out, use_cache=use_cache, quiet=not verbose)

    allvars = []
    seen = set()
    for name in listdir("/".join([root, local]), use_cache=use_cache, missing_ok=True):
        parsed = parse_ppfilename(name)
        if parsed is None or parsed.add in seen:
            continue
        seen.add(parsed.add)
        allvars.append(parsed.add)
    return allvars


def get_allvars(pp, verbose=False, use_cache=True):
    """``{ppname: [variables]}`` for every component of ``pp``."""
    allvars = {}
    for ppname in get_ppnames(pp, use_cache=use_cache):
        varnames = get_varnames(pp, ppname, verbose=verbose, use_cache=use_cache)
        if varnames is not None:
            allvars[ppname] = varnames
    return allvars


def find_variable(pp, variable, verbose=False, use_cache=True):
    """Components of ``pp`` that contain ``variable``."""
    allvars = get_allvars(pp, verbose=verbose, use_cache=use_cache)
    ppnames = [name for name, varnames in allvars.items() if variable in varnames]
    if verbose:
        for name in ppnames:
            print(variable + " is in " + name)
    if not ppnames:
        print("No " + variable + " in this pp.")
        return None
    return ppnames


def find_unique_variable(pp, variable, require=(), ignore=(), unique=True,
                         use_cache=True):
    """The single component containing ``variable`` that matches the filters."""
    if isinstance(ignore, str):
        ignore = [ignore]
    if isinstance(require, str):
        require = [require]
    candidates = find_variable(pp, variable, use_cache=use_cache) or []
    local_list = [
        e for e in candidates
        if all(r in e for r in require) and not any(s in e for s in ignore)
    ]
    if len(local_list) == 1:
        return local_list[0]
    if not local_list:
        raise ValueError(
            "No pp component contains variable '{0}' subject to require={1}, "
            "ignore={2}. Components that do contain it: {3}".format(
                variable, list(require), list(ignore), candidates)
        )
    if unique:
        raise ValueError(
            "Ambiguous request; more than one ppname containing variable "
            "'{0}' satisfies these constraints: {1}.".format(variable, local_list)
        )
    return local_list


# ---------------------------------------------------------------------------
# gaps
# ---------------------------------------------------------------------------


def find_year_gaps(paths, nrequired=1):
    """Years between the first and last file that no file covers.

    Post-processing genuinely goes missing on long runs (whole 5-year chunks
    absent in the middle of a control).  Concatenating across such a hole gives
    a time axis with an invisible discontinuity, so it is worth reporting.

    Parameters
    ----------
    paths : iterable of str
    nrequired : int, default 1
        A year counts as covered only if at least this many files cover it.
        Pass the number of variables when ``paths`` mixes several of them, so
        that a year present for ``thetao`` but not ``so`` is still flagged.

    Returns
    -------
    list of (int, int)
        Inclusive ``(first, last)`` year ranges with no (complete) coverage.
    """
    counts = {}
    for path in paths:
        try:
            y0, y1 = file_years(path)
        except ValueError:
            continue
        for year in range(y0, y1 + 1):
            counts[year] = counts.get(year, 0) + 1
    if not counts:
        return []
    covered = {y for y, n in counts.items() if n >= nrequired}
    missing = sorted(set(range(min(counts), max(counts) + 1)) - covered)

    gaps = []
    for year in missing:
        if gaps and year == gaps[-1][1] + 1:
            gaps[-1] = (gaps[-1][0], year)
        else:
            gaps.append((year, year))
    return gaps


def format_year_gaps(gaps):
    """``'336-340, 526-530'`` from a list of inclusive year ranges."""
    return ", ".join(
        str(a) if a == b else "{0}-{1}".format(a, b) for a, b in gaps
    )


def query_is1x1deg(ppname):
    """True if ``ppname`` looks like a 1x1-degree interpolated component."""
    return str(ppname).split("_")[-1] == "1x1deg"
