# gfdl_utils

A collection of codes for interaction with the GFDL filesystem.

```python
import gfdl_utils.core as gu

pp = "/archive/.../pp"
local = gu.get_local(pp, "ocean_annual", "ts")        # -> 'annual/5yr'

# What would this cost? (no data is read)
paths = gu.get_pathspp_list(pp, "ocean_annual", "ts", local, "*", "thetao",
                            years=(1180, 1680))
print(gu.status_report(paths).summary())
# 100 files, 1.17 TB total [93 OFL, 7 DUL]; 93 offline (1.09 TB)

# Open the last 500 years, recalling from tape with progress reporting.
ds = gu.open_frompp(pp, "ocean_annual", "ts", local, "*", "thetao",
                    years=(1180, 1680), dmget=True, max_gb=1500)
```

## Layout

| module | responsibility |
| --- | --- |
| `gfdl_utils.dmf` | DMF/tape: `dmls`/`dmget`, batched and honest about failure |
| `gfdl_utils.paths` | path construction, discovery, filename parsing, caching, gap detection |
| `gfdl_utils.load` | opening post-processed output with xarray |
| `gfdl_utils.mirror` | copying archive files to local scratch with `gcp` |
| `gfdl_utils.core` | backwards-compatible flat namespace re-exporting all of the above |

`import gfdl_utils.core as gu` continues to work exactly as before; the
submodules are for people who want only part of the package (importing
`gfdl_utils.dmf` or `gfdl_utils.paths` does not pull in xarray).

## Working with tape

`/archive` is tape-backed. Reading a file that is not resident on disk blocks —
often for a long time — with no output while DMF recalls it. The tape layer
makes that visible:

```python
from gfdl_utils import dmf

report = dmf.status_report(paths)      # one `dmls` per 500 paths, not per path
report.summary()
# '932 files, 1.17 TB total [900 OFL, 24 UNM, 8 DUL]; 924 offline (1.16 TB)'
report.offline, report.inflight, report.missing

dmf.dmget(paths, max_gb=2000)          # blocking recall with progress reporting
dmf.ensure_ondisk(paths)               # raise a clear error if anything is offline

for path in dmf.iter_online(paths):    # stream: files are yielded as they land
    process(path)
```

DMF states are reported individually rather than as a boolean: `REG`/`DUL`/
`MIG`/`NMG` (and `N/A` on non-DMF filesystems) are readable now, `UNM` means a
recall is in flight, `OFL` means tape only, `PAR`/`INV` mean partial or
inconsistent.

`dmf.dmget` refuses requests larger than `max_gb` (default 100 GB) unless you
raise the limit or pass `force=True`. GFDL asks that you confirm before
recalling more than 50 files or 100 GB.

On a host with no DMF client tools, everything that exists is reported as
readable, so the package works unchanged off the analysis nodes.

## Not opening files you do not need

`get_pathspp_list(..., years=(first, last))` parses the `<ppname>.<time>.<var>.nc`
filenames and drops whole files that fall outside the window, so they are never
opened and never recalled from tape. `open_frompp(..., years=...)` does the
same. This is the single biggest speed-up for taking a sub-period out of a long
control run.

Missing post-processing is reported rather than silently concatenated over:

```python
gu.find_year_gaps(paths)     # [(336, 340), (526, 530), (536, 540)]
```

Directory listings are cached (these directories routinely hold 15k+ entries and
are listed once per variable); call `gu.clear_cache()` if new output has been
written since.

## Testing

```
pip install -e .[test]
pytest
```

The tests never invoke the real `dmls`/`dmget`; the subprocess layer is stubbed
with a fake archive.

## Changes in 0.2.0

`gfdl_utils.core` still exports every name it did in 0.1.x, but some behaviour
changed. In rough order of how likely you are to notice:

* **`open_frompp` and `open_static` now raise `OfflineDataError` when files are
  on tape and `dmget=False`**, instead of handing them to `open_mfdataset` and
  blocking. Pass `dmget=True` to recall them, or `on_offline='ignore'` for the
  old behaviour (`on_offline='warn'` splits the difference). The module default
  is `gfdl_utils.load.DEFAULT_ON_OFFLINE`.
* **`dmget` requests larger than 100 GB now raise** unless `max_gb=` is raised
  or `force=True` is passed.
* **`issue_dmget` no longer backgrounds with `os.system(... &)`.** It batches
  the paths, bounds concurrency, waits by default (`wait=False` restores
  fire-and-forget), and raises `DMFError` on a non-zero exit instead of
  returning the backgrounding shell's status of 0.
* **`decode_timedelta=False` is now passed to xarray by default**, so variables
  with time-like units (`average_DT` and friends) come back as plain numbers.
  This matches the default xarray is moving to; set
  `gfdl_utils.load.DEFAULT_DECODE_TIMEDELTA = None` to defer to xarray.
* **cftime (object-dtype) variables such as `time_bnds` are loaded eagerly.**
  They are tiny, and leaving them dask-backed makes any later `concat` against
  an in-memory dataset fail with `NotImplementedError: Can not use auto
  rechunking with object dtype`. Disable with `load_cftime=False`.
* **`get_local` no longer falls back to `os.listdir(...)[-1]`** when the
  requested `local1priority`/`local2priority` is absent — that picked whichever
  directory the filesystem happened to return last. It now prefers a documented
  frequency order, and picks the chunk length that actually covers the most
  years, warning and listing the alternatives. `strict=True` raises instead.
* **`get_ppnames` returns only directories**, and every listing is sorted.
* **`mirror_path` actually skips already-mirrored files** (the previous version
  computed the list and then copied everything anyway) and groups by source
  directory, so a list spanning several directories is no longer flattened into
  one. It raises on `gcp` failure instead of ignoring the exit code.
* `query_ondisk` accepts a list, batches its `dmls` calls, and parses paths
  containing spaces. `wait_until_ondisk` polls with one batched `dmls` per
  iteration and reports DMF state counts.
