"""Mirror archive files onto fast local scratch with ``gcp``.

Useful when the same files are read many times: pull them once onto
``/vftmp/$USER`` (or another prefix) and read from there.
"""

from __future__ import annotations

import getpass
import os
import shutil
import subprocess

__all__ = ["mirror_path", "mirrored_path", "DEFAULT_PREFIX"]

DEFAULT_PREFIX = "/vftmp/{0}".format(getpass.getuser())


def mirrored_path(path, prefix=DEFAULT_PREFIX):
    """Where ``path`` lands under ``prefix``."""
    return "{0}{1}".format(prefix, path).replace("//", "/")


def mirror_path(path, prefix=DEFAULT_PREFIX, overwrite=False, verbose=True,
                timeout=None):
    """Copy ``path`` under ``prefix``, skipping files already mirrored there.

    The previous implementation computed the list of files that still needed
    copying and then never used it, handing the *full* list to ``gcp`` every
    time — so the skip-existing optimisation was dead code.  It also derived a
    single destination directory from ``path[0]``, so a list spanning several
    source directories was flattened into one.  Both are fixed here: files are
    grouped by source directory and only the missing ones are copied.

    Parameters
    ----------
    path : str or iterable of str
        File(s) to mirror.
    prefix : str, default ``/vftmp/$USER``
        Root under which the source tree is recreated.
    overwrite : bool, default False
        Re-copy files that already exist at the destination.
    verbose : bool, default True
    timeout : float, optional
        Seconds to allow each ``gcp`` invocation.

    Returns
    -------
    list of str
        The mirrored paths, in the same order as ``path``.

    Raises
    ------
    RuntimeError
        If ``gcp`` is unavailable, exits non-zero, or a file is missing from
        the destination afterwards.
    """
    if isinstance(path, str):
        paths = [path]
    elif path is None:
        return []
    else:
        paths = [str(p) for p in path]
    if not paths:
        return []

    destinations = [mirrored_path(p, prefix) for p in paths]

    # Group by source directory so a list spanning several directories keeps
    # its structure instead of being collapsed into one.
    groups = {}
    for src, dst in zip(paths, destinations):
        if not overwrite and (os.path.isfile(dst) or os.path.isfile(dst + ".gcp")):
            continue
        groups.setdefault(os.path.dirname(dst), []).append(src)

    if not groups:
        if verbose:
            print("All {0} files already mirrored under '{1}'.".format(
                len(paths), prefix))
        return destinations

    if shutil.which("gcp") is None:
        raise RuntimeError(
            "'gcp' is not on PATH, so files cannot be mirrored from this host."
        )

    ncopy = sum(len(v) for v in groups.values())
    if verbose:
        print("Mirroring {0} of {1} files to '{2}' ({3} already present).".format(
            ncopy, len(paths), prefix, len(paths) - ncopy), flush=True)

    from .dmf import batched  # local import: keeps this module import-light

    for destdir, sources in sorted(groups.items()):
        os.makedirs(destdir, exist_ok=True)
        for batch in batched(sources):
            cmd = ["gcp"] + list(batch) + [destdir + "/"]
            proc = subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, timeout=timeout,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    "gcp failed (exit code {0}) copying {1} files to {2}:\n{3}".format(
                        proc.returncode, len(batch), destdir,
                        proc.stderr.strip()[:2000])
                )

    absent = [d for d in destinations if not os.path.isfile(d)]
    if absent:
        raise RuntimeError(
            "gcp reported success but {0} of {1} files are missing from the "
            "mirror, e.g.\n  {2}".format(len(absent), len(paths), absent[0])
        )
    if verbose:
        print("Mirroring complete.", flush=True)
    return destinations
