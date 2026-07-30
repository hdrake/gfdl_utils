"""Utilities for working with the GFDL filesystem from Python.

Subpackages are imported lazily, so ``import gfdl_utils.dmf`` (or
``gfdl_utils.paths``) does not drag in xarray.  ``gfdl_utils.core`` remains
available as the historical flat namespace, i.e. ``import gfdl_utils`` followed
by ``gfdl_utils.core.open_frompp(...)`` works as it always did.
"""

__version__ = "0.2.0"

_SUBMODULES = ("core", "dmf", "load", "mirror", "paths")


def __getattr__(name):
    if name in _SUBMODULES:
        import importlib

        module = importlib.import_module("." + name, __name__)
        globals()[name] = module
        return module
    raise AttributeError(
        "module {0!r} has no attribute {1!r}".format(__name__, name)
    )


def __dir__():
    return sorted(set(globals()) | set(_SUBMODULES))
