import traceback
from safe_s1.reader import Sentinel1Reader

try:
    from importlib import metadata
except ImportError:  # for Python<3.8
    import importlib_metadata as metadata
try:
    __version__ = metadata.version("xarray-safe-s1")
except Exception:
    print("trace", traceback.format_exc())
    __version__ = "999"
