from importlib.metadata import version
from .metadata import Sentinel1Reader

try:
    __version__ = version("safe_s1")
except Exception:
    __version__ = "999"
