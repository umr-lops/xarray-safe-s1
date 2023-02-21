from importlib.metadata import version

try:
    __version__ = version("safe_rcm")
except Exception:
    __version__ = "999"
