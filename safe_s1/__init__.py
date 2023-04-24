from importlib.metadata import version

try:
    __version__ = version("safe_s1")
except Exception:
    __version__ = "999"
