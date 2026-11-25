"""
Read measurement files (tiff) of a remote SAFE with GDAL.

The XML files are read through the fsspec mapper. GDAL does not understand fsspec URLs:
they are translated to GDAL virtual file system paths (``s3://b/k`` -> ``/vsis3/b/k``),
and the fsspec `storage_options` are translated to GDAL config options, given to GDAL
with `rasterio.Env`.

The GDAL options travel with the dask tasks: each block is read in its own
`rasterio.Env`, so that it works with any dask scheduler (threads or processes).
"""

import re
from urllib.parse import urlsplit

import dask.array
import numpy as np
import rasterio
import rasterio.session
import rasterio.windows

_PROTOCOL_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://(.*)$")

# fsspec protocol -> GDAL virtual file system prefix
_VSI_PREFIX = {
    "s3": "/vsis3/",
    "s3a": "/vsis3/",
    "gs": "/vsigs/",
    "gcs": "/vsigs/",
}


def get_protocol(path):
    """
    Protocol of a path: 'file' for a local path, else the URL scheme (like 's3').

    Windows drives (``C:\\x.SAFE``) are local paths.
    """
    match = _PROTOCOL_RE.match(str(path))
    return match.group(1).lower() if match else "file"


def is_remote(path):
    """True if `path` is an URL that is not a local file."""
    return get_protocol(path) != "file"


def gdal_path(path):
    """
    GDAL path of a local path or fsspec URL.

    Parameters
    ----------
    path: str
        local path, ``file://``, ``s3://``, ``gs://``, ``http://`` or ``https://`` URL

    Returns
    -------
    str
        local path, or GDAL virtual file system path (``/vsis3/bucket/key``, ``/vsicurl/https://...``)

    Raises
    ------
    NotImplementedError
        for other protocols (like ``memory://``), not readable by GDAL
    """
    path = str(path)
    match = _PROTOCOL_RE.match(path)
    if match is None:
        return path
    protocol, rest = match.group(1).lower(), match.group(2)
    if protocol == "file":
        return rest
    if protocol in _VSI_PREFIX:
        return _VSI_PREFIX[protocol] + rest
    if protocol in ("http", "https"):
        return "/vsicurl/" + path
    raise NotImplementedError(
        f"cannot read measurement files from {protocol}:// with GDAL "
        "(supported: local paths, s3://, gs://, http(s)://)"
    )


class GDALCredentials(rasterio.session.Session):
    """
    GDAL credential options (``AWS_ACCESS_KEY_ID``...) given as they are.

    rasterio only accepts credentials through a `rasterio.session.Session`
    (`rasterio.session.AWSSession` needs boto3, which is not a dependency).
    """

    def __init__(self, options):
        self._options = dict(options)

    @property
    def credentials(self):
        return dict(self._options)

    def get_credential_options(self):
        return dict(self._options)

    def __repr__(self):
        # never show the secrets
        return f"GDALCredentials({sorted(self._options)})"


def gdal_env_options(path, storage_options=None):
    """
    Keyword arguments of `rasterio.Env` to read `path` with the fsspec `storage_options`.

    Only S3 options are translated (s3fs names); for other protocols, and for the
    options not given, GDAL uses its usual sources (environment variables like
    ``AWS_ACCESS_KEY_ID``, ``AWS_S3_ENDPOINT``, ~/.aws files...).

    ======================================================== ========================================
    s3fs storage option                                      GDAL config option
    ======================================================== ========================================
    ``key``, ``secret``, ``token``                           ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_SESSION_TOKEN``
    (or ``client_kwargs["aws_access_key_id"]``...)
    ``anon=True``                                            ``AWS_NO_SIGN_REQUEST=YES``
    ``endpoint_url`` or ``client_kwargs["endpoint_url"]``     ``AWS_S3_ENDPOINT``, ``AWS_HTTPS``, ``AWS_VIRTUAL_HOSTING=FALSE``
    ``client_kwargs["region_name"]``                         ``AWS_REGION``
    ``config_kwargs["s3"]["addressing_style"]="virtual"``    ``AWS_VIRTUAL_HOSTING=TRUE``
    ======================================================== ========================================

    Parameters
    ----------
    path: str
        local path or fsspec URL
    storage_options: dict, optional
        fsspec storage options

    Returns
    -------
    dict
        keyword arguments for `rasterio.Env` (empty for a local path)
    """
    storage_options = storage_options or {}
    if get_protocol(path) not in ("s3", "s3a"):
        return {}
    client_kwargs = storage_options.get("client_kwargs") or {}
    options = {}
    credentials = {}

    for option, client_option, gdal_option in (
        ("key", "aws_access_key_id", "AWS_ACCESS_KEY_ID"),
        ("secret", "aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
        ("token", "aws_session_token", "AWS_SESSION_TOKEN"),
    ):
        # s3fs accepts the keys as its own options, or as botocore client options
        value = storage_options.get(option) or client_kwargs.get(client_option)
        if value:
            credentials[gdal_option] = value
    if storage_options.get("anon"):
        credentials["AWS_NO_SIGN_REQUEST"] = "YES"

    endpoint_url = storage_options.get("endpoint_url") or client_kwargs.get(
        "endpoint_url"
    )
    if endpoint_url:
        url = urlsplit(endpoint_url)
        if not url.netloc:
            raise ValueError(
                f"endpoint_url must be like https://host, got {endpoint_url!r}"
            )
        options["AWS_S3_ENDPOINT"] = url.netloc + url.path.rstrip("/")
        options["AWS_HTTPS"] = "NO" if url.scheme == "http" else "YES"
        # custom endpoints (CDSE, minio...) are generally path-style
        options["AWS_VIRTUAL_HOSTING"] = "FALSE"

    addressing_style = (
        (storage_options.get("config_kwargs") or {})
        .get("s3", {})
        .get("addressing_style")
    )
    if addressing_style == "virtual":
        options["AWS_VIRTUAL_HOSTING"] = "TRUE"
    elif addressing_style == "path":
        options["AWS_VIRTUAL_HOSTING"] = "FALSE"

    region = client_kwargs.get("region_name") or storage_options.get("region_name")
    if region:
        options["AWS_REGION"] = region

    if credentials:
        options["session"] = GDALCredentials(credentials)
    return options


def _read_block(path, env_options, dtype, block_info=None):
    """Read the block `block_info` of band 1 of `path`, in its own `rasterio.Env`."""
    (row_start, row_stop), (col_start, col_stop) = block_info[None]["array-location"]
    window = rasterio.windows.Window(
        col_start, row_start, col_stop - col_start, row_stop - row_start
    )
    with rasterio.Env(**env_options), rasterio.open(path) as src:
        return src.read(1, window=window, out_dtype=dtype)


def open_band(path, env_options, chunks):
    """
    Band 1 of `path` as a lazy dask array.

    Parameters
    ----------
    path: str
        GDAL path (see `gdal_path`)
    env_options: dict
        keyword arguments of `rasterio.Env` (see `gdal_env_options`)
    chunks: tuple
        (line, sample) chunks, as accepted by `dask.array.map_blocks`

    Returns
    -------
    dask.array.Array
        2D array (line, sample), with the dtype of the file
    """
    with rasterio.Env(**env_options), rasterio.open(path) as src:
        shape = (src.height, src.width)
        dtype = np.dtype(_numpy_dtype(src.dtypes[0]))
    chunks = dask.array.core.normalize_chunks(chunks, shape, dtype=dtype)
    return dask.array.map_blocks(
        _read_block,
        path,
        env_options,
        dtype,
        chunks=chunks,
        dtype=dtype,
        meta=np.empty((0, 0), dtype=dtype),
    )


def _numpy_dtype(rasterio_dtype):
    # complex_int16 (SLC) has no numpy equivalent: GDAL gives complex64
    return "complex64" if rasterio_dtype == "complex_int16" else rasterio_dtype
