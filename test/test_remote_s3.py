"""
End-to-end read of a SAFE on S3, on a local fake S3 server (moto): no network, no credentials.

The test SAFE is uploaded to the fake bucket, read with an ``s3://`` URL, and compared
with the same SAFE read locally.
"""

import os
import pickle
import socket
import subprocess
import sys
import time

import numpy as np
import pytest

from safe_s1 import Sentinel1Reader, getconfig, sentinel1_xml_mappings

pytest.importorskip("moto.server")
pytest.importorskip("s3fs")
boto3 = pytest.importorskip("boto3")

BUCKET = "eodata"
PREFIX = "Sentinel-1/SAR"
KEYS = {"key": "testing", "secret": "testing"}
REGION = "us-east-1"
CHUNKS = {"line": 2000, "sample": 2000}


@pytest.fixture(scope="module")
def local_safe():
    name = getconfig.get_config()["product_paths"][0]
    return sentinel1_xml_mappings.get_test_file(name)


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def endpoint():
    """
    URL of a fake S3 server (moto), in its own process.

    Not a thread of the test process: `rasterio.open` keeps the GIL while GDAL
    makes its HTTP requests, which would block a server thread forever.
    """
    # the fake server is local: no proxy
    saved = {k: os.environ.get(k) for k in ("NO_PROXY", "no_proxy")}
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = "127.0.0.1,localhost"
    port = _free_port()
    server = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            pytest.fail("the moto server did not start")
        yield url
    finally:
        server.terminate()
        server.wait(timeout=30)
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@pytest.fixture(scope="module")
def s3_url(endpoint, local_safe):
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=KEYS["key"],
        aws_secret_access_key=KEYS["secret"],
        region_name=REGION,
    )
    client.create_bucket(Bucket=BUCKET)
    safe = os.path.basename(local_safe.rstrip("/"))
    for root, _, files in os.walk(local_safe):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), local_safe)
            client.upload_file(os.path.join(root, f), BUCKET, f"{PREFIX}/{safe}/{rel}")
    return f"s3://{BUCKET}/{PREFIX}/{safe}"


@pytest.fixture(scope="module")
def storage_options(endpoint):
    return dict(KEYS, client_kwargs={"endpoint_url": endpoint, "region_name": REGION})


@pytest.fixture(scope="module")
def local_reader(local_safe):
    return Sentinel1Reader(local_safe)


@pytest.fixture(scope="module")
def s3_reader(s3_url, storage_options):
    return Sentinel1Reader(s3_url, backend_kwargs={"storage_options": storage_options})


def assert_same_dn(local_ds, s3_ds):
    assert list(s3_ds.data_vars) == list(local_ds.data_vars)
    for var in local_ds.data_vars:
        assert s3_ds[var].dtype == local_ds[var].dtype
        np.testing.assert_array_equal(s3_ds[var].values, local_ds[var].values)
    for coord in ("pol", "line", "sample"):
        np.testing.assert_array_equal(s3_ds[coord].values, local_ds[coord].values)
    assert s3_ds.digital_number.attrs == local_ds.digital_number.attrs


def test_metadata(local_reader, s3_reader, s3_url):
    assert s3_reader.path == s3_url
    assert s3_reader.dsid == local_reader.dsid
    assert s3_reader.xsd_definitions == local_reader.xsd_definitions
    assert (
        s3_reader.dt["image"].to_dataset().equals(local_reader.dt["image"].to_dataset())
    )
    assert (
        s3_reader.dt["geolocationGrid"]
        .to_dataset()
        .equals(local_reader.dt["geolocationGrid"].to_dataset())
    )


def test_full_resolution(local_reader, s3_reader):
    window = dict(line=slice(1000, 3500), sample=slice(2000, 4500))
    _, local_ds = local_reader.load_digital_number(chunks=dict(CHUNKS))
    _, s3_ds = s3_reader.load_digital_number(chunks=dict(CHUNKS))
    assert s3_ds.digital_number.shape == local_ds.digital_number.shape
    assert_same_dn(local_ds.isel(window).compute(), s3_ds.isel(window).compute())


def test_full_resolution_graph_carries_the_s3_settings(local_reader, s3_reader):
    """The pickled graph is read without any GDAL setting in the process (dask processes / distributed)."""
    window = dict(line=slice(0, 1500), sample=slice(0, 1500))
    _, local_ds = local_reader.load_digital_number(chunks=dict(CHUNKS))
    _, s3_ds = s3_reader.load_digital_number(chunks=dict(CHUNKS))
    s3_ds = pickle.loads(pickle.dumps(s3_ds.isel(window)))
    assert_same_dn(
        local_ds.isel(window).compute(scheduler="synchronous"),
        s3_ds.compute(scheduler="synchronous"),
    )


def test_resampled(local_reader, s3_reader):
    # non-integer cell sizes use the same remote code path, see test_resampled_read*.py
    resolution = "1000m"
    local_res, local_ds = local_reader.load_digital_number(
        resolution=resolution, chunks=dict(CHUNKS)
    )
    s3_res, s3_ds = s3_reader.load_digital_number(
        resolution=resolution, chunks=dict(CHUNKS)
    )
    assert s3_res == local_res
    assert_same_dn(local_ds.compute(), s3_ds.compute())


def test_settings_from_environment(local_reader, s3_url, endpoint, monkeypatch):
    """Without storage_options (e.g. from xsar), s3fs and GDAL take the settings from the environment."""
    host_port = endpoint.split("://")[1]
    for name, value in {
        "AWS_ACCESS_KEY_ID": KEYS["key"],
        "AWS_SECRET_ACCESS_KEY": KEYS["secret"],
        "AWS_DEFAULT_REGION": REGION,
        "AWS_ENDPOINT_URL": endpoint,  # s3fs (botocore)
        "AWS_S3_ENDPOINT": host_port,  # GDAL
        "AWS_HTTPS": "NO",
        "AWS_VIRTUAL_HOSTING": "FALSE",
    }.items():
        monkeypatch.setenv(name, value)
    reader = Sentinel1Reader(s3_url)
    window = dict(line=slice(0, 1000), sample=slice(0, 1000))
    _, local_ds = local_reader.load_digital_number(chunks=dict(CHUNKS))
    _, s3_ds = reader.load_digital_number(chunks=dict(CHUNKS))
    assert_same_dn(local_ds.isel(window).compute(), s3_ds.isel(window).compute())
