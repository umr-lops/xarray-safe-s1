"""Translation of fsspec URLs / storage options for GDAL, and lazy band reading."""

import pickle

import numpy as np
import pytest
import rasterio

from safe_s1 import remote


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/data/x.SAFE/measurement/a.tiff", "/data/x.SAFE/measurement/a.tiff"),
        ("C:\\data\\x.SAFE\\a.tiff", "C:\\data\\x.SAFE\\a.tiff"),
        ("file:///data/x.SAFE/a.tiff", "/data/x.SAFE/a.tiff"),
        ("s3://eodata/S1/x.SAFE/a.tiff", "/vsis3/eodata/S1/x.SAFE/a.tiff"),
        ("S3://eodata/a.tiff", "/vsis3/eodata/a.tiff"),
        ("s3a://bucket/a.tiff", "/vsis3/bucket/a.tiff"),
        ("gs://bucket/a.tiff", "/vsigs/bucket/a.tiff"),
        ("https://host/x.SAFE/a.tiff", "/vsicurl/https://host/x.SAFE/a.tiff"),
    ],
)
def test_gdal_path(path, expected):
    assert remote.gdal_path(path) == expected


def test_gdal_path_unsupported():
    with pytest.raises(NotImplementedError, match="memory://"):
        remote.gdal_path("memory://x.SAFE/a.tiff")


@pytest.mark.parametrize(
    "path, remote_",
    [("/data/x.SAFE", False), ("C:\\x.SAFE", False), ("s3://b/x.SAFE", True)],
)
def test_is_remote(path, remote_):
    assert remote.is_remote(path) is remote_


def test_env_options_local_and_empty():
    assert remote.gdal_env_options("/data/x.SAFE", {"key": "k"}) == {}
    assert remote.gdal_env_options("s3://b/x.SAFE") == {}
    assert remote.gdal_env_options("s3://b/x.SAFE", {}) == {}


def test_env_options_cdse_like():
    options = remote.gdal_env_options(
        "s3://eodata/x.SAFE",
        {
            "key": "my-key",
            "secret": "my-secret",
            "client_kwargs": {
                "endpoint_url": "https://eodata.dataspace.copernicus.eu",
                "region_name": "default",
            },
        },
    )
    session = options.pop("session")
    assert options == {
        "AWS_S3_ENDPOINT": "eodata.dataspace.copernicus.eu",
        "AWS_HTTPS": "YES",
        "AWS_VIRTUAL_HOSTING": "FALSE",
        "AWS_REGION": "default",
    }
    assert session.get_credential_options() == {
        "AWS_ACCESS_KEY_ID": "my-key",
        "AWS_SECRET_ACCESS_KEY": "my-secret",
    }
    # secrets are not shown, and the session travels with the dask tasks
    assert "my-secret" not in repr(session)
    assert pickle.loads(pickle.dumps(session)).get_credential_options() == (
        session.get_credential_options()
    )
    # accepted by rasterio
    with rasterio.Env(session=session, **options):
        assert rasterio.env.getenv()["AWS_ACCESS_KEY_ID"] == "my-key"


def test_env_options_keys_in_client_kwargs():
    options = remote.gdal_env_options(
        "s3://eodata/x.SAFE",
        {
            "client_kwargs": {
                "endpoint_url": "https://eodata.dataspace.copernicus.eu",
                "aws_access_key_id": "my-key",
                "aws_secret_access_key": "my-secret",
            },
        },
    )
    assert options["session"].get_credential_options() == {
        "AWS_ACCESS_KEY_ID": "my-key",
        "AWS_SECRET_ACCESS_KEY": "my-secret",
    }


def test_env_options_http_endpoint_anon_virtual():
    options = remote.gdal_env_options(
        "s3://b/x.SAFE",
        {
            "anon": True,
            "endpoint_url": "http://127.0.0.1:5000",
            "config_kwargs": {"s3": {"addressing_style": "virtual"}},
        },
    )
    session = options.pop("session")
    assert options == {
        "AWS_S3_ENDPOINT": "127.0.0.1:5000",
        "AWS_HTTPS": "NO",
        "AWS_VIRTUAL_HOSTING": "TRUE",
    }
    assert session.get_credential_options() == {"AWS_NO_SIGN_REQUEST": "YES"}


def test_env_options_bad_endpoint():
    with pytest.raises(ValueError, match="endpoint_url"):
        remote.gdal_env_options("s3://b/x.SAFE", {"endpoint_url": "host:5000"})


@pytest.fixture
def tiff(tmp_path):
    data = np.arange(300 * 200, dtype="uint16").reshape(300, 200)
    path = str(tmp_path / "band.tiff")
    with rasterio.open(
        path, "w", driver="GTiff", width=200, height=300, count=1, dtype="uint16"
    ) as dst:
        dst.write(data, 1)
    return path, data


def test_open_band(tiff):
    path, data = tiff
    band = remote.open_band(path, {}, (128, 64))
    assert band.shape == data.shape
    assert band.dtype == data.dtype
    assert band.chunks[0] == (128, 128, 44)
    np.testing.assert_array_equal(band.compute(scheduler="threads"), data)
    # the graph is picklable (dask processes / distributed)
    np.testing.assert_array_equal(
        pickle.loads(pickle.dumps(band[100:200, 50:150])).compute(
            scheduler="synchronous"
        ),
        data[100:200, 50:150],
    )
