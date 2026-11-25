#############################
Reading a SAFE from S3
#############################

A SAFE can be read directly from an S3-compatible object storage, like the
Copernicus Data Space Ecosystem (CDSE) ``eodata`` bucket, without downloading it.

* the XML files (manifest, annotations, calibration, noise, XSD) are read with
  `fsspec <https://filesystem-spec.readthedocs.io>`_ (``s3fs`` must be installed);
* the measurement files (tiff) are read with GDAL, through its ``/vsis3/`` virtual
  file system: only the needed parts of the files are downloaded.

.. code-block:: python

    from safe_s1 import Sentinel1Reader

    storage_options = {
        "key": "<CDSE S3 access key>",
        "secret": "<CDSE S3 secret key>",
        "client_kwargs": {
            "endpoint_url": "https://eodata.dataspace.copernicus.eu",
            "region_name": "default",
        },
    }
    reader = Sentinel1Reader(
        "SENTINEL1_DS:s3://eodata/Sentinel-1/SAR/WV_SLC__1S/2026/11/16/"
        "S1C_WV_SLC__1SSV_20261116T000908_20261116T000926_010354_014AF4_903A.SAFE:WV_002",
        backend_kwargs={"storage_options": storage_options},
    )
    resolution, ds = reader.load_digital_number(chunks={"line": 2000, "sample": 2000})

The ``storage_options`` are the ``s3fs`` ones. They are translated to GDAL config
options for the measurement files:

======================================================== ===========================================================
s3fs storage option                                      GDAL config option
======================================================== ===========================================================
``key``, ``secret``, ``token``                           ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_SESSION_TOKEN``
``anon=True``                                            ``AWS_NO_SIGN_REQUEST=YES``
``endpoint_url`` or ``client_kwargs["endpoint_url"]``    ``AWS_S3_ENDPOINT``, ``AWS_HTTPS``, ``AWS_VIRTUAL_HOSTING=FALSE``
``client_kwargs["region_name"]``                         ``AWS_REGION``
``config_kwargs["s3"]["addressing_style"]``              ``AWS_VIRTUAL_HOSTING``
======================================================== ===========================================================

Without ``storage_options``, both ``s3fs`` and GDAL use their usual sources: the
environment variables (``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``...;
``AWS_S3_ENDPOINT`` for GDAL, ``AWS_ENDPOINT_URL`` for ``s3fs``) and the ``~/.aws`` files.

The GDAL options travel with the dask tasks: the digital numbers can be computed with
any dask scheduler (threads, processes, distributed).

Other protocols: ``gs://`` (``/vsigs/``) and ``http(s)://`` (``/vsicurl/``) are
translated too, with GDAL credentials from the environment. Filesystems that GDAL
cannot read (like ``memory://``) give a ``NotImplementedError`` when loading the
digital numbers.

.. warning::

    Never write your keys in a notebook or a script that is shared: read them from
    the environment or from a private configuration file.
