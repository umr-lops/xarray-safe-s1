#######################################################
xarray-safe-s1: xarray SAFE files reader for Sentinel-1
#######################################################

**safe_s1** is a SAR file reader



Documentation
-------------

Overview
........

    **safe_s1** reads Sentinel-1 SAFE products: metadata and luts are decoded from the xml
    files following `ESA Sentinel-1 Product Specification`_, and the *digital_number* are read
    from the measurement files with `rasterio`_ (GDAL), lazily with `dask`_ or resampled.

    The SAFE can be local, or remote on an S3-compatible object storage like the Copernicus
    Data Space Ecosystem: see :doc:`remote`.


    `safe_s1.metadata.Sentinel1reader` is the main class and contains a xarray.datatree with the useful data. In the following example, you will find some additional functions and properties that can be useful.

Examples
........

.. note::

    Those examples use `sentinel1_xml_mappings.get_test_file` to automatically download test data from https://cyclobs.ifremer.fr/static/sarwing_datarmor/xsardata/

    Those file are not official ones: they are resampled to a lower resolution and compressed to avoid big network transfert and disk usage.

    Don't use them for real science !

* :doc:`examples/simple_tutorial`
* :doc:`examples/resampled_read`
* :doc:`remote`


Reference
.........

* :doc:`api`

Get in touch
------------

- Report bugs, suggest features or view the source code `on github`_.

----------------------------------------------

Last documentation build: |today|

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   installing
   remote


.. toctree::
   :maxdepth: 1
   :caption: Examples

   examples/simple_tutorial
   examples/resampled_read


.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Reference

   api

.. _on github: https://github.com/umr-lops/xarray-safe-s1
.. _xarray: http://xarray.pydata.org
.. _dask: http://dask.org
.. _rasterio: https://rasterio.readthedocs.io/en/latest/
.. _xarray.open_rasterio: http://xarray.pydata.org/en/stable/generated/xarray.open_rasterio.html
.. _ESA Sentinel-1 Product Specification: https://earth.esa.int/documents/247904/1877131/Sentinel-1-Product-Specification
.. _xarray.Dataset: http://xarray.pydata.org/en/stable/generated/xarray.Dataset.html
.. _`recommended installation`: installing.rst#recommended-packages
.. _SAFE format: https://sentinel.esa.int/web/sentinel/user-guides/sentinel-1-sar/data-formats
.. _jupyter notebook: https://jupyter.readthedocs.io/en/latest/running.html#running
