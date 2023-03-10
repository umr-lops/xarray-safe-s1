import os
import posixpath

import datatree
import fsspec
import xarray as xr
from toolz.dicttoolz import valmap
from toolz.functoolz import compose_left, curry

from .product.reader import read_product
from .product.transformers import extract_dataset
from .xml import read_xml


@curry
def execute(tree, f, path):
    node = tree[path]

    return f(node)


def open_rcm(url, *, backend_kwargs=None, **dataset_kwargs):
    if not isinstance(url, (str, os.PathLike)):
        raise ValueError(f"cannot deal with object of type {type(url)}: {url}")

    if backend_kwargs is None:
        backend_kwargs = {}

    url = os.fspath(url)

    storage_options = backend_kwargs.get("storage_options", {})
    mapper = fsspec.get_mapper(url, **storage_options)

    tree = read_product(mapper, "metadata/product.xml")

    calibration_root = "metadata/calibration"
    lookup_table_structure = {
        "/incidenceAngles": {
            "path": "/imageReferenceAttributes",
            "f": compose_left(
                lambda obj: obj.attrs["incidenceAngleFileName"],
                lambda p: posixpath.join(calibration_root, p),
                curry(read_xml)(mapper),
                extract_dataset,
            ),
        }
    }
    calibration = valmap(
        lambda x: execute(**x)(tree),
        lookup_table_structure,
    )

    imagery_paths = tree["/sceneAttributes/ipdf"].to_series().to_dict()
    resolved = valmap(
        compose_left(
            lambda p: posixpath.join("metadata", p),
            posixpath.normpath,
        ),
        imagery_paths,
    )
    imagery_urls = valmap(
        mapper._key_to_str,
        resolved,
    )
    imagery_dss = valmap(
        compose_left(
            curry(mapper.fs.open),
            curry(xr.open_dataset)(engine="rasterio", chunks={}),
        ),
        imagery_urls,
    )
    dss = [ds.assign_coords(pole=coord) for coord, ds in imagery_dss.items()]
    imagery = xr.concat(dss, dim="pole")

    return tree.assign(
        {
            "lookupTables": datatree.DataTree.from_dict(calibration),
            "imagery": datatree.DataTree(imagery),
        }
    )
