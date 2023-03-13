import os
import posixpath

import datatree
import fsspec
import xarray as xr
from toolz.dicttoolz import valmap
from toolz.functoolz import compose_left, curry, juxt

from .product.reader import read_product
from .product.transformers import extract_dataset
from .xml import read_xml


@curry
def execute(tree, f, path):
    node = tree[path]

    return f(node)


@curry
def starcall(f, args):
    return f(*args)


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
                curry(extract_dataset)(dims="coefficients"),
            ),
        },
        "/lookupTables": {
            "path": "/imageReferenceAttributes/lookupTableFileName",
            "f": compose_left(
                lambda obj: obj.stack(stacked=["sarCalibrationType", "pole"]),
                lambda obj: obj.reset_index("stacked"),
                juxt(
                    compose_left(
                        lambda obj: obj.to_series().to_dict(),
                        curry(valmap)(lambda p: posixpath.join(calibration_root, p)),
                        curry(valmap)(curry(read_xml)(mapper)),
                        curry(valmap)(curry(extract_dataset)(dims="coefficients")),
                        curry(valmap)(lambda ds: ds["gains"].assign_attrs(ds.attrs)),
                        lambda d: xr.concat(list(d.values()), dim="stacked"),
                    ),
                    lambda obj: obj.coords,
                ),
                starcall(lambda arr, coords: arr.assign_coords(coords)),
                lambda arr: arr.set_index({"stacked": ["sarCalibrationType", "pole"]}),
                lambda arr: arr.unstack("stacked"),
                lambda arr: arr.rename("lookup_tables"),
            ),
        },
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
