import posixpath

import datatree
import numpy as np
import xarray as xr
from toolz.dicttoolz import itemmap, merge_with, valfilter, valmap
from toolz.functoolz import compose_left, curry, flip
from toolz.itertoolz import first

from safe_rcm.product.reader import execute

from .product.dicttoolz import keysplit
from .product.transformers import extract_dataset
from .xml import read_xml


def move_attrs_to_coords(ds, names):
    coords, attrs = keysplit(lambda k: k in names, ds.attrs)

    new = ds.copy()
    new.attrs = attrs

    return new.assign_coords(coords)


def pad_common(dss):
    def compute_padding(item, maximum):
        key, value = item

        return key, (0, maximum[key] - value)

    sizes = [dict(ds.sizes) for ds in dss]
    maximum_sizes = valmap(max, merge_with(list, *sizes))

    pad_widths = [itemmap(flip(compute_padding, maximum_sizes), _) for _ in sizes]

    return [
        ds.pad(padding, mode="constant", constant_values=np.nan)
        for ds, padding in zip(dss, pad_widths)
    ]


def _read_level(mapping):
    return (
        extract_dataset(mapping)
        .pipe(
            lambda ds: ds.swap_dims(
                {first(valfilter(lambda v: v > 1, ds.sizes)): "coefficients"}
            )
        )
        .pipe(lambda ds: ds.reset_coords())
        .pipe(
            move_attrs_to_coords,
            ["sarCalibrationType", "pixelFirstNoiseValue", "stepSize"],
        )
    )


def read_noise_level_file(mapper, path):
    layout = {
        "/referenceNoiseLevel": {
            "path": "/referenceNoiseLevel",
            "f": compose_left(
                curry(map, _read_level),
                curry(map, lambda ds: ds.expand_dims("sarCalibrationType")),
                list,
                curry(xr.combine_by_coords, combine_attrs="drop_conflicts"),
            ),
        },
        "/perBeamReferenceNoiseLevel": {
            "path": "/perBeamReferenceNoiseLevel",
            "f": compose_left(
                curry(map, _read_level),
                curry(map, lambda ds: ds.expand_dims("sarCalibrationType")),
                list,
                pad_common,
                curry(xr.combine_by_coords, combine_attrs="drop_conflicts"),
            ),
        },
        "/azimuthNoiseLevelScaling": {
            "path": "/azimuthNoiseLevelScaling",
            "f": compose_left(
                curry(map, _read_level),
                list,
                pad_common,
                curry(xr.combine_by_coords, combine_attrs="drop_conflicts"),
            ),
        },
    }

    decoded = read_xml(mapper, path)

    converted = valmap(lambda x: execute(**x)(decoded), layout)

    return converted


def read_noise_levels(mapper, root, fnames):
    fnames = fnames.data.tolist()
    paths = [posixpath.join(root, name) for name in fnames]

    poles = [path.removesuffix(".xml").split("_")[1] for path in paths]
    trees = [read_noise_level_file(mapper, path) for path in paths]
    merged = merge_with(list, *trees)
    combined = valmap(
        compose_left(
            curry(xr.concat, dim="pole", combine_attrs="no_conflicts"),
            lambda x: x.assign_coords(pole=poles),
        ),
        merged,
    )

    return datatree.DataTree.from_dict(combined)
