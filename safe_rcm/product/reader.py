import datatree
import toolz
import xarray as xr
from toolz.functoolz import compose_left, curry

from ..xml import read_xml
from . import converters, transformers
from .dicttoolz import query


@curry
def attach_path(obj, path):
    if not hasattr(obj, "encoding"):
        raise ValueError(
            "cannot attach source path: `obj` does not have a `encoding` attribute."
        )

    new = obj.copy()
    new.encoding["xpath"] = path

    return new


@curry
def execute(mapping, f, path):
    subset = query(path, mapping)

    return compose_left(f, attach_path(path=path))(subset)


@curry
def convert(converters, item):
    key, value = item
    converter = converters.get(key, lambda x: x)
    return key, converter(value)


def read_product(fs, product_url):
    decoded = read_xml(fs, product_url)

    layout = {
        "/": {
            "path": "/",
            "f": curry(converters.extract_metadata)(collapse=["securityAttributes"]),
        },
        "/imageReferenceAttributes": {
            "path": "/imageReferenceAttributes",
            "f": converters.extract_metadata,
        },
        "/imageReferenceAttributes/geographicInformation/ellipsoidParameters": {
            "path": "/imageReferenceAttributes/geographicInformation/ellipsoidParameters",
            "f": curry(transformers.extract_dataset)(dims="params"),
        },
        "/imageReferenceAttributes/geographicInformation/geolocationGrid": {
            "path": "/imageReferenceAttributes/geographicInformation/geolocationGrid/imageTiePoint",
            "f": compose_left(
                curry(transformers.extract_nested_datatree)(dims="tie_points"),
                lambda tree: xr.merge([node.ds for node in tree.subtree]),
                lambda ds: ds.set_index(tie_points=["line", "pixel"]),
                lambda ds: ds.unstack("tie_points"),
            ),
        },
        "/imageReferenceAttributes/geographicInformation/rationalFunctions": {
            "path": "/imageReferenceAttributes/geographicInformation/rationalFunctions",
            "f": curry(transformers.extract_dataset)(dims="coefficients"),
        },
    }

    converted = toolz.dicttoolz.valmap(
        lambda x: execute(**x)(decoded),
        layout,
    )
    return datatree.DataTree.from_dict(converted)
