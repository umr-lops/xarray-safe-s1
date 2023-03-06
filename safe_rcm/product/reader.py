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
        "/sourceAttributes": {
            "path": "/sourceAttributes",
            "f": converters.extract_metadata,
        },
        "/sourceAttributes/radarParameters": {
            "path": "/sourceAttributes/radarParameters",
            "f": transformers.extract_dataset,
        },
        "/sourceAttributes/radarParameters/prfInformation": {
            "path": "/sourceAttributes/radarParameters/prfInformation",
            "f": transformers.extract_nested_dataset,
        },
        "/sourceAttributes/orbitAndAttitude/orbitInformation": {
            "path": "/sourceAttributes/orbitAndAttitude/orbitInformation",
            "f": compose_left(
                curry(transformers.extract_dataset)(dims="timeStamp"),
                lambda ds: ds.assign_coords(
                    {"timeStamp": ds["timeStamp"].astype("datetime64")}
                ),
            ),
        },
        "/sourceAttributes/orbitAndAttitude/attitudeInformation": {
            "path": "/sourceAttributes/orbitAndAttitude/attitudeInformation",
            "f": compose_left(
                curry(transformers.extract_dataset)(dims="timeStamp"),
                lambda ds: ds.assign_coords(
                    {"timeStamp": ds["timeStamp"].astype("datetime64")}
                ),
            ),
        },
        "/imageReferenceAttributes": {
            "path": "/imageReferenceAttributes",
            "f": converters.extract_metadata,
        },
        "/imageReferenceAttributes/rasterAttributes": {
            "path": "/imageReferenceAttributes/rasterAttributes",
            "f": transformers.extract_dataset,
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
        "/sceneAttributes": {
            "path": "/sceneAttributes/imageAttributes",
            "f": compose_left(
                toolz.itertoolz.first,  # GRD datasets only have 1
                curry(toolz.dicttoolz.keyfilter)(lambda x: not x.startswith("@")),
                transformers.extract_dataset,
            ),
        },
    }

    converted = toolz.dicttoolz.valmap(
        lambda x: execute(**x)(decoded),
        layout,
    )
    return datatree.DataTree.from_dict(converted)
