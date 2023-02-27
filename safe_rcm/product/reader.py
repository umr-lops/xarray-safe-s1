import toolz

from ..xml import read_xml
from . import converters
from .dicttoolz import query


def execute(f, path, kwargs={}):
    def inner(mapping):
        subset = query(path, mapping)

        return f(subset, **kwargs)

    return inner


def read_product(fs, product_url):
    decoded = read_xml(fs, product_url)

    layout = {
        "/": {
            "path": "/",
            "f": converters.extract_metadata,
            "kwargs": {"collapse": ["securityAttributes"]},
        },
    }

    converted = toolz.dicttoolz.valmap(
        lambda x: execute(**x)(decoded),
        layout,
    )
    return converted
