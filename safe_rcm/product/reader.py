import toolz
from lxml import etree

from ..schema import open_schema
from . import converters, utils
from .dicttoolz import query


def execute(f, path, kwargs={}):
    def inner(mapping):
        subset = query(path, mapping)

        return f(subset, **kwargs)

    return inner


def read_product(fs, product_url):
    tree = etree.fromstring(fs.cat(product_url))

    namespaces = toolz.dicttoolz.keymap(
        lambda x: x if x is not None else "rcm", tree.nsmap
    )
    schema_location = tree.xpath("./@xsi:schemaLocation", namespaces=namespaces)[0]
    _, schema_path = schema_location.split(" ")

    if not schema_path.startswith(".."):
        raise ValueError("schema path is absolute, the code can't handle that")

    root, _ = product_url.rsplit("/", maxsplit=1)
    schema_url = utils.absolute_url_path(f"{root}/{schema_path}")
    schema_root, schema_name = schema_url.rsplit("/", maxsplit=1)

    schema = open_schema(fs, schema_root, schema_name)

    decoded = schema.decode(tree)

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
