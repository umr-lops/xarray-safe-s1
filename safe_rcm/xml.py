import toolz
import xmlschema
from lxml import etree

from . import fs_utils


def open_schema(fs, root, name, *, glob="*.xsd"):
    """fsspec-compatible way to open remote schema files

    Parameters
    ----------
    fs : fsspec.filesystem
        pre-instantiated fsspec filesystem instance
    root : str
        URL of the root directory of the schema files
    name : str
        File name of the schema to open.
    glob : str, default: "*.xsd"
        The glob used to find other schema files

    Returns
    -------
    xmlschema.XMLSchema
        The opened schema object
    """
    urls = sorted(
        fs.glob(f"{root}/{glob}"), key=lambda u: u.endswith(name), reverse=True
    )
    sources = [fs.open(u) for u in urls]

    return xmlschema.XMLSchema(sources)


def read_xml(fs, url):
    tree = etree.fromstring(fs.cat(url))

    namespaces = toolz.dicttoolz.keymap(
        lambda x: x if x is not None else "rcm", tree.nsmap
    )
    schema_location = tree.xpath("./@xsi:schemaLocation", namespaces=namespaces)[0]
    _, schema_path = schema_location.split(" ")

    schema_url = fs_utils.urljoin(url, schema_path)
    schema_root, schema_name = fs_utils.split(schema_url)

    schema = open_schema(fs, schema_root, schema_name)

    decoded = schema.decode(tree)

    return decoded
