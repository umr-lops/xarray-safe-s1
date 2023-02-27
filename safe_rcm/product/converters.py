import datatree
import toolz
import xarray as xr

from . import utils


def determine_indexes(columns, hint):
    if hint == "first" or isinstance(hint, list):
        if hint == "first":
            hint = [0]
        return [col for index, col in enumerate(columns) if index in hint]
    elif hint == "attribute":
        return [col.lstrip("@") for col in columns]
    else:
        raise ValueError(f"unknown index hint: {hint}")


def preprocess_names(mapping, namespaces):
    def preprocess(name):
        return utils.strip_namespaces(name, namespaces)

    return toolz.dicttoolz.keymap(preprocess, mapping)


def preprocess_variables(mapping, index_columns):
    def preprocess(col, dims):
        if not isinstance(col[0], dict):
            return (dims, col)

        merged = toolz.dicttoolz.merge_with(list, *col)
        attrs_, data = utils.split_marked(merged, marker="@")
        attrs = toolz.dicttoolz.valmap(toolz.first, attrs_)

        return (index_columns, data["$"], attrs)

    return {key: preprocess(col, index_columns) for key, col in mapping.items()}


def convert_table(table, *, namespaces={}, index_hint="first", dtypes={}):
    columns = toolz.dicttoolz.merge_with(list, *table)
    renamed = preprocess_names(columns, namespaces)
    indexes = determine_indexes(renamed, hint=index_hint)
    transformed = preprocess_variables(renamed, indexes)

    return (
        xr.Dataset(transformed)
        .assign(
            {name: lambda ds: ds[name].astype(dtype) for name, dtype in dtypes.items()}
        )
        .pipe(lambda obj: obj if list(obj) != ["$"] else obj["$"].rename(None))
    )


def metadata_filter(item, ignore):
    """filter metadata items

    Metadata items are either attributes (the name starts with an '@'), or they are scalars.
    """
    k, v = item

    return (k.startswith("@") or utils.is_scalar(v)) and k not in ignore


def extract_metadata(
    mapping,
    collapse=(),
    ignore=("@xmlns", "@xmlns:rcm", "@xmlns:xsi", "@xsi:schemaLocation"),
):
    # extract the metadata
    filter_ = toolz.functoolz.flip(metadata_filter, ignore)
    metadata = toolz.dicttoolz.keymap(
        lambda k: k.lstrip("@"), toolz.dicttoolz.itemfilter(filter_, mapping)
    )

    # collapse the selected items
    to_collapse = toolz.dicttoolz.keyfilter(lambda x: x in collapse, mapping)
    collapsed = dict(toolz.itertoolz.concat(v.items() for v in to_collapse.values()))

    attrs = metadata | collapsed
    return datatree.DataTree(xr.Dataset(attrs=attrs))
