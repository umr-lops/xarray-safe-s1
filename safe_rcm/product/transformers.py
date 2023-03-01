import datatree
import toolz
import xarray as xr

from .dicttoolz import keysplit, valsplit
from .predicates import is_array, is_scalar


def extract_variable(obj, dims=()):
    attributes, data = keysplit(lambda k: k.startswith("@"), obj)
    if list(data) != ["$"]:
        raise ValueError("not a variable")

    values = data["$"]
    if is_scalar(values):
        dims = ()

    attrs = toolz.dicttoolz.keymap(lambda k: k.lstrip("@"), attributes)

    return xr.Variable(dims, values, attrs)


def extract_dataset(obj, dims=()):
    attrs, variables = valsplit(is_scalar, obj)

    vars_ = toolz.dicttoolz.valmap(
        toolz.functoolz.curry(extract_variable)(dims=dims), variables
    )
    return xr.Dataset(data_vars=vars_, attrs=attrs)


def extract_nested_variable(obj, dims):
    if is_array(obj):
        return xr.Variable(dims, obj)

    columns = toolz.dicttoolz.merge_with(list, *obj)
    attributes, data = keysplit(lambda k: k.startswith("@"), columns)
    renamed = toolz.dicttoolz.keymap(lambda k: k.lstrip("@"), attributes)
    attrs = toolz.dicttoolz.valmap(toolz.itertoolz.first, renamed)

    return xr.Variable(dims, data["$"], attrs)


def extract_nested_dataset(obj, dims=None):
    if not isinstance(obj, list):
        raise ValueError(f"unknown type: {type(obj)}")

    columns = toolz.dicttoolz.merge_with(list, *obj)
    processed = toolz.dicttoolz.valmap(
        toolz.functoolz.curry(extract_nested_variable)(dims=dims), columns
    )

    return xr.Dataset(processed)


def extract_nested_datatree(obj, dims=None):
    if not isinstance(obj, list):
        raise ValueError(f"unknown type: {type(obj)}")

    datasets = toolz.dicttoolz.merge_with(list, *obj)

    tree = toolz.dicttoolz.valmap(
        toolz.functoolz.curry(extract_nested_dataset)(dims=dims), datasets
    )

    return datatree.DataTree.from_dict(tree)
