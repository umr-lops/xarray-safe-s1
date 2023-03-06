import numpy as np
import toolz


def is_scalar(x):
    return not toolz.itertoolz.isiterable(x) or isinstance(x, (str, bytes))


def is_composite_value(obj):
    if not isinstance(obj, list) or len(obj) not in [1, 2]:
        return False

    if any(list(el) != ["@dataStream", "$"] for el in obj):
        return False

    data_stream_values = [el["@dataStream"].lower() for el in obj]
    return data_stream_values in (["real", "imaginary"], ["magnitude"])


def is_complex(obj):
    return is_composite_value(obj) and len(obj) == 2


def is_magnitude(obj):
    return is_composite_value(obj) and len(obj) == 1


def is_array(obj):
    # definition of a array:
    # - list of scalars
    # - list of 1d lists
    # - complex array:
    #   - complex parts
    #   - list of complex values
    if not isinstance(obj, list):
        return False

    if len(obj) == 0:
        # zero-sized list, not sure what to do here
        return False

    elem = obj[0]
    if is_complex(obj):
        return not is_scalar(elem["$"])
    elif is_scalar(elem):
        return True
    elif isinstance(elem, list):
        if len(elem) == 1 and is_scalar(elem[0]):
            return True
        elif is_complex(elem):
            # array of imaginary values
            return True

    return False


def is_scalar_variable(obj):
    if not isinstance(obj, dict):
        return False

    if not all(is_scalar(v) for v in obj.values()):
        return False

    return all(k == "$" or k.startswith("@") for k in obj)


def is_nested(obj):
    """nested means: list of dict, but all dict values are scalar or 1-valued"""
    if not isinstance(obj, list) or len(obj) == 0:
        return False

    elem = obj[0]
    if not isinstance(elem, dict):
        return False

    if all(
        is_scalar(v) or (is_array(v) and len(v) == 1) or is_scalar_variable(v)
        for v in elem.values()
    ):
        return True

    return False


def is_nested_array(obj):
    return is_nested(obj) and "$" in obj[0]


def is_nested_dataset(obj):
    return is_nested(obj) and "$" not in obj[0]


def is_attr(column):
    """an attribute is a index if it has multiple unique values"""
    return np.unique(column).size == 1
