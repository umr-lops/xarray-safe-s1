import toolz


def query(path, mapping):
    if path == "/":
        return mapping

    keys = path.lstrip("/").split("/")
    return toolz.dicttoolz.get_in(keys, mapping, no_default=True)


def itemsplit(predicate, d):
    groups = toolz.itertoolz.groupby(predicate, d.items())
    first = dict(groups.get(True, ()))
    second = dict(groups.get(False, ()))

    return first, second


def valsplit(predicate, d):
    wrapper = lambda item: predicate(item[1])
    return itemsplit(wrapper, d)


def keysplit(predicate, d):
    wrapper = lambda item: predicate(item[0])
    return itemsplit(wrapper, d)
