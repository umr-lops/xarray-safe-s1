import posixpath
from urllib.parse import urlsplit, urlunsplit


def split_url(url):
    if url.count("::") > 1:
        # TODO: unlike urllib.parse, `fsspec` allows nested urls
        #       so we need to find a way to support that, as well
        raise ValueError("don't know how to deal with nested urls")

    return urlsplit(url)


def normalize_url_path(url):
    """convert the url's path component to absolute"""
    if url.count("::") > 1:
        # TODO: unlike urllib.parse, `fsspec` allows nested urls
        #       so we need to find a way to support that, as well
        raise ValueError("don't know how to deal with nested urls")

    split = urlsplit(url)
    normalized = posixpath.normpath(split.path)

    return urlunsplit(split._replace(path=normalized))


def dirname(url):
    split = split_url(url)
    new_path = posixpath.dirname(split.path)
    return urlunsplit(split._replace(path=new_path))


def join_path(url, path):
    split = split_url(url)
    if posixpath.isabs(path):
        joined_path = path
    else:
        joined_path = posixpath.normpath(posixpath.join(split.path, path))

    joined = split._replace(path=joined_path)

    return urlunsplit(joined)


def split(url):
    split = split_url(url)

    dirname, fname = posixpath.split(split.path)

    return urlunsplit(split._replace(path=dirname)), fname
