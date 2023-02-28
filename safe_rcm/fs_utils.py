import posixpath
from urllib.parse import urljoin  # noqa: F401
from urllib.parse import urlsplit, urlunsplit


def split_url(url):
    if url.count("::") != 0:
        # TODO: unlike urllib.parse, `fsspec` allows chaining urls
        #       so we need to find a way to support that, as well
        raise ValueError("don't know how to deal with url chains")

    return urlsplit(url)


def split(url):
    split = split_url(url)

    dirname, fname = posixpath.split(split.path)

    return urlunsplit(split._replace(path=dirname)), fname
