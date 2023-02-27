import functools
import string

import hypothesis.strategies as st
from hypothesis import given

from safe_rcm.product import utils


def shared(*, key):
    def outer(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            result = func(*args, **kwargs)

            return st.shared(result, key=key)

        return inner

    return outer


markers = st.characters()
marker = st.shared(markers, key="marker")


def marked_mapping(marker):
    values = st.just(None)

    unmarked_keys = st.text()
    marked_keys = st.builds(lambda k, m: m + k, unmarked_keys, marker)
    keys = st.one_of(unmarked_keys, marked_keys)

    return st.dictionaries(keys, values)


@given(marked_mapping(marker), marker)
def test_split_marked(mapping, marker):
    marked, unmarked = utils.split_marked(mapping, marker=marker)

    assert list(unmarked) == [key for key in mapping if not key.startswith(marker)]


@shared(key="namespaces")
def namespaces():
    values = st.just(None)

    keys = st.text(string.ascii_letters, min_size=1, max_size=4)
    return st.dictionaries(keys, values)


@st.composite
def draw_from(draw, elements):
    elements = draw(elements)

    if not elements:
        return ""

    return draw(st.sampled_from(elements))


def prefixed_names(namespaces):
    def builder(base, prefix):
        return f"{prefix}:{base}" if prefix != "" else base

    bases = st.text(string.ascii_letters, min_size=1)
    all_prefixes = namespaces.map(list)
    prefixes = draw_from(all_prefixes)

    return st.builds(builder, bases, prefixes)


@given(prefixed_names(namespaces()), namespaces())
def test_strip_namespaces(name, namespaces):
    stripped = utils.strip_namespaces(name, namespaces)

    assert ":" not in stripped
