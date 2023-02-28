import pytest

from safe_rcm import fs_utils

# TODO: find properties that allow us to use `hypothesis`


@pytest.mark.parametrize(
    ["url", "expected"],
    (
        pytest.param("file:///a/b.xml", ("file:///a", "b.xml"), id="local"),
        pytest.param(
            "http://host:9754/a/b.xml", ("http://host:9754/a", "b.xml"), id="http"
        ),
    ),
)
def test_split(url, expected):
    actual = fs_utils.split(url)

    assert actual == expected
