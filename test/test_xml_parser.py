import pickle

from safe_s1.xml_parser import XmlParser

XML = b"""<?xml version="1.0"?>
<product xmlns:s1="http://www.esa.int/safe/sentinel-1.0">
  <s1:mission>S1A</s1:mission>
  <pixel>10.5</pixel>
</product>
"""


class CountingMapper(dict):
    """dict mapper counting file reads, like an fsspec mapper"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reads = 0

    def __getitem__(self, key):
        self.reads += 1
        return super().__getitem__(key)


def first_float(values):
    return float(values[0])


def make_parser(**kwargs):
    mapper = CountingMapper({"annotation.xml": XML})
    parser = XmlParser(
        mapper,
        xpath_mappings={"annotation": {"pixel": (first_float, "/product/pixel")}},
        namespaces={},
        **kwargs,
    )
    return mapper, parser


def test_xml_parsed_once():
    mapper, parser = make_parser()
    for _ in range(3):
        assert parser.get_var("annotation.xml", "annotation.pixel") == 10.5
        assert parser.xpath("annotation.xml", "/product/s1:mission") == ["S1A"]
    assert mapper.reads == 1
    assert parser._namespaces["s1"] == "http://www.esa.int/safe/sentinel-1.0"


def test_clear_cache():
    mapper, parser = make_parser()
    parser.getroot("annotation.xml")
    parser.clear_cache()
    parser.getroot("annotation.xml")
    assert mapper.reads == 2


def test_cache_disabled():
    mapper, parser = make_parser(cache=False)
    parser.getroot("annotation.xml")
    parser.getroot("annotation.xml")
    assert mapper.reads == 2


def test_pickle_drops_parsed_roots():
    _, parser = make_parser()
    parser.getroot("annotation.xml")
    clone = pickle.loads(pickle.dumps(parser))
    assert clone._roots == {}
    assert parser._roots  # the original keeps its cache
    assert clone.get_var("annotation.xml", "annotation.pixel") == 10.5
