import fsspec
import pandas as pd
import pytest

from safe_s1 import sentinel1_xml_mappings
from safe_s1.reader import Sentinel1Reader, split_dataset_name
from safe_s1.xml_parser import XmlParser

SAFE = "S1A_IW_GRDH_1SDV_20240418T080141_20240418T080210_053485_067D74_C073.SAFE"


@pytest.mark.parametrize(
    "name, expected",
    [
        # bare paths: the whole SAFE
        (f"/data/{SAFE}", (f"/data/{SAFE}", "")),
        (f"/data/{SAFE}/", (f"/data/{SAFE}", "")),
        (f"s3://eodata/S1/{SAFE}", (f"s3://eodata/S1/{SAFE}", "")),
        (f"C:\\data\\{SAFE}", (f"C:\\data\\{SAFE}", "")),
        # gdal-like names, with or without dataset id
        (f"SENTINEL1_DS:/data/{SAFE}:", (f"/data/{SAFE}", "")),
        (f"SENTINEL1_DS:/data/{SAFE}", (f"/data/{SAFE}", "")),
        (f"SENTINEL1_DS:/data/{SAFE}:IW1", (f"/data/{SAFE}", "IW1")),
        (f"SENTINEL1_DS:/data/{SAFE}:WV_012", (f"/data/{SAFE}", "WV_012")),
        (f"SENTINEL1_DS:/data/{SAFE}/:EW", (f"/data/{SAFE}", "EW")),
        (f"SENTINEL1_DS:s3://eodata/S1/{SAFE}:", (f"s3://eodata/S1/{SAFE}", "")),
        (f"SENTINEL1_DS:s3://eodata/S1/{SAFE}", (f"s3://eodata/S1/{SAFE}", "")),
        (f"SENTINEL1_DS:s3://eodata/S1/{SAFE}:IW2", (f"s3://eodata/S1/{SAFE}", "IW2")),
        (f"SENTINEL1_DS:C:\\data\\{SAFE}:S3", (f"C:\\data\\{SAFE}", "S3")),
        (f"SENTINEL1_DS:C:\\data\\{SAFE}", (f"C:\\data\\{SAFE}", "")),
    ],
)
def test_split_dataset_name(name, expected):
    assert split_dataset_name(name) == expected


def test_split_dataset_name_without_path():
    with pytest.raises(ValueError):
        split_dataset_name("SENTINEL1_DS::IW1")


XSD = b"""<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <xsd:complexType name="geolocationGridPointType">
    <xsd:sequence>
      <xsd:element name="slantRangeTime" type="xsd:double">
        <xsd:annotation><xsd:documentation>Two way slant range time [s].</xsd:documentation></xsd:annotation>
      </xsd:element>
      <xsd:element name="line" type="xsd:unsignedInt"/>
    </xsd:sequence>
  </xsd:complexType>
</xsd:schema>
"""


def reader_on(url, files):
    """Sentinel1Reader skeleton reading its xml files through the fsspec mapper of url"""
    fs = fsspec.filesystem("memory")
    for relpath, content in files.items():
        fs.pipe(f"{url}/{relpath}", content)
    reader = Sentinel1Reader.__new__(Sentinel1Reader)
    reader.path = url
    reader.manifest = "manifest.safe"
    reader.xml_parser = XmlParser(
        mapper=fsspec.get_mapper(url),
        xpath_mappings=sentinel1_xml_mappings.xpath_mappings,
        compounds_vars=sentinel1_xml_mappings.compounds_vars,
        namespaces=sentinel1_xml_mappings.namespaces,
    )
    reader.xml_parser.get_compound_var = lambda xml_file, var: pd.DataFrame(
        {
            "xsd_product": ["support/s1-level-1-product.xsd"]
        }  # normpath-ed by the mapping
    )
    return reader


def test_annotation_definitions_remote():
    reader = reader_on(
        f"memory://eodata/{SAFE}", {"support/s1-level-1-product.xsd": XSD}
    )
    definitions = reader.get_annotation_definitions()
    assert set(definitions) == {"slantRangeTime", "line"}
    assert "[s]" in str(definitions["slantRangeTime"])
    assert definitions["line"] is None


def test_annotation_definitions_missing_xsd():
    reader = reader_on(f"memory://eodata/no-xsd/{SAFE}", {})
    assert reader.get_annotation_definitions() == {}
