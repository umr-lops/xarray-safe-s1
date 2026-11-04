"""End-to-end tests of the resampled read, on the test product used by the CI.

S1A IW GRDH 2017-09-07, 16 778 lines x 25 187 samples of 10 m, VV and VH.
It starts with 5 lines and 5 samples of zeros (outside the swath), so at 1000 m
(cells of 100 x 100 pixels) the first row and the first column of cells are 95 %
in the swath, and the corner cell 95 x 95 / 100 x 100 = 90.25 %.

The cells read by `load_digital_number` are compared with the pixels of the
measurement files, recomputed with numpy."""

import os

import numpy as np
import pytest
import rasterio
from rasterio.windows import Window

from safe_s1 import Sentinel1Reader, getconfig, sentinel1_xml_mappings

CHUNKS = {"line": 5000, "sample": 5000}
CELL = 100  # pixels per cell at 1000 m


@pytest.fixture(scope="module")
def reader():
    name = getconfig.get_config()["product_paths"][0]
    return Sentinel1Reader(sentinel1_xml_mappings.get_test_file(name))


@pytest.fixture(scope="module")
def measurement(reader):
    """Measurement file of each polarisation."""
    files = [os.path.join(reader.path, f) for f in reader.files["measurement"]]
    pols = [str(p) for p in reader.manifest_attrs["polarizations"]]
    return dict(zip(pols, files))


@pytest.fixture(scope="module")
def at_1000m(reader):
    return reader.load_digital_number(resolution="1000m", chunks=dict(CHUNKS))


def pixels_of_cell(path, i, j):
    """The 100 x 100 full resolution pixels of cell (i, j)."""
    with rasterio.open(path) as src:
        return src.read(1, window=Window(CELL * j, CELL * i, CELL, CELL))


def rms_of_valid_pixels(pixels):
    valid = pixels[pixels > 0].astype(np.float64)
    return np.sqrt(np.mean(valid**2)) if valid.size else 0


def test_full_resolution_is_the_file_itself(reader, measurement):
    """Without resolution, the pixels are the ones of the file, in uint16, no valid_fraction."""
    res, ds = reader.load_digital_number(resolution=None, chunks=dict(CHUNKS))

    assert res is None
    assert "valid_fraction" not in ds
    assert ds.sizes["line"] == 16778
    assert ds.sizes["sample"] == 25187
    assert ds.digital_number.dtype == np.uint16
    with rasterio.open(measurement["VV"]) as src:
        pixels = src.read(1, window=Window(1000, 2000, 50, 50))
    np.testing.assert_array_equal(
        ds.digital_number.sel(
            pol="VV", line=slice(2000, 2049), sample=slice(1000, 1049)
        ),
        pixels,
    )


def test_grid_at_1000m(at_1000m):
    """16 778 x 25 187 pixels at 1000 m: 167 x 251 whole cells of 100 x 100 pixels.

    Cell coordinates are the cell centers: 49.5 for the first cell, and
    166 x 100 + 49.5 = 16 649.5 (line), 250 x 100 + 49.5 = 25 049.5 (sample) for the last.
    The 78 last lines and 87 last samples (less than one cell) are not used."""
    res, ds = at_1000m

    assert res == 1000
    assert ds.sizes["line"] == 167
    assert ds.sizes["sample"] == 251
    assert ds.line[0] == 49.5
    assert ds.line[-1] == 16649.5
    assert ds.sample[0] == 49.5
    assert ds.sample[-1] == 25049.5
    assert ds.digital_number.dtype == np.float32
    assert ds.valid_fraction.dtype == np.float32


def test_valid_fraction_along_the_zeros_at_the_image_start(at_1000m):
    """5 lines and 5 samples of zeros at the start of the image:
    corner cell 90.25 % valid, rest of the first row and column 95 %, all other cells 100 %.
    """
    _, ds = at_1000m
    fraction = ds.valid_fraction.values

    np.testing.assert_allclose(fraction[0, 0], 0.9025, atol=1e-6)
    np.testing.assert_allclose(fraction[0, 1:], 0.95, atol=1e-6)
    np.testing.assert_allclose(fraction[1:, 0], 0.95, atol=1e-6)
    np.testing.assert_allclose(fraction[1:, 1:], 1, atol=1e-6)


def test_three_cells_checked_by_hand(at_1000m):
    """Three cells of the scene, with their values (rms of the valid pixels):

    * corner cell (0, 0), 90.25 % valid (the zeros are not counted): VV 326.24, VH 64.74;
    * cell in the middle of the scene (83, 125): VV 225.72, VH 66.42;
    * last cell (166, 250), pixels 16 600 to 16 699 x 25 000 to 25 099:
      VV 204.23, VH 106.92."""
    _, ds = at_1000m
    dn = ds.digital_number

    np.testing.assert_allclose(dn.sel(pol="VV")[0, 0], 326.24, atol=0.01)
    np.testing.assert_allclose(dn.sel(pol="VH")[0, 0], 64.74, atol=0.01)
    np.testing.assert_allclose(dn.sel(pol="VV")[83, 125], 225.72, atol=0.01)
    np.testing.assert_allclose(dn.sel(pol="VH")[83, 125], 66.42, atol=0.01)
    np.testing.assert_allclose(dn.sel(pol="VV")[166, 250], 204.23, atol=0.01)
    np.testing.assert_allclose(dn.sel(pol="VH")[166, 250], 106.92, atol=0.01)


def test_any_cell_is_the_rms_of_its_own_pixels(at_1000m, measurement):
    """Same check on the borders of the scene and on 20 cells taken at random:
    each cell is the rms of the non-zero pixels it covers, and valid_fraction is
    the fraction of these pixels."""
    _, ds = at_1000m
    n = ds.sizes["line"]
    m = ds.sizes["sample"]
    rng = np.random.default_rng(0)
    cells = [(0, 0), (0, m - 1), (n - 1, 0), (n - 1, m - 1), (0, m // 2), (n // 2, 0)]
    cells += [(rng.integers(n), rng.integers(m)) for _ in range(20)]
    fraction = ds.valid_fraction.values

    for pol, path in measurement.items():
        dn = ds.digital_number.sel(pol=pol).values
        for i, j in cells:
            pixels = pixels_of_cell(path, i, j)
            np.testing.assert_allclose(
                dn[i, j],
                rms_of_valid_pixels(pixels),
                rtol=1e-5,
                err_msg=f"{pol} cell {(i, j)}",
            )
            np.testing.assert_allclose(
                fraction[i, j], np.mean(pixels > 0), atol=1e-6, err_msg=f"cell {(i, j)}"
            )


@pytest.mark.parametrize(
    "resolution",
    [1000, {"line": 100, "sample": 100}],
    ids=["1000_is_in_meters", "dict_is_in_pixels"],
)
def test_1000m_can_be_written_three_ways(reader, at_1000m, resolution):
    """ "1000m", 1000 (meters) and {"line": 100, "sample": 100} (pixels) give the same cells."""
    _, expected = at_1000m
    res, ds = reader.load_digital_number(resolution=resolution, chunks=dict(CHUNKS))

    assert res is None
    np.testing.assert_array_equal(ds.line, expected.line)
    np.testing.assert_array_equal(ds.sample, expected.sample)
    np.testing.assert_array_equal(ds.digital_number, expected.digital_number)
    np.testing.assert_array_equal(ds.valid_fraction, expected.valid_fraction)
