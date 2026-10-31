"""Resampled read (`resolution` given): cell content must match the cell coordinates,
zeros outside the swath must not be counted, and cell means must not be rounded.
Synthetic GeoTIFFs only, no SAR product needed."""

import warnings

import numpy as np
import pytest
import rasterio
from rasterio.enums import Resampling

from safe_s1.reader import read_cells, read_valid_fraction

warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)


def write_tiff(path, array):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype=array.dtype,
    ) as dst:
        dst.write(array, 1)
    return str(path)


def cell_center(k, r):
    """Coordinate given by `load_digital_number` to cell k, for r pixels per cell."""
    return r * k + (r - 1) / 2


@pytest.mark.parametrize("r", [10.0, 2.5])
def test_cell_content_matches_coordinates(tmp_path, r):
    """Image whose value is the line (resp. sample) index + 1 (0 is nodata):
    the mean of a cell minus 1 must be the coordinate of the cell.
    Height and width are not multiples of r, so a read of the whole image squeezed
    into the cells would drift towards the end of the image."""
    height, width = 1003, 807
    out_shape = (int(height / r), int(width / r))
    resolution = {"line": r, "sample": r}
    lines = np.repeat(np.arange(1, height + 1, dtype=np.uint16)[:, None], width, axis=1)
    samples = np.repeat(
        np.arange(1, width + 1, dtype=np.uint16)[None, :], height, axis=0
    )

    cells = read_cells(
        write_tiff(tmp_path / "lines.tif", lines),
        resolution,
        out_shape,
        Resampling.average,
    )[0]
    expected = cell_center(np.arange(out_shape[0]), r)
    np.testing.assert_allclose(cells[:, 0] - 1, expected, atol=0.1)

    cells = read_cells(
        write_tiff(tmp_path / "samples.tif", samples),
        resolution,
        out_shape,
        Resampling.average,
    )[0]
    expected = cell_center(np.arange(out_shape[1]), r)
    np.testing.assert_allclose(cells[0, :] - 1, expected, atol=0.1)


def test_zeros_are_not_counted(tmp_path):
    """Constant image with a band of zeros (outside the swath) cutting cells in two."""
    array = np.full((200, 200), 50, dtype=np.uint16)
    array[:, 135:] = 0
    path = write_tiff(tmp_path / "border.tif", array)
    resolution = {"line": 10.0, "sample": 10.0}

    cells = read_cells(path, resolution, (20, 20), Resampling.rms)[0]
    np.testing.assert_allclose(cells[:, :14], 50)  # full and half-empty cells
    np.testing.assert_array_equal(cells[:, 14:], 0)  # empty cells

    fraction = read_valid_fraction(path, resolution, (20, 20))
    np.testing.assert_allclose(fraction[:, :13], 1)
    np.testing.assert_allclose(fraction[:, 13], 0.5)
    np.testing.assert_array_equal(fraction[:, 14:], 0)


def test_cell_means_are_not_rounded(tmp_path):
    """Checkerboard of 20 and 21: the rms of a cell is not an integer."""
    array = (20 + np.indices((100, 100)).sum(axis=0) % 2).astype(np.uint16)
    path = write_tiff(tmp_path / "checkerboard.tif", array)

    cells = read_cells(path, {"line": 10.0, "sample": 10.0}, (10, 10), Resampling.rms)
    assert cells.dtype == np.float32
    np.testing.assert_allclose(cells, np.sqrt((20**2 + 21**2) / 2), rtol=1e-6)
