#!/usr/bin/env bash

conda remove -y --force cytoolz numpy xarray toolz python-dateutil
python -m pip install \
  -i https://pypi.anaconda.org/scientific-python-nightly-wheels/simple \
  --no-deps \
  --pre \
  --upgrade \
  numpy \
  xarray
python -m pip install --upgrade \
  git+https://github.com/pytoolz/toolz \
  git+https://github.com/dateutil/dateutil
