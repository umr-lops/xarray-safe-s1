# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/umr-lops/xarray-safe-s1/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                 |    Stmts |     Miss |   Cover |   Missing |
|------------------------------------- | -------: | -------: | ------: | --------: |
| safe\_s1/\_\_init\_\_.py             |       11 |        5 |     55% |7-8, 11-13 |
| safe\_s1/getconfig.py                |       18 |        4 |     78% |     19-22 |
| safe\_s1/reader.py                   |      363 |       47 |     87% |145, 175-180, 195-197, 206, 229, 298-299, 436, 452, 484, 500, 543, 585, 591, 608, 626-627, 642, 651, 671, 688, 710-719, 749, 808-810, 863-866, 884-885, 891, 960-962, 1030-1034 |
| safe\_s1/sentinel1\_xml\_mappings.py |      255 |       42 |     84% |109, 715, 743-766, 771, 790-792, 846, 848-850, 891, 947, 989-1038, 1228 |
| safe\_s1/xml\_parser.py              |       82 |        4 |     95% |111, 120, 173, 194 |
| **TOTAL**                            |  **729** |  **102** | **86%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/umr-lops/xarray-safe-s1/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/umr-lops/xarray-safe-s1/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/umr-lops/xarray-safe-s1/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/umr-lops/xarray-safe-s1/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fumr-lops%2Fxarray-safe-s1%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/umr-lops/xarray-safe-s1/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.