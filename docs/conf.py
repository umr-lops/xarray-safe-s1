# -- Project information -----------------------------------------------------
import datetime as dt

project = "xarray-safe-s1"
author = f"{project} developers"
initial_year = "2023"
year = dt.datetime.now().year
copyright = f"{initial_year}-{year}, {author}"

# The root toctree document.
root_doc = "index"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
    "myst_parser",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "IPython.sphinxext.ipython_directive",
    "IPython.sphinxext.ipython_console_highlighting",
    "nbsphinx",
    "jupyter_sphinx",
    "sphinx.ext.napoleon",
]

extlinks = {
    "issue": ("https://github.com/umr-lops/xarray-safe-s1/issues/%s", "GH%s"),
    "pull": ("https://github.com/umr-lops/xarray-safe-s1/pull/%s", "PR%s"),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

html_static_path = ["_static"]

html_style = "css/xsar.css"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "directory"]

# nitpicky mode: complain if references could not be found
nitpicky = True

# Napoleon settings
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_type_aliases = None


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_book_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]

# -- Options for the intersphinx extension -----------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "dask": ("https://docs.dask.org/en/latest", None),
    "xarray": ("https://docs.xarray.dev/en/latest/", None),
    "rasterio": ("https://rasterio.readthedocs.io/en/latest/", None),
    "datatree": ("https://xarray-datatree.readthedocs.io/en/latest/", None),
}

html_theme_options = {
    "navigation_depth": 4,  # FIXME: doesn't work as expeted: should expand side menu
    "collapse_navigation": False,  # FIXME: same as above
}

# If true, links to the reST sources are added to the pages.
html_show_sourcelink = False

nbsphinx_allow_errors = False

nbsphinx_execute = "always"

nbsphinx_timeout = 300

today_fmt = "%b %d %Y at %H:%M"
