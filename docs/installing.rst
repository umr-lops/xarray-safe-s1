************
Installation
************


conda install
#############

Install xarray-safe-s1

.. code-block::

    conda create -n safe_s1
    conda activate safe_s1
    conda install -c conda-forge xarray-safe-s1


pip install
###########

Install xarray-safe-s1

.. code-block::

    conda create -n safe_s1
    conda activate safe_s1
    pip install git+https://github.com/umr-lops/xarray-safe-s1.git


Developement  installation
..........................

.. code-block::

    git clone https://github.com/umr-lops/xarray-safe-s1
    cd xsar
    # this is needed to register git filters
    git config --local include.path ../.gitconfig
    pip install -e .
    pip install -r requirements.txt

Pytest configuration
....................

Pytest uses a default configuration file (`config.yml`) in which we can found products paths to test.
This configuration can be superseded by adding a local config file on the home directory :
(`~/xarray-safe-s1/localconfig.yml`).
In this file, testing files can be listed in the var `product_paths`.
