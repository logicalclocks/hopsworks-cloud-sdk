============
Development
============

Setting up development environment
---------------------------------------------------
This section shows a way to configure a development environment that allows you to run tests and build documentation.

The recommended way to run the unit tests is to use the Dockerized Linux workspace via the Makefile provided at :code:`docker/Makefile`.
The commands below will build the Docker image, start a running container with your local hopsworks-cloud-sdk source code mounted
into it from the host at :code:`/hops`. A python 3.6 environment is available in the container at :code:`/hops_venv3.6/`.
Moreover, the commands below will also open a BASH shell into it (you must have GNU Make and Docker installed beforehand
(:code:`sudo apt-get install docker.io`)):

.. code-block:: bash

    cd docker # go to where Makefile is located

    # Build docker container and mount (you might need sudo),
    # this will fail if you already have a container named "hops" running
    # (if that is the case, just run `make shell` (or `sudo make shell`) to login to the existing container instead,
    # or if you want the kill the existing container and re-build, you can execute: `make clean` and then make build run shell)
    make build run shell # this might require sudo

    # Run the unit tests
    cd /hops/docker # once inside docker
    ./run_tests.sh 3.6 # for python 3.6
    ./run_tests.sh 3.6 -s # run tests with verbose flag

    # Alternatively you can skip the bash scripts and write the commands yourself (this gives you more control):
    cd /hops #inside the container
    source /hops_venv3.6/bin/activate # for python 3.6
    pip install -e . # install latest source
    pytest -v hops # run tests, note if you want to re-run just edit the code in your host and run the same command, you do not have to re-run pip install..

Also note that when you edit files inside your local machine the changes will automatically get reflected in the docker
container since the directory is mounted there so you can easily re-run tests during development as you do code-changes.

To open up a shell in an already built :code:`hops` container:

.. code-block:: bash

    cd docker # go to where Makefile is located
    make shell


To kill an existing :code:`hops` container:


.. code-block:: bash

    cd docker # go to where Makefile is located
    make clean

Unit tests
----------
To run unit tests locally:

.. code-block:: bash

    pytest -v hops # Runs all tests
    pytest -v hops/tests/test_featurestore.py # Run specific test module
    pytest -v hops/tests/test_featurestore.py -k 'test_project_featurestore' # Run specific test in module
    pytest -m prepare # run test setups before parallel execution. **Note**: Feature store test suite is best run sequentially, otherwise race-conditions might cause errors.
    pytest -v hops -n 5 # Run tests in parallel with 5 workers. (Run prepare first)
    pytest -v hops -n auto #Run with automatically selected number of workers
    pytest -v hops -s # run with printouts (stdout)

Documentation
-------------

We use sphinx to automatically generate API-docs

.. code-block:: bash

    pip install -e .[docs]
    cd docs; make html
