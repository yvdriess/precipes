# precipes
Parallel Recipes : massively parallel and distributed workflows made easy

**This precipes version, provided as-is, is a demonstrator version and not production-ready. CWL support is currently in a semi-working state, pending changes to the Rabix CWL implementation.**

Key features:
  - Write down a workflow or pipeline in a simple declarative json script. For each stage you only specify a bash command string, input names and output names.
  - Support for Common Workflow Language (CWL) input, a multi-vendor data analysis workflow specification format.
  - Automatic parallel execution across multiple runs and independent work inside each run.
  - Exploits local parallelism (node-level) as well as distributed parallelism (across nodes).
  - Can be deployed on a variety of setups. Tested so far: workstations, private clusters and Amazon EC2.
  
Technical: 
  - A parallel recipe file is a JSON script describing a simple dependency graph. Internally, this graph is translated to an Intel Concurrent Collections (https://github.com/icnc/icnc) graph. We use CnC to coordinate, parallelize and distribute the computations.
  - Made for and tested on Linux distributions (ubuntu, redhat)
  - Will not work on OSX (Darwin) due to the Intel CnC prerequisite.

Notice:
  - This version is a first demonstrator, showing how CnC can be used as a coordination language. This version includes both a CWL and 'Parallel Recipe' front-end. The Parallel Recipes scripting has preliminary dynamic split/join work subdivision. CWL support is preliminary, but will become the preferred input language in the longer term.
  - Everything not considered essential to the workflow description is kept out of the parallel recipes JSON file. Deployment on clusters and cloud environments is currently manual, viz. the user handles MPI startup or manual command line startup. Several pre-made scripts are provided in the `samples/exome/` directory. 

### Prerequisites
  - Intel CnC (https://github.com/icnc/icnc) v0.9+
  - Intel TBB (https://www.threadingbuildingblocks.org/) v4.3+
  - a C++11 compliant compiler: clang 3.4+, gcc-4.7+, icc-14+
  - Boost (http://www.boost.org/) v1.57+
  - GNU Make
For CWL support:
  - Flask (http://flask.pocoo.org/) Python web framework
  - libcurl (http://curl.haxx.se/libcurl/)
  - Rabix (https://github.com/rabix/rabix) CWL implementation, python framework


### Building
  - run 'make'

### Running
#### Parallel Recipes
  - `./precipes samples/hello_world.json world`
  - `./precipes samples/test_pipeline.json foo bar baz`

  See `samples/exome/` for a much more involved example running the best-practices exome pipeline.

#### CWL
  Preflight check:
  - Keep the `python rabix_http.py` Rabix HTTP interface process running on the same machine. This interface requires the flask and Rabix 
  - `precipes` currently only accepts preprocessed CWL input workflows, viz. a single flat json file. Use the `cwl_linker.py` script to process the `.cwl` files into the expected `.json` format.
    `python cwl_linker.py revsort.cwl > revsort.json`

  Example:
  - To run in CWL input mode, use the `-c` switch and pass the workflow input arguments to the command line. 
    `./precipes -c samples/cwl/revsort.json foo.txt true`
  - Multiples of the expected inputs are considered different parallel batches. 
    `./precipes -c samples/cwl/revsort.json text_1.txt true text_2.txt true.`

### Parallel Recipe
*TODO: explain the simple json recipe format*
