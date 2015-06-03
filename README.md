# precipes
Parallel Recipes : massively parallel and distributed workflows made easy

**Caveat Programmer: This is a first demonstrator version. A rework and cleanup pass is underway, pending a stable release.**

Key features:
  - Write down a workflow or pipeline in a simple declarative json script. For each stage you only specify a bash command string, input names and output names.
  - Automatic parallel execution across multiple runs and independent work inside each run.
  - Exploits local parallelism (node-level) as well as distributed parallelism (across nodes).
  - Can be deployed on a variety of setups. Tested so far: workstations, private clusters and Amazon EC2.
  
Technical: 
  - A parallel recipe file is a JSON script describing a simple dependency graph. Internally, this graph is translated to an Intel Concurrent Collections (https://github.com/icnc/icnc) graph. We use CnC to coordinate, parallelize and distribute the computations.
  - Made for and tested on Linux distributions (ubuntu, redhat)
  - Will not work on OSX (Darwin) due to the Intel CnC prerequisite.

Notice:
  - This version is a first demonstrator, showing how CnC can be used as a coordination language. Future versions will include front-end changes to the Parallel Recipes scripting: dynamic split/join work subdivision, resource description, initialisation scripting, ...  Back-end changes on top of CnC will include dynamic load-balancing, resource-aware scheduling and resilience.
  - Deployment on clusters and cloud environments currently relies heavily on provided scripts (cfr provided bash scripts). Everything not considered essential to the workflow description is kept out of the parallel recipes JSON file.

### Prerequisites
  - Intel CnC (https://github.com/icnc/icnc) v0.9+
  - Intel TBB (https://www.threadingbuildingblocks.org/) v4.3+
  - a C++11 compliant compiler: clang 3.4+, gcc-4.7+, icc-14+
  - GNU Make

### Building
  - run 'make'

### Running
  ./precipes samples/hello_world.json world foo bar baz
