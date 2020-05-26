# IPA HiFi Genome Assembler

## (Internal) Compiling from source
To build IPA from source, some non-public packages are required. This section is intended for internal purposes only.

### Compilation on PacBio clusters
```
git clone <url>
cd ipa2
module purge
source module.sh
source env.sh
bash build_from_source.sh
```
The `module.sh` file contains all tools required for compilation.
The `build_from_source.sh` will run a small example automatically after the compilation is done.

### Compilation on a personal machine
```
git clone <url>
cd ipa2
source env.sh
make
```
This requires the following dependencies:
- GCC>=8
- NetworkX Python module
- Snakemake Python module >= 5.4.3
- Boost
- Nim
- Samtools

### Example test run
Using the Snakemake workflow:
```
cd examples/ivan-200k-t1/
make snake
```

Using the WDL workflow (this requires that the `modules/pbpipeline-resources` module is cloned):
```
cd examples/ivan-200k-t1/
make wdl
```

### Snakemake run on a local machine using the Makefile:
```
/usr/bin/time make snake 2>&1 | tee log.tee
```

### Snakemake run on a cluster using the Makefile:
```
/usr/bin/time make qsub 2>&1 | tee log.tee
```
The exact cluster config options can be modified in the Makefile script.

### Snakemake run on a local machine
The command line listed below can be used to run the Snakemake workflow.
```
WF_SNAKEMAKE=<path_to_the_ipa.snakefile>
snakemake -p -j 1 -d RUN -s ${WF_SNAKEMAKE} --configfile config.json --config MAKEDIR=.. -- finish
```

#### Cromwell run on a local machine
The command line listed below can be used to run the Cromwell workflow.
```
CROMWELL=<path_to_cromwell.jar_file>
WF_CROMWELL=<path_to_ipa.wdl_file>
java -jar ${CROMWELL} run --inputs config.json ${WF_CROMWELL} 2>&1 | tee log.wdl.tee
```
