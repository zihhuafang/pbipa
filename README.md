# IPA HiFi Genome Assembler

## Description

This repo contains the implementation of the IPA HiFi Genome Assembler.
It's currently implemented as a Snakemake workflow (`workflow/ipa.snakemake`) and runs the following stages:
1. Building the SeqDB and SeedDB from the input reads.
2. Overlapping using the Pancake overlapper.
3. Phasing the overlaps using the Nighthawk phasing tool.
4. Filtering the overlaps using Falconc m4Filt.
5. Contig construction using Falcon's `ovlp_to_graph` and `graph_to_contig` tools.
6. Read tracking for read-to-contig assignment.
7. Polishing using Racon.

For more info: https://github.com/PacificBiosciences/pbbioconda/wiki/IPA-Documentation

## Installation

Installation should be simple, considering that majority of the code is in C++ and Nim.

# The `smrttools/incremental` here is only used to provide the Networkx Python library for `ovlp_to_graph` and `graph_to_contig`. If those are available locally on your machine,
# the `module load...` line can be omitted.

### Compilation on PacBio clusters
```
git clone ssh://git@bitbucket.nanofluidics.com:7999/sat/ipa2.git
cd ipa2
module purge
source module.sh
source env.sh
bash build_from_source.sh
```
The `module.sh` file contains all tools required for compilation, including `smrttools/incremental` which contains the NetworkX Python library.
The `build_from_source.sh` will run a small example automatically after the compilation is done.

### Compilation on a personal machine
```
git clone ssh://git@bitbucket.nanofluidics.com:7999/sat/ipa2.git
cd ipa2
source env.sh
make
```
This requires the following dependencies to be installed:
- GCC>=8
- NetworkX Python module
- Snakemake Python module
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

## Usage

One file is required to run IPA:
These files are needed to run IPA:
- (mandatory) Config file: `config.json`.
- (optional) Input FOFN in case the config points to it: `input.fofn`.
- (optional) Makefile which wraps running of the tool. This can be copied from `examples/ecoli-k12-2019_03_30/Makefile`.

The Makefile can be useful so that issuing the Snakemake/Cromwell jobs can be done with a simple command like `make snake` or `make wdl`.

### Config file
The structure of `config.json` is given here:
```
{
    "ipa2.reads_fn": "input.fofn",
    "ipa2.genome_size": 0,
    "ipa2.coverage": 0,
    "ipa2.advanced_options": "",
    "ipa2.polish_run": 0,
    "ipa2.phase_run": 1,
    "ipa2.nproc": 8
}
```

Explanation of each parameter:
- `ipa2.reads_fn`: Can be a FOFN, FASTA, FASTQ, BAM or XML. Also, gzipped versions of FASTA and FASTQ are available.
- `ipa2.genome_size`: Used for downsampling in combination with `coverage`. If `genome_size * coverage <=0` downsampling is turned off.
- `ipa2.coverage`: Used for downsampling in combination with `genome_size`. If `genome_size * coverage <=0` downsampling is turned off.
- `ipa2.advanced_options`: A single line listing advanced options in the form of `key = value` pairs, separated with `;`.
- `ipa2.polish_run`: Polishing will be applied if the value of this parameter is equal to `1`.
- `ipa2.phase_run`: Phasing will be applied if the value of this parameter is equal to `1`.
- `ipa2.nproc`: Number of threads to use on each compute node.

An example config with a custom `ipa2.advanced_options` string:
```
{
    "ipa2.reads_fn": "input.fofn",
    "ipa2.genome_size": 0,
    "ipa2.coverage": 0,
    "ipa2.advanced_options": "config_seeddb_opt = -k 30 -w 80 --space 1; config_block_size = 2048; config_phasing_piles = 20000",
    "ipa2.polish_run": 1,
    "ipa2.phase_run": 1,
    "ipa2.nproc": 8
}
```

The list of all options that can be modified via the `advanced_options` string and their defaults:
```
config_autocomp_max_cov = 1
config_block_size = 4096
config_coverage = 0
config_existing_db_prefix = ''
config_genome_size = 0
config_ovl_filter_opt = '--max-diff 80 --max-cov 100 --min-cov 1 --bestn 10 --min-len 4000 --gapFilt --minDepth 4'
config_ovl_min_idt = 98
config_ovl_min_len = 1000
config_ovl_opt = ''
config_phase_run = 1
config_phasing_opt = ''
config_phasing_piles = 10000
config_polish_run = 1
config_seeddb_opt = '-k 30 -w 80 --space 1'
config_seqdb_opt = '--compression 1'
config_use_seq_ids = 1
```

### Snakemake run on a local machine using the Makefile:
```
/usr/bin/time make snake 2>&1 | tee log.tee
```

### Snakemake run on a cluster using the Makefile:
```
/usr/bin/time make qsub 2>&1 | tee log.tee
```
The exact cluster config options can be modified in the Makefile script:
```
cluster_S=/bin/bash
cluster_N=ipa
cluster_P=-cwd
cluster_Q=default
cluster_E=qsub_log.stderr
cluster_O=qsub_log.stdout
cluster_CPU=-pe smp 8
cluster_JOBS=10
```

To modify the number of CPUs used by each job, two settings need to be set:
- `cluster_CPU` in the Makefile
- `ipa2.nproc` in he config

The number of jobs run simultaneously can be set with `cluster_JOBS`.

### Snakemake run on a local machine
The command line listed below can be used to run the Snakemake workflow.
```
WF_SNAKEMAKE=<path_to_the_ipa.snakemake_file>
CWD=${PWD}/RUN snakemake -p -j 1 -d RUN -s ${WF_SNAKEMAKE} --configfile config.json --config MAKEDIR=.. -- finish
```

#### Cromwell run on a local machine
The command line listed below can be used to run the Snakemake workflow.
```
CROMWELL=<path_to_cromwell.jar_file>
WF_CROMWELL=<path_to_ipa.wdl_file>
java -jar ${CROMWELL} run --inputs config.json ${WF_CROMWELL} 2>&1 | tee log.wdl.tee
```
