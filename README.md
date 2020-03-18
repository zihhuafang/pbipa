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

## Installation

Installation should be simple, considering that majority of the code is in C++ and Nim.

The `smrttools/incremental` here is only used to provide the Networkx Python library for `ovlp_to_graph` and `graph_to_contig`. If those are available locally on your machine,
the `module load...` line can be omitted.

```
git clone ssh://git@bitbucket.nanofluidics.com:7999/sat/ipa2.git
cd ipa2
module load smrttools/incremental
source env.sh
make all
```

### Test run
Using the Snakemake workflow:
```
cd examples/ivan-200k-t1-haploid/
make snake
```

Using the WDL workflow:
```
cd examples/ivan-200k-t1-haploid/
make wdl
```

### Usage

#### Config file
To run IPA, a `config.json` file needs to be provided to the workflow.
The structure of `config.json` is given here:
```
{
    "ipa2.reads_fn": "input.fofn",
    "ipa2.genome_size": 0,
    "ipa2.coverage": 0,
    "ipa2.advanced_options": "",
    "ipa2.polish_run": 1,
    "ipa2.phase_run": 1,
    "ipa2.nproc": 8
}
```

Explanation of each parameter:
- `ipa2.reads_fn`: Can be a FOFN, FASTA or FASTQ
- `ipa2.genome_size`: Currently only a placeholder, not used yet. Will be used for downsampling.
- `ipa2.coverage`: Currently only a placeholder, not used yet. Will be used for downsampling.
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
config_genome_size = 0
config_coverage = 0
config_polish_run = 1
config_phase_run = 1
config_existing_db_prefix =
config_block_size = 4096
config_seqdb_opt = --compression 0
config_seeddb_opt = -k 32 -w 80 --space 2
config_ovl_opt =
config_ovl_min_idt = 98
config_ovl_min_len = 1000
config_ovl_filter_opt = --max-diff 80 --max-cov 100 --min-cov 1 --bestn 10 --min-len 4000 --gapFilt --minDepth 4
config_use_seq_ids = 0
config_phasing_opt =
config_phasing_piles = 10000
```

#### Snakemake run
The command line listed below can be used to run the Snakemake workflow.
```
WF_SNAKEMAKE=<path_to_the_ipa.snakemake_file>
CWD=${PWD}/RUN snakemake -p -j 1 -d RUN -s ${WF_SNAKEMAKE} --configfile config.json --config MAKEDIR=.. -- finish
```

#### Cromwell run
The command line listed below can be used to run the Snakemake workflow.
```
CROMWELL=<path_to_cromwell.jar_file>
WF_CROMWELL=<path_to_ipa.wdl_file>
java -jar ${CROMWELL} run --inputs config.json ${WF_CROMWELL} 2>&1 | tee log.wdl.tee
```