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

### Example usage
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