# IPA HiFi Genome Assembler

## Description

This repo contains the implementation of the IPA HiFi Genome Assembler.
It's currently implemented as a Snakemake workflow (`workflow/ipa.snakemake`) and runs the following stages:
1. Building the SeqDB and SeedDB from the input reads.
2. Overlapping using the Pancake overlapper.
3. Filtering the overlaps using Falconc m4Filt.
4. Contig construction using Falcon's `ovlp_to_graph` and `graph_to_contig` tools.

## Installation

Installation should be simple, considering that majority of the code is in C++ and Nim.
The String Graph code from Falcon is still being used, and for this the Smrttools module is needed.

```
git clone ssh://git@bitbucket.nanofluidics.com:7999/~isovic/ipa2.git
cd ipa2
module purge
source module.sh
source env.sh
make all
```

### Example usage
```
cd examples/ecoli-k12-2019_03_30/
make
```
