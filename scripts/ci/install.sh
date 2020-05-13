#!/usr/bin/env bash
set -vex

###########
# INSTALL #
###########

if [[ ${PREFIX_ARG} ]]; then
  ## Cleaning out old installation from /mnt/software
  rm -rf "${PREFIX_ARG}"/*
fi

mkdir -p ${PREFIX_ARG}/bin
mkdir -p ${PREFIX_ARG}/etc

cd scripts/ci
cp -Lf ../ipa ${PREFIX_ARG}/bin/
cp -Lf ../ipa2_graph_to_contig ${PREFIX_ARG}/bin/
cp -Lf ../ipa2_ovlp_to_graph ${PREFIX_ARG}/bin/
cp -Lf ../../bash/ipa2-task ${PREFIX_ARG}/bin/
cp -Lf ../../etc/ipa.snakefile ${PREFIX_ARG}/etc/
