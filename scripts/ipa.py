#! /usr/bin/env python3

"""
Author: Ivan Sovic
"""

import os
import sys
import json
import argparse
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_PATH = os.path.join('{}'.format(SCRIPT_DIR), '..', 'etc', 'ipa.snakemake')

def run(genome_size, coverage, advanced_opt, polish_run, phase_run, nproc, max_nchunks, tmp_dir, input_fn):
    phase_run_int = 1 if phase_run.lower() in ['true', '1'] else 0
    polish_run_int = 1 if polish_run.lower() in ['true', '1'] else 0

    config = {
        'reads_fn': input_fn,
        'genome_size': genome_size,
        'coverage': coverage,
        'advanced_options': advanced_opt,
        'polish_run': polish_run_int,
        'phase_run': phase_run_int,
        'nproc': nproc,
        'max_nchunks': max_nchunks,
        'tmp_dir': tmp_dir,
    }

    with open('config.json', 'w') as fp_out:
        fp_out.write(json.dumps(config, indent = 4))
        fp_out.write('\n')

    cmd = 'snakemake -p -j 1 -d RUN -s {} --configfile config.json -- finish'.format(WORKFLOW_PATH)

    env = {}
    env.update(os.environ)
    subprocess.run(cmd, shell = True, env = env)

class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
   pass

def parse_args(argv):
    parser = argparse.ArgumentParser(description="Improved Phased Assembly tool for HiFi reads.",
                                     formatter_class=HelpF)
    parser.add_argument('--genome-size', type=int, default=0,
                        help='Genome size, required only for downsampling.')
    parser.add_argument('--coverage', type=int, default=0,
                        help='Downsampled coverage, used only for downsampling if genome_size * coverage > 0.')
    parser.add_argument('--advanced-opt', type=str, default="",
                        help='Advanced options.')
    parser.add_argument('--polish-run', type=str, default='true',
                        help='Run polishing if true.')
    parser.add_argument('--phase-run', type=str, default='true',
                        help='Run phasing if true.')
    parser.add_argument('--nproc', type=int, default=8,
                        help='Number of threads to use.')
    parser.add_argument('--max-nchunks', type=int, default=40,
                        help='Maximum number of parallel jobs to run.')
    parser.add_argument('--tmp-dir', type=str, default='/tmp',
                        help='Temporary directory for some disk based operations like sorting.')
    parser.add_argument('input_fn', type=str,
                        help='Input reads in FASTA, FASTQ, BAM, XML or FOFN formats.')
    args = parser.parse_args(argv[1:])
    return args

def main(argv=sys.argv):
    args = parse_args(argv)
    run(**vars(args))

if __name__ == '__main__':  # pragma: no cover
    main()
