#! /usr/bin/env python3
import argparse
import json
import logging
import math
import multiprocessing
import os
import shlex
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_PATH = os.path.join('{}'.format(SCRIPT_DIR), '..', 'etc', 'ipa.snakefile')
NCPUS = multiprocessing.cpu_count()
LOG = logging.getLogger()

def write_if_changed(fn, content):
    if os.path.exists(fn):
        current = open(fn).read()
        if current == content:
            return
        raise RuntimeError('{} changed!'.format(fn))
    with open(fn, 'w') as fout:
        fout.write(content)

def abspath(dn, fn):
    """Return (normalized) absolute path to fn, relative to dn."""
    if fn.startswith('/'):
        return os.path.normpath(fn)
    return os.path.normpath(os.path.join(dn, fn))

def run(genome_size, coverage, advanced_opt, no_polish, no_phase,
        njobs, nthreads, nshards, tmp_dir, run_dir, dry_run, resume, input_fns):
    nthreads = NCPUS // njobs if not nthreads else nthreads
    snakefile_fn = os.path.abspath(WORKFLOW_PATH)
    phase_run_int = 1 if not no_phase else 0
    polish_run_int = 1 if not no_polish else 0

    # Create FOFN, and absolutize paths.
    # TODO: Skip this for dataset XML?
    if 1 == len(input_fns) and input_fns[0].endswith('.fofn'):
        fofn_fn = input_fns[0]
        input_fns = list(open(fofn_fn).read().splitlines())
        abs_dn = os.path.abspath(os.path.dirname(fofn_fn))
    else:
        abs_dn = os.getcwd()
    abs_input_fns = [abspath(abs_dn, fn) for fn in input_fns]

    # Validate inputs.
    exts = ['.fasta', '.fastq', '.bam', '.xml'] # TODO: Worry about abspath recursively?
    for fn in abs_input_fns:
        ext = os.path.splitext(fn)[1]
        if ext not in exts:
            msg = 'Found "{}" ({}). Reads must have one of the following extensions: {}'.format(
                    ext, fn, exts)
            raise RuntimeError(msg)

    run_dn = os.path.normpath(run_dir)
    if os.path.isdir(run_dn):
        if not resume:
            msg = 'Run-directory "{}" exists. Remove and re-try.'.format(run_dn)
            raise RuntimeError(msg)
    else:
        os.makedirs(run_dn)

    reads_fn = os.path.join(run_dn, 'input.fofn')
    content = '\n'.join(abs_input_fns + [''])
    write_if_changed(reads_fn, content)

    config = {
        'reads_fn': reads_fn,
        'genome_size': genome_size,
        'coverage': coverage,
        'advanced_options': advanced_opt,
        'polish_run': polish_run_int,
        'phase_run': phase_run_int,
        'max_nchunks': nshards,
        'nproc': nthreads,
        'tmp_dir': tmp_dir,
    }

    config_fn = os.path.join(run_dn, 'config.json')
    content = json.dumps(config, indent = 4, separators=(',', ': ')) + '\n'
    #write_if_changed(config_fn, content)
    open(config_fn, 'w').write(content)
    try:
        import yaml
        config_fn = os.path.join(run_dn, 'config.yaml')
        content = yaml.dump(config, indent = 4)
        #write_if_changed(config_fn, content)
        open(config_fn, 'w').write(content)
    except ImportError:
        pass

    if nthreads*njobs > NCPUS:
        msg = f"""You may have over-subscribed your local machine.
We have detected only {NCPUS} CPUs, but you have assumed {njobs*nthreads} are available.
(njobs*nthreads)==({njobs}*{nthreads})=={njobs*nthreads} > {NCPUS}
"""
        LOG.warning(msg)

    snakemake = subprocess.check_output(['which', 'snakemake'], encoding='ascii').rstrip()
    words = [
            snakemake,
            '-j', str(njobs),
            '-d', run_dn,
            '-p',
            '-s', snakefile_fn,
            '--configfile', config_fn,
            '--reason',
            '--', 'finish',
    ]
    #cmd = shlex.join(words) # python3.8
    cmd = ' '.join(words)
    print(cmd, flush=True)

    if dry_run:
        print('Dry-run:')
        words.insert(1, '--dryrun')
        cmd = ' '.join(words)

    # We want to replace the current process, rather than capture output.
    # Because internally we run via a wrapp, the executable is actually python.
    # So we need to do some magic.
    os.execv('/usr/bin/env', ['python'] + words)

    #env = {}
    #env.update(os.environ)
    #subprocess.run(cmd, shell = True, env = env)


def nearest_divisor(v, x):
    """
    >>> nearest_divisor(24, 4.9)
    4
    >>> nearest_divisor(25, 4.9)
    5
    """
    higher = int(math.ceil(x))
    lower = int(math.floor(x))
    if higher - x > x - lower:
        # Start with lower.
        curr = lower
        sense = 1
    else:
        # Start with higher.
        curr = higher
        sense = -1
    delta = 1
    while curr > 0 and curr <= v:
        if v % curr == 0:
            return curr
        curr += sense*delta
        delta += 1
        sense *= -1
    raise RuntimeError(f'No divisor found for {v, x}')

def get_version():
    try:
        import networkx
    except ImportError as exc:
        LOG.exception('Try "pip3 install --user networkx"')

    cmd = """
        echo "ipa (wrapper) version=1.0.1"
        IPA_QUIET=1 ipa2-task version
        falconc version
        nighthawk --version
        pancake --version
        pblayout --version
        samtools --version
        echo "racon version=$(racon --version)"
        echo "snakemake version=$(snakemake --version)"
"""
    output = subprocess.check_output(cmd, shell=True)
    return output.decode('ascii')
class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
   pass

def parse_args(argv):
    default_njobs = nearest_divisor(NCPUS, math.log2(NCPUS))
    default_nthreads = NCPUS // default_njobs

    description = "Improved Phased Assembly tool for HiFi reads."
    epilog = """
Try "ipa local --help".
Or "ipa --version" to validate dependencies.
"""
    parser = argparse.ArgumentParser(description=description,
                                     epilog=epilog,
                                     formatter_class=HelpF)
    parser.add_argument('--version', action='version', version=get_version())

    subparsers = parser.add_subparsers(help='sub-command help')
    lparser = subparsers.add_parser('local',
            description='This sub-command runs snakemake in local-mode.',
            epilog='(Some defaults may vary by machine ncpus.)',
            help='to run snakemake on your local machine')
    cparser = subparsers.add_parser('cluster',
            description='This sub-command runs snakemake in cluster-mode, i.e. with job-distribution.',
            epilog='(NOT YET IMPLEMENTED)',
            help='to run snakemake on your cluster (not yet implemented)')

    lparser.add_argument('input_fns', type=str, nargs='+',
                        help='Input reads in FASTA, FASTQ, BAM, XML or FOFN formats.')
    lparser.add_argument('--genome-size', type=int, default=0,
                        help='Genome size, required only for downsampling.')
    lparser.add_argument('--coverage', type=int, default=0,
                        help='Downsampled coverage, used only for downsampling if genome_size * coverage > 0.')
    lparser.add_argument('--advanced-opt', type=str, default="",
                        help='Advanced options (quoted).')
    lparser.add_argument('--no-polish', action='store_true',
                        help='Skip polishing.')
    lparser.add_argument('--no-phase', action='store_true',
                        help='Skip phasing.')
    lparser.add_argument('--tmp-dir', type=str, default='/tmp',
                        help='Temporary directory for some disk based operations like sorting.')
    lparser.add_argument('--run-dir', type=str, default='./RUN',
                        help='Directory in which to run snakemake.')
    lparser.add_argument('--resume', action='store_true',
                        help='Restart snakemake, but after regenerating the config file. In this case, run-dir can already exist.')
    lparser.add_argument('--nthreads', type=int, default=0,
                        help='Maximum number of threads to use per job. If 0, then use ncpus/njobs.')
    lparser.add_argument('--njobs', type=int, default=default_njobs,
                        help='Maximum number of simultaneous jobs, each running up to nthreads.')
    lparser.add_argument('--nshards', type=int, default=default_njobs,
                        help='Maximum number of parallel tasks to split work into (though the number of simultaneous jobs could be much lower).')
    lparser.add_argument('--dry-run', '-n', action='store_true',
                        help='Print the snakemake command and do a "dry run" quickly. Very useful!')
    args = parser.parse_args(argv[1:])
    return args

def main(argv=sys.argv):
    logging.basicConfig()
    args = parse_args(argv)
    run(**vars(args))

if __name__ == '__main__':  # pragma: no cover
    main()
