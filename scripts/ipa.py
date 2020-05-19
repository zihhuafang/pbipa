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

def generate_fofn(args):
    """Return generated filename.
    input_fn is a list, but after this call it will be a single filename.
    """
    input_fn = args.input_fn
    if not input_fn:
        msg = 'No input filenames provided. (-i foo.fasta)'
        LOG.error(msg)
        sys.exit(1)
    # Create run_dir, if necessary.
    if not os.path.isdir(args.run_dir):
        os.makedirs(args.run_dir)
    # Absolutize paths.
    if 1 == len(input_fn) and input_fn[0].endswith('.fofn'):
        fofn_fn = input_fn[0]
        input_fn = list(open(fofn_fn).read().splitlines())
        abs_dn = os.path.abspath(os.path.dirname(fofn_fn))
    else:
        abs_dn = os.getcwd()
    abs_input_fns = [abspath(abs_dn, fn) for fn in input_fn]

    # Validate inputs.
    exts = ['.fasta', '.fastq', '.bam', '.xml'] # TODO: Worry about abspath recursively?
    for fn in abs_input_fns:
        ext = os.path.splitext(fn)[1]
        if ext not in exts:
            msg = 'Found "{}" ({}). Reads must have one of the following extensions: {}'.format(
                    ext, fn, exts)
            raise RuntimeError(msg)

    # Write FOFN.
    reads_fn = os.path.join(args.run_dir, 'input.fofn')
    content = '\n'.join(abs_input_fns + [''])
    write_if_changed(reads_fn, content)

    args.input_fn = reads_fn

def normalize_args(args):
    args.run_dir = os.path.normpath(args.run_dir)
    args.nthreads = NCPUS // args.njobs if not args.nthreads else args.nthreads
    args.phase_run = 1 if not args.no_phase else 0
    args.polish_run = 1 if not args.no_polish else 0
    args.cluster_args = getattr(args, 'cluster_args', '')
    args.resume = getattr(args, 'resume', True)

def validate(args):
    """Possibly create run_dir.
    """
    if args.nthreads*args.njobs > NCPUS:
        msg = f"""You may have over-subscribed your local machine.
We have detected only {NCPUS} CPUs, but you have assumed {args.njobs*args.nthreads} are available.
(njobs*nthreads)==({args.njobs}*{args.nthreads})=={args.njobs*args.nthreads} > {NCPUS}
"""
        LOG.warning(msg)

    snake_dn = os.path.join(args.run_dir, '.snakemake')
    if os.path.isdir(snake_dn):
        if not args.resume:
            msg = f'Run-directory "{snake_dn}" exists. Remove and re-try.'
            raise RuntimeError(msg)

    lock_dn = os.path.join(args.run_dir, '.snakemake', 'locks')
    if os.path.isdir(lock_dn) and os.listdir(lock_dn):
        if not args.unlock:
            msg = f'Snakemake lock-directory "{lock_dn}" is not empty. Remove and re-try, or use "--unlock".'
            raise RuntimeError(msg)

    check_dependencies()

def check_dependencies():
    print("Checking dependencies ...")
    cmd = """
        which python3
        which ipa2-task
        which falconc
        which nighthawk
        which pancake
        which pblayout
        which racon
        which samtools

        echo "snakemake version=$(python3 -m snakemake --version)"
        IPA_QUIET=1 ipa2-task version
        falconc version
        nighthawk --version
        pancake --version
        pblayout --version
        echo "racon version=$(racon --version)"
        samtools --version | head -n 2
"""
    output = subprocess.check_output(cmd, shell=True)
    print(output.decode('ascii'))

def write_config(args):
    """Return config_fn.
    """
    config = {
        'reads_fn': args.input_fn,
        'genome_size': args.genome_size,
        'coverage': args.coverage,
        'advanced_options': args.advanced_opt,
        'polish_run': args.polish_run,
        'phase_run': args.phase_run,
        'max_nchunks': args.nshards,
        'nproc': args.nthreads,
        'tmp_dir': args.tmp_dir,
    }
    config_fn = os.path.join(args.run_dir, 'config.json')
    content = json.dumps(config, indent = 4, separators=(',', ': ')) + '\n'
    #write_if_changed(config_fn, content)
    open(config_fn, 'w').write(content)
    try:
        import yaml
        config_fn = os.path.join(args.run_dir, 'config.yaml')
        content = yaml.dump(config, indent = 4)
        #write_if_changed(config_fn, content)
        open(config_fn, 'w').write(content)
    except ImportError:
        pass

    if args.verbose:
        msg = f'Wrote "{config_fn}".\n{content}'
        print(msg)

    return config_fn

def run_validate(args):
    check_dependencies()

def run_local(args):
    args.cluster_args = None # ignored in local-mode

    normalize_args(args)
    validate(args)
    generate_fofn(args)
    config_fn = write_config(args)
    run(args, config_fn)

def run_dist(args):
    args.resume = True # always on for dist-mode

    normalize_args(args)
    validate(args)
    generate_fofn(args)
    config_fn = write_config(args)
    run(args, config_fn)

def run(args, config_fn):
    run_dn = args.run_dir
    unlock = args.unlock
    verbose = args.verbose
    target = args.target
    dry_run = args.dry_run
    njobs = args.njobs
    cluster_args = args.cluster_args

    snakefile_fn = os.path.abspath(WORKFLOW_PATH)

    #snakemake = subprocess.check_output(['which', 'snakemake'], encoding='ascii').rstrip()
    pyexe = sys.executable
    words = [
            '-j', str(njobs),
            '-d', run_dn,
            '-p',
            '-s', snakefile_fn,
            '--configfile', config_fn,
            '--reason',
    ]
    if cluster_args:
        words.extend(['--cluster', cluster_args, '--latency-wait', '60', '--rerun-incomplete'])
        if verbose:
            words.extend(['--verbose'])
    if unlock:
        words.extend(['--unlock'])
    if not verbose:
        os.environ['IPA_QUIET'] = '1'
    else:
        if 'IPA_QUIET' in os.environ:
            del os.environ['IPA_QUIET']
    if target:
        words.extend('--', target)

    words[0:0] = [pyexe, '-m', 'snakemake']

    if dry_run:
        words.insert(3, '--dryrun') # after python -m snakemake
        cmd = ' '.join(words)

    #cmd = shlex.join(words) # python3.8
    cmd = ' '.join(shlex.quote(word) for word in words)
    print("\nTo run this yourself:")
    print(cmd)
    if dry_run:
        print("\nStarting snakemake --dryrun ...", flush=True)
    else:
        print("\nStarting snakemake ...", flush=True)

    # We want to replace the current process, rather than capture output.
    # Because internally we run via a wrapp, the executable is actually python.
    # So we need to do some magic.
    #os.execv('/usr/bin/env', ['python3'] + words)

    env = {}
    env.update(os.environ)
    subprocess.run(cmd, shell = True, env = env)


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
        import snakemake
    except ImportError as exc:
        LOG.exception('Try "pip3 install --user networkx snakemake"')

    return """
ipa (wrapper) version=1.0.2
"""

def add_common_options(parser, cmd='local'):
    default_njobs = nearest_divisor(NCPUS, math.log2(NCPUS))
    default_nthreads = NCPUS // default_njobs

    parser.add_argument('input_fn', type=str, nargs='*',
            help='(Required.) Input reads in FASTA, FASTQ, BAM, XML or FOFN formats.')

    alg = parser.add_argument_group('Algorithmic options') #, 'These are all you really need to know.')
    alg.add_argument('--no-polish', action='store_true',
            help='Skip polishing.')
    alg.add_argument('--no-phase', action='store_true',
            help='Skip phasing.')
    alg.add_argument('--genome-size', type=int, default=0,
            help='Genome size, required only for downsampling.')
    alg.add_argument('--coverage', type=int, default=0,
            help='Downsampled coverage, only if genome_size * coverage > 0.')
    alg.add_argument('--advanced-opt', type=str, default="",
            help='Advanced options (quoted).')

    wf = parser.add_argument_group('Workflow options') #, 'These impact how the tasks are run.')
    wf.add_argument('--nthreads', type=int, default=0,
                        help='Maximum number of threads to use per job. If 0, then use ncpus/njobs.')
    wf.add_argument('--nshards', type=int, default=default_njobs,
                        help='Maximum number of parallel tasks to split work into (though the number of simultaneous jobs could be much lower).')
    wf.add_argument('--tmp-dir', type=str, default='/tmp',
                        help='Temporary directory for some disk based operations like sorting.')
    wf.add_argument('--verbose', action='store_true',
            help='Extra logging for each task. (Show full env, e.g.)')

    snake = parser.add_argument_group('Snakemake options') #, 'These impact how snakemake is run.')
    snake.add_argument('--njobs', type=int, default=default_njobs,
                        help='Maximum number of simultaneous jobs, each running up to nthreads.')
    snake.add_argument('--run-dir', type=str, default='./RUN',
                        help='Directory in which to run snakemake.')
    snake.add_argument('--target', type=str, default='',
                        help='"finish" is implied, but you can use this to short-circuit.')
    snake.add_argument('--unlock', action='store_true',
                        help='Pass "--unlock" to snakemake, in case snakemake crashed earlier.')
    snake.add_argument('--dry-run', '-n', action='store_true',
                        help='Print the snakemake command and do a "dry run" quickly. Very useful!')
    if cmd == 'local':
        snake.add_argument('--resume', action='store_true',
                            help='Restart snakemake, but after regenerating the config file. In this case, run-dir may already exist. (Without --resume, run-dir must not already exist.)')
        snake.add_argument('--cluster-args', type=str, default=None,
                            help=argparse.SUPPRESS)
    else:
        assert cmd == 'dist'
        snake.add_argument('--resume', action='store_true',
                            help=argparse.SUPPRESS)
        snake.add_argument('--cluster-args', type=str, default='echo "no defaults yet"',
                            help='(Required) Pass this along to snakemake, for conveniently running in a compute cluster.')

class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
   pass

def parse_args(argv):
    description = "Improved Phased Assembly tool for HiFi reads."
    epilog = """
Try "ipa local --help".
Or "ipa validate" to validate dependencies.
https://github.com/PacificBiosciences/pbbioconda/wiki/Improved-Phased-Assember
"""
    parser = argparse.ArgumentParser(description=description,
                                     epilog=epilog,
                                     formatter_class=HelpF)
    parser.add_argument('--version', action='version', version=get_version())

    subparsers = parser.add_subparsers(
            description='One of these must follow the options listed above and may be followed by sub-command specific options.',
            help='sub-command help')
    lparser = subparsers.add_parser('local',
            description='This sub-command runs snakemake in local-mode.',
            epilog='',
            help='Run IPA on your local machine.')
    dparser = subparsers.add_parser('dist',
            description='This sub-command runs snakemake in cluster-mode, i.e. with job-distribution.',
            epilog='',
            help='Distribute IPA jobs to your cluster.')
    vparser = subparsers.add_parser('validate',
            description='This sub-command shows the versions of dependencies.',
            help='Check dependencies.')
    parser.set_defaults(cmd=None)
    lparser.set_defaults(cmd=run_local)
    dparser.set_defaults(cmd=run_dist)
    vparser.set_defaults(cmd=run_validate)

    add_common_options(lparser, 'local')
    add_common_options(dparser, 'dist')

    args = parser.parse_args(argv[1:])
    if not args.cmd:
        parser.print_help()
        sys.exit(2)
    elif run_local == args.cmd:
        if not args.input_fn:
            lparser.print_help()
            sys.exit(2)
    elif run_dist == args.cmd:
        if not args.input_fn:
            dparser.print_help()
            sys.exit(2)
    return args

def main(argv=sys.argv):
    logging.basicConfig()
    args = parse_args(argv)
    args.cmd(args)

if __name__ == '__main__':  # pragma: no cover
    main()
