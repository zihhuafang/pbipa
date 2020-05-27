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
    exts = ['.fasta', '.fastq', '.bam', '.xml', '.fasta.gz', '.fastq.gz']
    for fn in abs_input_fns:
        for valid_ext in exts:
            if fn.endswith(valid_ext):
                break
        else:
            msg = f'Bad extension for "{os.path.basename(fn)}". Reads must have one of the following extensions: {exts}'
            raise RuntimeError(msg)

    # Write FOFN.
    reads_fn = os.path.join(args.run_dir, 'input.fofn')
    content = '\n'.join(abs_input_fns + [''])
    write_if_changed(reads_fn, content)

    args.input_fn = reads_fn

def normalize_args(args):
    """Except nthreads and njobs, which have different defaults
    for local versus dist.
    """
    args.run_dir = os.path.normpath(args.run_dir)
    args.phase_run = 1 if not args.no_phase else 0
    args.polish_run = 1 if not args.no_polish else 0
    args.cluster_args = getattr(args, 'cluster_args', '')
    args.resume = getattr(args, 'resume', True)

def validate(args):
    """Possibly create run_dir.
    """
    snake_dn = os.path.join(args.run_dir, '.snakemake')
    if os.path.isdir(snake_dn):
        if not args.resume:
            msg = f'Run-directory "{snake_dn}" exists. Remove and re-try. (Or use "--resume".)'
            raise RuntimeError(msg)

    lock_dn = os.path.join(args.run_dir, '.snakemake', 'locks')
    if os.path.isdir(lock_dn) and os.listdir(lock_dn):
        if not args.unlock:
            msg = f'Snakemake lock-directory "{lock_dn}" is not empty. Remove and re-try. (Or use "--unlock".)'
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

def choose_local_defaults(args, ncpus):
    """Update the fields of {args}, based on ncpus.
    Some or all might be {None}.
    """
    if args.njobs == 0 and args.nthreads == 0:
        msg = f"""
Please specify both '--njobs' and '--nthreads'.
"""
        raise RuntimeError(msg)

    if args.njobs > 0 and args.nthreads > 0:
        # Accept the choices of the user.
        if args.nthreads*args.njobs > ncpus:
            msg = f"""You may have over-subscribed your local machine.
We have detected only {ncpus} CPUs, but you have assumed {args.njobs*args.nthreads} are available.
(njobs*nthreads)==({args.njobs}*{args.nthreads})=={args.njobs*args.nthreads} > {ncpus}
"""
            LOG.warning(msg)
        return

    if args.njobs > ncpus or args.nthreads > ncpus:
        msg = f"""You have exceeded the number of CPUs in your machine.
We have detected only {ncpus} CPUs, but you have assumed {args.njobs*args.nthreads} are available.
(njobs*nthreads)==({args.njobs}*{args.nthreads})=={args.njobs*args.nthreads} > {ncpus}
"""
        raise RuntimeError(msg)
    elif args.njobs == 0 and args.nthreads != 0:
        assert type(args.nthreads) == int
        assert args.nthreads > 0
        args.njobs = ncpus // args.nthreads
    elif args.njobs != 0 and args.nthreads == 0:
        assert type(args.njobs) == int
        assert args.njobs > 0
        args.nthreads = ncpus // args.njobs
    else:
        # Not actually possible.
        raise AssertionError('not possible')

    if args.njobs * args.nthreads > ncpus:
        # Not actually possible, since we round down on division.
        msg = f"""You may have over-subscribed your local machine.
We have detected only {ncpus} CPUs, but you have assumed {args.njobs*args.nthreads} are available.
(njobs*nthreads)==({args.njobs}*{args.nthreads})=={args.njobs*args.nthreads} > {ncpus}
"""
        raise RuntimeError(msg)
    elif args.njobs * args.nthreads < ncpus:
        msg = f"""Because you choose a value that does not divide the number of CPUs, you are under-utilizing your local machine.
We have detected {ncpus} CPUs, but you have assumed {args.njobs*args.nthreads} are available.
(njobs*nthreads)==({args.njobs}*{args.nthreads})=={args.njobs*args.nthreads} < {ncpus}
"""
        raise RuntimeError(msg)

def run_local(args):
    args.cluster_args = None # ignored in local-mode

    normalize_args(args)
    choose_local_defaults(args, ncpus=NCPUS)
    validate(args)
    generate_fofn(args)
    config_fn = write_config(args)
    run(args, config_fn)

def run_dist(args):
    if not args.cluster_args:
        msg = 'Distributed mode requires --cluster string'
        raise RuntimeError(msg)

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
    if args.only_print:
        return
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


def get_version():
    try:
        import networkx
        import snakemake
    except ImportError as exc:
        LOG.exception('Try "pip3 install --user networkx snakemake"')

    return """
ipa (wrapper) version=1.0.3
"""

def add_common_options(parser, cmd='local'):
    parser.add_argument('--input-fn', '-i', type=str, action='append', default=[],
            help='(Required.) Input reads in FASTA, FASTQ, BAM, XML or FOFN formats. Repeat "-i fn1 -i fn2" for multiple inputs, or use a "file-of-filenames", e.g. "-i foo.fofn".')

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
                        help='(Required) Maximum number of threads to use per job. (Applies to both remote and local tasks.)')
    wf.add_argument('--nshards', type=int, default=40,
                        help='Maximum number of parallel tasks to split work into (though the number of simultaneous jobs could be much lower).')
    wf.add_argument('--tmp-dir', type=str, default='/tmp',
                        help='Temporary directory for some disk based operations like sorting.')
    wf.add_argument('--verbose', action='store_true',
            help='Extra logging for each task. (Show full env, e.g.)')

    snake = parser.add_argument_group('Snakemake options') #, 'These impact how snakemake is run.')
    snake.add_argument('--njobs', type=int, default=0,
                        help='(Required) Maximum number of simultaneous jobs, each running up to nthreads.')
    snake.add_argument('--run-dir', type=str, default='./RUN',
                        help='Directory in which to run snakemake.')
    snake.add_argument('--target', type=str, default='',
                        help='"finish" is implied, but you can use this to short-circuit.')
    snake.add_argument('--unlock', action='store_true',
                        help='Pass "--unlock" to snakemake, in case snakemake crashed earlier.')
    snake.add_argument('--dry-run', '-n', action='store_true',
                        help='Print the snakemake command and do a "dry run" quickly. Very useful!')
    snake.add_argument('--only-print', action='store_true',
                        help='Do not actually run snakemake. Simply print the snakemake command and exit.')
    if cmd == 'local':
        snake.add_argument('--resume', action='store_true',
                            help='Restart snakemake, but after regenerating the config file. In this case, run-dir may already exist. (Without --resume, run-dir must not already exist.)')
        snake.add_argument('--cluster-args', type=str, default=None,
                            help=argparse.SUPPRESS)
    else:
        assert cmd == 'dist'
        snake.add_argument('--resume', action='store_true',
                            help=argparse.SUPPRESS)
        snake.add_argument('--cluster-args', type=str, default='',
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
    parser.add_argument('--debug', action='store_true',
            help=argparse.SUPPRESS)

    subparsers = parser.add_subparsers(
            description='One of these must follow the options listed above and may be followed by sub-command specific options.',
            help='sub-command help')
    lparser = subparsers.add_parser('local',
            description='This sub-command runs snakemake in local-mode.',
            #epilog='',
            formatter_class=HelpF,
            help='Run IPA on your local machine.')
    dparser = subparsers.add_parser('dist',
            description='This sub-command runs snakemake in cluster-mode, i.e. with job-distribution.',
            #epilog='',
            formatter_class=HelpF,
            help='Distribute IPA jobs to your cluster.')
    vparser = subparsers.add_parser('validate',
            description='This sub-command shows the versions of dependencies.',
            #epilog='',
            formatter_class=HelpF,
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

def setup_logging(args):
    level = logging.DEBUG if args.debug else logging.INFO
    fmt='%(levelname)s: %(message)s'
    logging.basicConfig(format=fmt, level=level)
    if not hasattr(args, 'run_dir'):
        return
    if not args.run_dir:
        msg = 'Specified empty run-dir'
        raise RuntimeError(msg)
    if not os.path.isdir(args.run_dir):
        os.makedirs(args.run_dir)
    fn = os.path.join(args.run_dir, 'ipa.log')
    hdlr = logging.FileHandler(fn, mode='w')
    hdlr.setFormatter(logging.Formatter(fmt))
    LOG.addHandler(hdlr)

def main(argv=sys.argv):
    args = parse_args(argv)
    setup_logging(args)
    # cmdline = shlex.join(sys.argv) # py3.8
    cmdline = ' '.join(shlex.quote(arg) for arg in sys.argv)
    LOG.info(cmdline)
    if args.debug:
        args.cmd(args)
    else:
        try:
            args.cmd(args)
        except Exception as exc:
            LOG.error(str(exc) + '\nExiting.')
            sys.exit(1)

if __name__ == '__main__':  # pragma: no cover
    main()
