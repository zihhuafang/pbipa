import pytest
from argparse import Namespace
from ipa import *

def test_choose_local_defaults():
    # user specified both
    args = Namespace(njobs=16, nthreads=4)
    choose_local_defaults(args, 70)
    assert args.njobs == 16
    assert args.nthreads == 4

    # auto-calc njobs
    args = Namespace(njobs=0, nthreads=2)
    choose_local_defaults(args, 70)
    assert args.njobs == 35
    assert args.nthreads == 2

    # auto-calc nthreads
    args = Namespace(njobs=2, nthreads=0)
    choose_local_defaults(args, 70)
    assert args.njobs == 2
    assert args.nthreads == 35

    # auto-calc nthreads, but not a divisor in ncpus
    args = Namespace(njobs=9, nthreads=0)
    with pytest.raises(RuntimeError) as excinfo:
        choose_local_defaults(args, 70)
    assert 'under-utilizing' in str(excinfo.value)

    # auto-calc nthreads, but njobs was too high already
    args = Namespace(njobs=140, nthreads=0)
    with pytest.raises(RuntimeError) as excinfo:
        choose_local_defaults(args, 70)
    assert 'exceeded' in str(excinfo.value)

    # Require at least one.
    args = Namespace(njobs=0, nthreads=0)
    with pytest.raises(RuntimeError) as excinfo:
        choose_local_defaults(args, 70)
    assert 'Please specify both' in str(excinfo.value)
