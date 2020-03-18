"""
Author: Ivan Sovic
"""

#! /usr/bin/env python3

import os
import re
import sys
import shlex
import json
import argparse
import collections

default_config = """\
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
"""

def formatter_json(config_dict):
    return json.dumps(config_dict, sort_keys=True, indent=4, separators=(',', ': '))

def formatter_bash(config_dict):
    lines = []
    keys = sorted(config_dict.keys())
    for key in sorted(config_dict.keys()):
        val = shlex.quote(config_dict[key])
        # Accumulate the lines.
        line = '{}={}'.format(key, val)
        lines.append(line)
    ret = '\n'.join(lines)
    return ret

FORMATTER_LOOKUP = {
    'json': formatter_json,
    'bash': formatter_bash
}

def convert_config_to_dict(in_config):
    # Define the output config dict.
    config_dict = {}

    # Replace newlines with a ';' to make it uniform.
    in_config = ';'.join(in_config.split('\n'))

    sl_lines = in_config.split(';')

    # Fill out the dict.
    for line in sl_lines:
        line = line.strip()
        if not line:
            continue
        sl_line = line.split('=')
        assert len(sl_line) == 2, 'Malformed config option. Each config option needs to have exactly one "=" character, specifying the config option on the left side, and the value on the right side. Line: "{}"'.format(line)
        param_name = sl_line[0].strip()
        param_val = sl_line[1].strip()
        config_dict[param_name] = param_val

    return config_dict

def validate_param_names(valid_params, test_params):
    valid_params = set(valid_params)
    unknown_params = [param_name for param_name in test_params if param_name not in valid_params]
    assert len(unknown_params) == 0, 'Unknown config parameters specified: {}'.format(str(unknown_params))

def run(fp_in, out_formatter, out_fn, out_fmt):
    # Collect the input lines.
    in_str = ';'.join([line.strip() for line in fp_in])

    # Load the defaults.
    default_config_dict = convert_config_to_dict(default_config)

    # Load the user specified options.
    user_config_dict = convert_config_to_dict(in_str)

    # Validate the user config. There shouldn't be any keys which do not
    # appear in the default_config_dict.
    validate_param_names(default_config_dict.keys(), user_config_dict.keys())

    # Update the defaults with user specified values.
    config_dict = default_config_dict
    config_dict.update(user_config_dict)

    # Write the dict.
    out_str = out_formatter(config_dict)
    with open(out_fn, 'w') as fp_out:
        fp_out.write(out_str)
        fp_out.write('\n')

class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
   pass

def parse_args(argv):
    parser = argparse.ArgumentParser(description="Takes an advanced options string, and reformats it into JSON format. "\
                                    "Input/output is on stdin/stdout. Options which aren't set explicitly in the input "\
                                    "will be set to default (configurable via args).",
                                     formatter_class=HelpF)
    parser.add_argument('--out-fmt', type=str, default='json', choices=['json', 'bash'],
                        help='Output format of the config file.')
    parser.add_argument('--out-fn', type=str, default='generated.config.sh',
                        help='Output file.')
    args = parser.parse_args(argv[1:])
    return args

def main(argv=sys.argv):
    args = parse_args(argv)
    selected_formatter = FORMATTER_LOOKUP[args.out_fmt]
    run(sys.stdin, selected_formatter, **vars(args))

if __name__ == '__main__':  # pragma: no cover
    main()