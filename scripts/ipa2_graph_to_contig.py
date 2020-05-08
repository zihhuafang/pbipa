#! /usr/bin/env python3

import os
import argparse
import logging
import sys
import networkx as nx
import time
import contextlib

LOG = logging.getLogger(__name__)
RCMAP = dict(list(zip("ACGTacgtNn-", "TGCAtgcaNn-")))

def log(msg):
    sys.stderr.write(msg)
    sys.stderr.write('\n')

def time_diff_to_str(time_list):
    elapsed_time = time_list[1] - time_list[0]
    return time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

def log_time(label, time_list):
    LOG.info('Time for "{}": {}'.format(label, time_diff_to_str(time_list)))

def rc(seq):
    return "".join([RCMAP[c] for c in seq[::-1]])

def reverse_end(node_id):
    node_id, end = node_id.split(":")
    new_end = "B" if end == "E" else "E"
    return node_id + ":" + new_end

######################################################
### The open_progress, Percenter and FilePercenter ###
### were copied here from falcon_kit.io.           ###
### The filesize was copied from pypeflow.io.      ###
######################################################
class Percenter(object):
    """Report progress by golden exponential.

    Usage:
        counter = Percenter('mystruct', total_len(mystruct))

        for rec in mystruct:
            counter(len(rec))
    """
    def __init__(self, name, total, log=LOG.info, units='units'):
        if sys.maxsize == total:
            log('Counting {} from "{}"'.format(units, name))
        else:
            log('Counting {:,d} {} from\n  "{}"'.format(total, units, name))
        self.total = total
        self.log = log
        self.name = name
        self.units = units
        self.call = 0
        self.count = 0
        self.next_count = 0
        self.a = 1 # double each time
    def __call__(self, more, label=''):
        self.call += 1
        self.count += more
        if self.next_count <= self.count:
            self.a = 2 * self.a
            self.a = max(self.a, more)
            self.a = min(self.a, (self.total-self.count), round(self.total/10.0))
            self.next_count = self.count + self.a
            if self.total == sys.maxsize:
                msg = '{:>10} count={:15,d} {}'.format(
                    '#{:,d}'.format(self.call), self.count, label)
            else:
                msg = '{:>10} count={:15,d} {:6.02f}% {}'.format(
                    '#{:,d}'.format(self.call), self.count, 100.0*self.count/self.total, label)
            self.log(msg)
    def finish(self):
        self.log('Counted {:,d} {} in {} calls from:\n  "{}"'.format(
            self.count, self.units, self.call, self.name))


def FilePercenter(fn, log=LOG.info):
    if '-' == fn or not fn:
        size = sys.maxsize
    else:
        size = filesize(fn)
        if fn.endswith('.dexta'):
            size = size * 4
        elif fn.endswith('.gz'):
            size = sys.maxsize # probably 2.8x to 3.2x, but we are not sure, and higher is better than lower
            # https://stackoverflow.com/a/22348071
            # https://jira.pacificbiosciences.com/browse/TAG-2836
    return Percenter(fn, size, log, units='bytes')

@contextlib.contextmanager
def open_progress(fn, mode='r', log=LOG.info):
    """
    Usage:
        with open_progress('foo', log=LOG.info) as stream:
            for line in stream:
                use(line)

    That will log progress lines.
    """
    def get_iter(stream, progress):
        for line in stream:
            progress(len(line))
            yield line

    fp = FilePercenter(fn, log=log)
    with open(fn, mode=mode) as stream:
        yield get_iter(stream, fp)
    fp.finish()

def filesize(fn):
    """In bytes.
    Raise if fn does not exist.
    """
    return os.stat(fn).st_size
######################################################

def compose_tiling_paths(edge_data, ctg_id, path_edges):
    total_score = 0
    total_length = 0
    tiling_path_lines = []

    # Splice-in the rest of the path sequence.
    for vv, ww in path_edges:
        rid, s, t, aln_score, idt, e_seq, inphase = edge_data[(vv, ww)]
        tiling_path_lines.append('%s %s %s %s %d %d %d %0.2f %s' % (
            ctg_id, vv, ww, rid, s, t, aln_score, idt, inphase))
        total_length += abs(s - t)
        total_score += aln_score

    return tiling_path_lines, total_score, total_length

def run(sg_edges_list_fn, utg_data_fn, ctg_paths_fn):
    time_total = [time.time()]

    ### Load the string graph edge data.
    time_edge_data = [time.time()]
    edge_data = {}
    with open_progress(sg_edges_list_fn) as fp_in:
        for l in fp_in:
            l = l.strip().split()
            """001039799:E 000333411:E 000333411 17524 20167 17524 99.62 G"""
            v, w, rid, s, t, aln_score, idt, type_ = l[0:8]
            inphase = 'u' if len(l) < 9 else l[8]
            if type_ != "G":
                continue
            s = int(s)
            t = int(t)
            aln_score = int(aln_score)
            idt = float(idt)
            e_seq = None
            edge_data[(v, w)] = (rid, s, t, aln_score, idt, e_seq, inphase)
    time_edge_data += [time.time()]
    log_time('edge_data', time_edge_data)

    ### Load the unitig data.
    time_utg_data = [time.time()]
    utg_data = {}
    with open_progress(utg_data_fn) as fp_in:
        for l in fp_in:
            l = l.strip().split()
            s, v, t, type_, length, score, path_or_edges = l
            if type_ not in ["compound", "simple", "contained"]:
                continue
            length = int(length)
            score = int(score)
            if type_ in ("simple", "contained"):
                path_or_edges = path_or_edges.split("~")
            else:
                path_or_edges = [tuple(e.split("~"))
                                 for e in path_or_edges.split("|")]
            utg_data[(s, v, t)] = type_, length, score, path_or_edges
    time_utg_data += [time.time()]
    log_time('utg_data', time_utg_data)

    ### Produce tiling paths from contig annotations.
    time_write_contigs = [time.time()]
    layout_ctg = set()
    with open_progress(ctg_paths_fn) as fp_in, \
            open("p_ctg_tiling_path", "w") as fp_pctg_tp, \
            open("a_ctg_all_tiling_path", "w") as fp_actg_tp:
        for l in fp_in:
            l = l.strip().split()
            ctg_id, c_type_, i_utig, t0, length, score, utgs = l
            ctg_id = ctg_id
            s0 = i_utig.split("~")[0]

            if (reverse_end(t0), reverse_end(s0)) in layout_ctg:
                continue
            else:
                layout_ctg.add((s0, t0))

            ctg_label = i_utig + "~" + t0
            length = int(length)
            utgs = utgs.split("|")
            one_path = []
            total_score = 0
            total_length = 0

            a_ctg_group = {}

            for utg in utgs:
                s, v, t = utg.split("~")
                type_, length, score, path_or_edges = utg_data[(s, v, t)]
                total_score += score
                total_length += length
                if type_ == "simple":
                    if len(one_path) != 0:
                        one_path.extend(path_or_edges[1:])
                    else:
                        one_path.extend(path_or_edges)
                if type_ == "compound":

                    c_graph = nx.DiGraph()

                    all_alt_path = []
                    for ss, vv, tt in path_or_edges:
                        type_, length, score, sub_path = utg_data[(ss, vv, tt)]

                        v1 = sub_path[0]
                        for v2 in sub_path[1:]:
                            c_graph.add_edge(
                                v1, v2, e_score=edge_data[(v1, v2)][3])
                            v1 = v2

                    shortest_path = nx.shortest_path(c_graph, s, t, "e_score")
                    score = nx.shortest_path_length(c_graph, s, t, "e_score")
                    all_alt_path.append((score, shortest_path))

                    while 1:
                        n0 = shortest_path[0]
                        for n1 in shortest_path[1:]:
                            c_graph.remove_edge(n0, n1)
                            n0 = n1
                        try:
                            shortest_path = nx.shortest_path(
                                c_graph, s, t, "e_score")
                            score = nx.shortest_path_length(
                                c_graph, s, t, "e_score")
                            all_alt_path.append((score, shortest_path))

                        except nx.exception.NetworkXNoPath:
                            break

                    # Is sorting required, if we are appending the shortest paths in order?
                    all_alt_path.sort()
                    all_alt_path.reverse()
                    shortest_path = all_alt_path[0][1]

                    # The longest branch in the compound unitig is added to the primary path.
                    if len(one_path) != 0:
                        one_path.extend(shortest_path[1:])
                    else:
                        one_path.extend(shortest_path)

                    a_ctg_group[(s, t)] = all_alt_path

            if len(one_path) == 0:
                continue

            one_path_edges = list(zip(one_path[:-1], one_path[1:]))

            # Compose the primary contig.
            p_edge_lines, p_total_score, p_total_length = compose_tiling_paths(edge_data, ctg_id, one_path_edges)

            # Write out the tiling path.
            fp_pctg_tp.write('\n'.join(p_edge_lines))
            fp_pctg_tp.write('\n')

            a_id = 0
            for (v, w) in a_ctg_group.keys():
                atig_output = []

                # Compose the base sequence.
                for sub_id in range(len(a_ctg_group[(v, w)])):
                    score, atig_path = a_ctg_group[(v, w)][sub_id]
                    atig_path_edges = list(zip(atig_path[:-1], atig_path[1:]))

                    a_ctg_id = '%s-%03d-%02d' % (ctg_id, a_id + 1, sub_id)
                    a_edge_lines, a_total_score, a_total_length = compose_tiling_paths(
                        edge_data, a_ctg_id, atig_path_edges)

                    # Keep the placeholder for these values for legacy purposes, but mark
                    # them as for deletion.
                    # The base a_ctg will also be output to the same file, for simplicity.
                    delta_len = 0
                    idt = 1.0
                    cov = 1.0
                    seq = None
                    atig_output.append((v, w, atig_path, a_total_length, a_total_score, seq, atig_path_edges, a_ctg_id, a_edge_lines, delta_len, idt, cov))

                if len(atig_output) == 1:
                    continue

                for sub_id, data in enumerate(atig_output):
                    v, w, tig_path, a_total_length, a_total_score, seq, atig_path_edges, a_ctg_id, a_edge_lines, delta_len, a_idt, cov = data

                    # Write out the tiling path.
                    fp_actg_tp.write('\n'.join(a_edge_lines))
                    fp_actg_tp.write('\n')

                a_id += 1

    time_write_contigs += [time.time()]
    log_time('write_contigs', time_write_contigs)

    time_total += [time.time()]
    log_time('TOTAL', time_total)

class HelpF(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass

def main(argv=sys.argv):
    sys.stderr.write("IPA2 version of graph_to_contig.\n")

    description = 'Generate the primary and alternate contig tiling paths, given the string graph.'
    epilog = """
We write these:
    p_ctg_tiling_path
    a_ctg_all_tiling_path
"""
    parser = argparse.ArgumentParser(
            description=description,
            formatter_class=HelpF,
            epilog=epilog)
    parser.add_argument('--sg-edges-list-fn', type=str,
            default='./sg_edges_list',
            help='Input. File containing string graph edges, produced by ovlp_to_graph.py.')
    parser.add_argument('--utg-data-fn', type=str,
            default='./utg_data',
            help='Input. File containing unitig data, produced by ovlp_to_graph.py.')
    parser.add_argument('--ctg-paths-fn', type=str,
            default='./ctg_paths',
            help='Input. File containing contig paths, produced by ovlp_to_graph.py.')
    args = parser.parse_args(argv[1:])
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='[%(asctime)s %(levelname)s] %(msg)s', datefmt='%Y-%m-%d %H:%M:%S')
    run(**vars(args))

if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    main(sys.argv)
