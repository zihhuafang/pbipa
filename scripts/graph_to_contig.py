"""
TODO: (from convo w/ Ivan)
the issue with this script (but would still like to re-read it to refresh my memory). The script loads all edge sequences and tries to do two things at once: create p_ctg and a_ctg sequences, and align the bubbles using those sequences


If we generate:
1. All paths first (as tiling paths) for all p_ctg and all a_ctg without loading sequences - this should not consume much space (take a look at *_tiling_paths files).
2. Load the first read of each tiling path fully, and only edge sequences for every transition, we can generate the output sequences with the same memory/disk consumption.
3. Align bubbles after that.

Our resource consumption should be same

Bubbles?
It aligns them to produce the identity score

After that the dedup_a_tigs.py script is used to deduplicate fake a_ctg.
But that script is simple, and only depends on the alignment info that the previous script stored in the a_ctg header.
"""

import argparse
import logging
import sys
import networkx as nx
from falcon_kit.io import open_progress
import time

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


def yield_first_seq(one_path_edges, seqs):
    if one_path_edges and one_path_edges[0][0] != one_path_edges[-1][1]:
        # If non-empty, and non-circular,
        # prepend the entire first read.
        (vv, ww) = one_path_edges[0]
        (vv_rid, vv_letter) = vv.split(":")
        if vv_letter == 'E':
            first_seq = seqs[vv_rid]
        else:
            assert vv_letter == 'B'
            first_seq = "".join([RCMAP[c] for c in seqs[vv_rid][::-1]])
        yield first_seq

def compose_ctg(seqs, edge_data, ctg_id, path_edges, proper_ctg):
    total_score = 0
    total_length = 0
    edge_lines = []
    sub_seqs = []

    # If required, add the first read to the path sequence.
    if proper_ctg:
        sub_seqs = list(yield_first_seq(path_edges, seqs))
        total_length = 0 if len(sub_seqs) == 0 else len(sub_seqs[0])

    # Splice-in the rest of the path sequence.
    for vv, ww in path_edges:
        rid, s, t, aln_score, idt, e_seq = edge_data[(vv, ww)]
        sub_seqs.append(e_seq)
        edge_lines.append('%s %s %s %s %d %d %d %0.2f' % (
            ctg_id, vv, ww, rid, s, t, aln_score, idt))
        total_length += abs(s - t)
        total_score += aln_score

    return edge_lines, sub_seqs, total_score, total_length

def parse_seqdb_headers(fp_in, reads_in_layout):
    id_to_name = {}
    name_to_id = {}
    for line in fp_in:
        sl = line.strip().split()
        if sl[0] != 'S':
            continue
        seq_id = int(sl[1])
        seq_name = sl[2]
        seq_id_str = '%09d' % seq_id
        if seq_id_str not in reads_in_layout:
            continue
        id_to_name[seq_id_str] = seq_name
        name_to_id[seq_name] = seq_id_str
    return id_to_name, name_to_id

def load_reads(reads_fn, name_to_id=None):
    in_paths = [reads_fn]
    if reads_fn.lower().endswith('.fofn'):
        with open(reads_fn) as fp_in:
            in_paths = [line.strip() for line in fp_in]

    # Using a custom sequence reader because it supports both FASTA and FASTQ.
    seqs = {}

    # If a name_to_id map is provided, then use it find the IDs of the reads.
    # Otherwise, just use the header.
    if name_to_id:
        for record in yield_seq(in_paths):
            # Each record is a list of 2 or 4 elements, which correspond to FASTA/FASTQ lines.
            # The header also contains the '>' or '@' character.
            rname = record[0][1:].split()[0]
            # Value -1 is an invalid value for seqIDs, but using it as
            # a dummy value can therefore allow us to avoid a branching and
            # double dict lookups.
            # Nothing will match -1.
            seq_id = name_to_id.get(rname, -1)
            seqs[seq_id] = record[1]
    else:
        for record in yield_seq(in_paths):
            rname = record[0][1:].split()[0]
            seqs[rname] = record[1]

    return seqs

def run(improper_p_ctg, proper_a_ctg, preads_fasta_fn, seqdb_fn, sg_edges_list_fn, utg_data_fn, ctg_paths_fn):
    """improper==True => Neglect the initial read.
    We used to need that for unzip.
    """
    time_total = [time.time()]

    time_reads_in_layout = [time.time()]
    reads_in_layout = set()
    with open_progress(sg_edges_list_fn) as f:
        for l in f:
            l = l.strip().split()
            """001039799:E 000333411:E 000333411 17524 20167 17524 99.62 G"""
            v, w, rid, s, t, aln_score, idt, type_ = l
            if type_ != "G":
                continue
            r1 = v.split(":")[0]
            reads_in_layout.add(r1)
            r2 = w.split(":")[0]
            reads_in_layout.add(r2)
    time_reads_in_layout += [time.time()]
    log_time('reads_in_layout', time_reads_in_layout)

    time_parse_seqdb_headers = [time.time()]
    name_to_id = None
    if seqdb_fn:
        with open_progress(seqdb_fn) as fp_in:
            _, name_to_id = parse_seqdb_headers(fp_in, reads_in_layout)
    time_parse_seqdb_headers += [time.time()]
    log_time('parse_seqdb_headers', time_parse_seqdb_headers)

    time_load_reads = [time.time()]
    seqs = load_reads(preads_fasta_fn, name_to_id)
    time_load_reads += [time.time()]
    log_time('load_reads', time_load_reads)

    time_edge_data = [time.time()]
    edge_data = {}
    with open_progress(sg_edges_list_fn) as f:
        for l in f:
            l = l.strip().split()
            """001039799:E 000333411:E 000333411 17524 20167 17524 99.62 G"""
            v, w, rid, s, t, aln_score, idt, type_ = l

            if type_ != "G":
                continue
            r1, dir1 = v.split(":")
            reads_in_layout.add(r1) # redundant, but harmless
            r2, dir2 = w.split(":")
            reads_in_layout.add(r2) # redundant, but harmless

            s = int(s)
            t = int(t)
            aln_score = int(aln_score)
            idt = float(idt)

            if s < t:
                e_seq = seqs[rid][s:t]
                assert 'E' == dir2
            else:
                # t and s were swapped for 'c' alignments in ovlp_to_graph.generate_string_graph():702
                # They were translated from reverse-dir to forward-dir coordinate system in LA4Falcon.
                e_seq = "".join([RCMAP[c] for c in seqs[rid][t:s][::-1]])
                assert 'B' == dir2
            edge_data[(v, w)] = (rid, s, t, aln_score, idt, e_seq)
    time_edge_data += [time.time()]
    log_time('edge_data', time_edge_data)

    time_utg_data = [time.time()]
    utg_data = {}
    with open_progress(utg_data_fn) as f:
        for l in f:
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

    time_write_contigs = [time.time()]
    p_ctg_out = open("p_ctg.fasta", "w")
    a_ctg_out = open("a_ctg_all.fasta", "w")
    p_ctg_t_out = open("p_ctg_tiling_path", "w")
    a_ctg_t_out = open("a_ctg_all_tiling_path", "w")
    layout_ctg = set()
    with open_progress(ctg_paths_fn) as f:
        for l in f:
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

            #a_ctg_data = []
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

                    # a_ctg_data.append( (s, t, shortest_path) ) #first path is the same as the one used in the primary contig
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
                            #a_ctg_data.append( (s, t, shortest_path) )
                            all_alt_path.append((score, shortest_path))

                        except nx.exception.NetworkXNoPath:
                            break
                        # if len(shortest_path) < 2:
                        #    break
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
            p_edge_lines, p_ctg_seq_chunks, p_total_score, p_total_length = compose_ctg(seqs, edge_data, ctg_id, one_path_edges, (not improper_p_ctg))

            # Write out the tiling path.
            p_ctg_t_out.write('\n'.join(p_edge_lines))
            p_ctg_t_out.write('\n')

            # Write the sequence.
            # Using the `total_score` instead of `p_total_score` intentionally. Sum of
            # edge scores is not identical to sum of unitig scores.
            p_ctg_out.write('>%s %s %s %d %d\n' % (ctg_id, ctg_label, c_type_, p_total_length, total_score))
            p_ctg_out.write(''.join(p_ctg_seq_chunks))
            p_ctg_out.write('\n')

            a_id = 0
            for (v, w) in a_ctg_group.keys():
                atig_output = []

                # Compose the base sequence.
                for sub_id in range(len(a_ctg_group[(v, w)])):
                    score, atig_path = a_ctg_group[(v, w)][sub_id]
                    atig_path_edges = list(zip(atig_path[:-1], atig_path[1:]))

                    a_ctg_id = '%s-%03d-%02d' % (ctg_id, a_id + 1, sub_id)
                    a_edge_lines, sub_seqs, a_total_score, a_total_length = compose_ctg(
                        seqs, edge_data, a_ctg_id, atig_path_edges, proper_a_ctg)

                    seq = ''.join(sub_seqs)

                    # Keep the placeholder for these values for legacy purposes, but mark
                    # them as for deletion.
                    # The base a_ctg will also be output to the same file, for simplicity.
                    delta_len = 0
                    idt = 1.0
                    cov = 1.0
                    atig_output.append((v, w, atig_path, a_total_length, a_total_score, seq, atig_path_edges, a_ctg_id, a_edge_lines, delta_len, idt, cov))

                if len(atig_output) == 1:
                    continue

                for sub_id, data in enumerate(atig_output):
                    v, w, tig_path, a_total_length, a_total_score, seq, atig_path_edges, a_ctg_id, a_edge_lines, delta_len, a_idt, cov = data

                    # Write out the tiling path.
                    a_ctg_t_out.write('\n'.join(a_edge_lines))
                    a_ctg_t_out.write('\n')

                    # Write the sequence.
                    a_ctg_out.write('>%s %s %s %d %d %d %d %0.2f %0.2f\n' % (a_ctg_id, v, w, a_total_length, a_total_score, len(atig_path_edges), delta_len, idt, cov))
                    a_ctg_out.write(''.join(seq))
                    a_ctg_out.write('\n')

                a_id += 1
    a_ctg_out.close()
    p_ctg_out.close()
    a_ctg_t_out.close()
    p_ctg_t_out.close()
    time_write_contigs += [time.time()]
    log_time('write_contigs', time_write_contigs)

    time_total += [time.time()]
    log_time('TOTAL', time_total)

#######################################
#######################################
#######################################

"""
Observes the next num_chars characters from a given file handle.
Returns the characters (if possible) and returns the handle back.
"""
def peek(fp, num_chars):
    prev_pos = fp.tell()
    data = fp.read(num_chars)
    if len(data) == 0:
        return ''
    fp.seek(prev_pos, 0)
    return data

"""
Returns a single read from the given FASTA/FASTQ file.
Parameter header contains only the header of the read.
Parameter lines contains all lines of the read, which include:
- header
- seq
- '+' if FASTQ
- quals if FASTQ
Parameter lines is an array of strings, each for one component.
Please note that multiline FASTA/FASTQ entries (e.g. sequence line)
will be truncated into one single line.
Author: Ivan Sovic, 2015.
"""
def get_single_read(fp):
    lines = []

    STATE_HEADER = 0
    STATE_SEQ = 1
    STATE_QUAL_SEPARATOR = 2
    STATE_QUAL = 4
    state = STATE_HEADER      # State machine. States:
                                # 0 header, 1 seq, 2 '+' line, 3 quals.
    num_lines = 0
    header = ''
    seq = ''
    qual_separator = ''
    qual = ''
    lines = []

    next_char = peek(fp, 1)
    while (len(next_char) > 0):
        line = fp.readline().rstrip()
        next_char = peek(fp, 1)

        if (state == STATE_HEADER):
            if (len(line) == 0): continue
            header_separator = line[0]
            header = line[1:]         # Strip the '>' or '@' sign from the beginning.
            lines.append(line)
            next_state = STATE_SEQ
        elif (state == STATE_SEQ):
            seq += line
            if (len(next_char) == 0):
                lines.append(seq)
                next_state = STATE_HEADER
                break      # EOF.
            elif (header_separator == '>' and next_char == header_separator):
                lines.append(seq)
                next_state = STATE_HEADER
                break      # This function reads only one sequence.
            elif (header_separator == '@' and next_char == '+'):
                lines.append(seq)
                next_state = STATE_QUAL_SEPARATOR
            else:
                next_state = STATE_SEQ

        elif (state == STATE_QUAL_SEPARATOR):
            qual_separator = line
            lines.append(line)
            next_state = STATE_QUAL

        elif (state == STATE_QUAL):
            qual += line
            if (len(next_char) == 0):
                lines.append(qual)
                next_state = STATE_HEADER
                break      # EOF.
            elif (next_char == header_separator and len(qual) == len(seq)):
                lines.append(qual)
                next_state = STATE_HEADER
                break      # This function reads only one sequence.
            else:
                next_state = STATE_QUAL
        state = next_state

    return [header, lines]

def yield_seq(fofn_lines):
    """
    Yields a single sequence from a set of FASTA/FASTQ files provided
    by the list of file names.
    """
    for file_name in fofn_lines:
        with open(file_name) as fp_in:
            while(True):
                [header, seq] = get_single_read(fp_in)
                if (len(seq) == 0): break
                yield(seq)

#######################################
#######################################
#######################################

class HelpF(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass

def main(argv=sys.argv):
    sys.stderr.write("IPA2 version of graph_to_contig.\n")

    description = 'Generate the primary and alternate contig fasta files and tiling paths, given the string graph.'
    epilog = """
We write these:

    p_ctg_out = open("p_ctg.fasta", "w")
    a_ctg_out = open("a_ctg_all.fasta", "w")
    p_ctg_t_out = open("p_ctg_tiling_path", "w")
    a_ctg_t_out = open("a_ctg_all_tiling_path", "w")
"""
    parser = argparse.ArgumentParser(
            description=description,
            formatter_class=HelpF,
            epilog=epilog)
    parser.add_argument('--improper-p-ctg', action='store_true',
            help='Skip the initial read in each p_ctg path.')
    parser.add_argument('--proper-a-ctg', action='store_true',
            help='Skip the initial read in each a_ctg path.')
    parser.add_argument('--preads-fasta-fn', type=str,
            default='./preads4falcon.fasta',
            help='Input. Preads file, required to construct the contigs.')
    parser.add_argument('--seqdb-fn', type=str,
            default='',
            help='The SeqDB file to give a relation between ID->header.')
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
