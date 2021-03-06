#! /usr/bin/env python3

import networkx as nx
import argparse
import logging
import os
import random
import re
import shlex
import subprocess
import sys
import time

# Not sure if adds to stability, but at least adds determinism.
from collections import OrderedDict as dict

PYTHONHASHSEED = os.environ.get('PYTHONHASHSEED')
#random.seed(int(os.environ['PYTHONHASHSEED']))  # probably harmless but has no impact here
if PYTHONHASHSEED:
    import warnings
    warnings.warn('PYTHONHASHSEED={}'.format(PYTHONHASHSEED))

LOG = logging.getLogger(__name__)

###################################
### Ordered set implementation. ###
###################################
# http://code.activestate.com/recipes/576694/
import collections.abc

class OrderedSet(collections.abc.MutableSet):

    def __init__(self, iterable=None):
        self.end = end = []
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def update(self, other):
        for i in other:
            self.add(i)

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)

set = OrderedSet
###################################


class SGNode(object):
    """
    class representing a node in the string graph
    """

    def __init__(self, node_name):
        self.name = node_name
        self.out_edges = []
        self.in_edges = []

    def add_out_edge(self, out_edge):
        self.out_edges.append(out_edge)

    def add_in_edge(self, in_edge):
        self.in_edges.append(in_edge)


class SGEdge(object):
    """
    class representing an edge in the string graph
    """

    def __init__(self, in_node, out_node):
        self.in_node = in_node
        self.out_node = out_node
        self.attr = {}

    def set_attribute(self, attr, value):
        self.attr[attr] = value


def reverse_end(node_name):
    if (node_name == 'NA'):
        return node_name
    if (len(node_name) < 2 or (node_name[-2:] not in [':B', ':E'])):
        raise Exception(
            'Invalid node name. Node name passed to method: "{node_name}", expected format: "(%d)+:[BE]" or "NA".'.format(node_name=node_name))
    node_id, end = node_name.split(":")
    new_end = "B" if end == "E" else "E"
    return node_id + ":" + new_end


class StringGraph(object):
    """
    class representing the string graph
    """

    def __init__(self):
        self.nodes = {}
        self.edges = {}
        self.e_reduce = {}
        self.best_in = {}

    def add_node(self, node_name):
        """
        add a node into the graph by given a node name
        """
        if node_name not in self.nodes:
            self.nodes[node_name] = SGNode(node_name)

    def add_edge(self, in_node_name, out_node_name, **attributes):
        """
        add an edge into the graph by given a pair of nodes
        """
        if (in_node_name, out_node_name) not in self.edges:

            self.add_node(in_node_name)
            self.add_node(out_node_name)
            in_node = self.nodes[in_node_name]
            out_node = self.nodes[out_node_name]

            edge = SGEdge(in_node, out_node)
            self.edges[(in_node_name, out_node_name)] = edge
            in_node.add_out_edge(edge)
            out_node.add_in_edge(edge)
        edge = self.edges[(in_node_name, out_node_name)]
        for (k, v) in attributes.items():
            edge.attr[k] = v

    def init_reduce_dict(self):
        for e in self.edges:
            self.e_reduce[e] = False

    def bfs_nodes(self, n, exclude=None, depth=5):
        all_nodes = set()
        all_nodes.add(n)
        candidate_nodes = set()
        candidate_nodes.add(n)
        dp = 1
        while dp < depth and len(candidate_nodes) > 0:
            v = candidate_nodes.pop()
            for e in v.out_edges:
                w = e.out_node
                if w == exclude:
                    continue
                if w not in all_nodes:
                    all_nodes.add(w)
                    if len(w.out_edges) > 0:
                        candidate_nodes.add(w)
            dp += 1

        return all_nodes

    def mark_chimer_edges(self):

        multi_in_nodes = {}
        multi_out_nodes = {}
        for n_name in self.nodes:
            n = self.nodes[n_name]
            out_nodes = [e.out_node for e in n.out_edges if self.e_reduce[(
                e.in_node.name, e.out_node.name)] == False]
            in_nodes = [e.in_node for e in n.in_edges if self.e_reduce[(
                e.in_node.name, e.out_node.name)] == False]

            if len(out_nodes) >= 2:
                multi_out_nodes[n_name] = out_nodes
            if len(in_nodes) >= 2:
                multi_in_nodes[n_name] = in_nodes

        chimer_candidates = set()
        out_set = set()
        in_set = set()
        for n_name in multi_out_nodes:
            out_nodes = set(multi_out_nodes[n_name])
            out_set |= out_nodes

        for n_name in multi_in_nodes:
            in_nodes = set(multi_in_nodes[n_name])
            in_set |= in_nodes

        chimer_candidates = out_set & in_set

        chimer_nodes = []
        chimer_edges = set()
        for n in chimer_candidates: # sort, or OrderedSet
            out_nodes = set([e.out_node for e in n.out_edges])
            test_set = set()
            for in_node in [e.in_node for e in n.in_edges]:
                test_set = test_set | set(
                    [e.out_node for e in in_node.out_edges])
            test_set -= set([n])
            if len(out_nodes & test_set) == 0:
                flow_node1 = set()
                flow_node2 = set()
                for v in list(out_nodes):
                    flow_node1 |= self.bfs_nodes(v, exclude=n)
                for v in list(test_set):
                    flow_node2 |= self.bfs_nodes(v, exclude=n)
                if len(flow_node1 & flow_node2) == 0:
                    for e in n.out_edges:
                        v, w = e.in_node.name, e.out_node.name
                        if self.e_reduce[(v, w)] != True:
                            self.e_reduce[(v, w)] = True
                            chimer_edges.add((v, w))
                            rv = reverse_end(w)
                            rw = reverse_end(v)
                            self.e_reduce[(rv, rw)] = True
                            chimer_edges.add((rv, rw))

                    for e in n.in_edges:
                        v, w = e.in_node.name, e.out_node.name
                        if self.e_reduce[(v, w)] != True:
                            self.e_reduce[(v, w)] = True
                            chimer_edges.add((v, w))
                            rv = reverse_end(w)
                            rw = reverse_end(v)
                            self.e_reduce[(rv, rw)] = True
                            chimer_edges.add((rv, rw))
                    chimer_nodes.append(n.name)
                    chimer_nodes.append(reverse_end(n.name))

        return chimer_nodes, chimer_edges

    def mark_spur_edge(self):

        removed_edges = set()
        for v in self.nodes:
            if len([e for e in self.nodes[v].out_edges if self.e_reduce[(e.in_node.name, e.out_node.name)] != True]) > 1:
                for out_edge in self.nodes[v].out_edges:
                    w = out_edge.out_node.name

                    if len(self.nodes[w].out_edges) == 0 and self.e_reduce[(v, w)] != True:
                        self.e_reduce[(v, w)] = True
                        removed_edges.add((v, w))
                        v2, w2 = reverse_end(w), reverse_end(v)
                        self.e_reduce[(v2, w2)] = True
                        removed_edges.add((v2, w2))

            if len([e for e in self.nodes[v].in_edges if self.e_reduce[(e.in_node.name, e.out_node.name)] != True]) > 1:
                for in_edge in self.nodes[v].in_edges:
                    w = in_edge.in_node.name
                    if len(self.nodes[w].in_edges) == 0 and self.e_reduce[(w, v)] != True:
                        self.e_reduce[(w, v)] = True
                        removed_edges.add((w, v))
                        v2, w2 = reverse_end(w), reverse_end(v)
                        self.e_reduce[(w2, v2)] = True
                        removed_edges.add((w2, v2))
        return removed_edges

    def mark_tr_edges(self):
        """
        transitive reduction
        """
        n_mark = {}
        e_reduce = self.e_reduce
        FUZZ = 500
        for n in self.nodes:
            n_mark[n] = "vacant"

        for (n_name, node) in self.nodes.items():

            out_edges = node.out_edges
            if len(out_edges) == 0:
                continue

            out_edges.sort(key=lambda x: x.attr["length"])

            for e in out_edges:
                w = e.out_node
                n_mark[w.name] = "inplay"

            max_len = out_edges[-1].attr["length"]

            max_len += FUZZ

            for e in out_edges:
                e_len = e.attr["length"]
                w = e.out_node
                if n_mark[w.name] == "inplay":
                    w.out_edges.sort(key=lambda x: x.attr["length"])
                    for e2 in w.out_edges:
                        if e2.attr["length"] + e_len < max_len:
                            x = e2.out_node
                            if n_mark[x.name] == "inplay":
                                n_mark[x.name] = "eliminated"

            for e in out_edges:
                e_len = e.attr["length"]
                w = e.out_node
                w.out_edges.sort(key=lambda x: x.attr["length"])
                if len(w.out_edges) > 0:
                    x = w.out_edges[0].out_node
                    if n_mark[x.name] == "inplay":
                        n_mark[x.name] = "eliminated"
                for e2 in w.out_edges:
                    if e2.attr["length"] < FUZZ:
                        x = e2.out_node
                        if n_mark[x.name] == "inplay":
                            n_mark[x.name] = "eliminated"

            for out_edge in out_edges:
                v = out_edge.in_node
                w = out_edge.out_node
                if n_mark[w.name] == "eliminated":
                    e_reduce[(v.name, w.name)] = True
                    v_name, w_name = reverse_end(w.name), reverse_end(v.name)
                    e_reduce[(v_name, w_name)] = True
                n_mark[w.name] = "vacant"

    def mark_best_overlap(self):
        """
        find the best overlapped edges
        """

        best_edges = set()
        removed_edges = set()

        for v in self.nodes:

            out_edges = self.nodes[v].out_edges
            if len(out_edges) > 0:
                out_edges.sort(key=lambda e: -e.attr["score"])
                for e in out_edges:
                    if self.e_reduce[(e.in_node.name, e.out_node.name)] != True:
                        best_edges.add((e.in_node.name, e.out_node.name))
                        break

            in_edges = self.nodes[v].in_edges
            if len(in_edges) > 0:
                in_edges.sort(key=lambda e: -e.attr["score"])
                for e in in_edges:
                    if self.e_reduce[(e.in_node.name, e.out_node.name)] != True:
                        best_edges.add((e.in_node.name, e.out_node.name))
                        self.best_in[v] = e.in_node.name
                        break

        LOG.debug(f"X {len(best_edges)}")

        for (e_n, e) in self.edges.items():
            v = e_n[0]
            w = e_n[1]
            if self.e_reduce[(v, w)] != True:
                if (v, w) not in best_edges:
                    self.e_reduce[(v, w)] = True
                    removed_edges.add((v, w))
                    v2, w2 = reverse_end(w), reverse_end(v)
                    self.e_reduce[(v2, w2)] = True
                    removed_edges.add((v2, w2))

        return removed_edges

    def resolve_repeat_edges(self):

        edges_to_reduce = []
        nodes_to_test = set()
        for (v_n, v) in self.nodes.items():

            out_nodes = []
            for e in v.out_edges:
                if self.e_reduce[(e.in_node.name, e.out_node.name)] == False:
                    out_nodes.append(e.out_node.name)

            in_nodes = []
            for e in v.in_edges:
                if self.e_reduce[(e.in_node.name, e.out_node.name)] == False:
                    in_nodes.append(e.in_node.name)

            if len(out_nodes) == 1 and len(in_nodes) == 1:
                nodes_to_test.add(v_n)

        for v_n in list(nodes_to_test):

            v = self.nodes[v_n]

            out_nodes = []
            for e in v.out_edges:
                if self.e_reduce[(e.in_node.name, e.out_node.name)] == False:
                    out_nodes.append(e.out_node.name)

            in_nodes = []
            for e in v.in_edges:
                if self.e_reduce[(e.in_node.name, e.out_node.name)] == False:
                    in_nodes.append(e.in_node.name)

            in_node_name = in_nodes[0]

            for out_edge in self.nodes[in_node_name].out_edges:
                vv = out_edge.in_node.name
                ww = out_edge.out_node.name

                ww_out = self.nodes[ww].out_edges
                v_out = self.nodes[v_n].out_edges
                ww_out_nodes = set([n.out_node.name for n in ww_out])
                v_out_nodes = set([n.out_node.name for n in v_out])
                o_overlap = len(ww_out_nodes & v_out_nodes)

                ww_in_count = 0
                for e in self.nodes[ww].in_edges:
                    if self.e_reduce[(e.in_node.name, e.out_node.name)] == False:
                        ww_in_count += 1

                if ww != v_n and\
                   self.e_reduce[(vv, ww)] == False and\
                   ww_in_count > 1 and\
                   ww not in nodes_to_test and\
                   o_overlap == 0:
                    edges_to_reduce.append((vv, ww))

            out_node_name = out_nodes[0]

            for in_edge in self.nodes[out_node_name].in_edges:
                vv = in_edge.in_node.name
                ww = in_edge.out_node.name

                vv_in = self.nodes[vv].in_edges
                v_in = self.nodes[v_n].in_edges
                vv_in_nodes = set([n.in_node.name for n in vv_in])
                v_in_nodes = set([n.in_node.name for n in v_in])
                i_overlap = len(vv_in_nodes & v_in_nodes)

                vv_out_count = 0
                for e in self.nodes[vv].out_edges:
                    if self.e_reduce[(e.in_node.name, e.out_node.name)] == False:
                        vv_out_count += 1

                if vv != v_n and\
                   self.e_reduce[(vv, ww)] == False and\
                   vv_out_count > 1 and\
                   vv not in nodes_to_test and\
                   i_overlap == 0:
                    edges_to_reduce.append((vv, ww))

        removed_edges = set()
        for e in edges_to_reduce:
            self.e_reduce[e] = True
            removed_edges.add(e)

        return removed_edges

def reverse_edge(e):
    e1, e2 = e
    return reverse_end(e2), reverse_end(e1)


def reverse_path(p):
    p = p[::-1]
    return [reverse_end(n) for n in p]

def ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = True, undirected = False):
    if len(ug.edges()) == 0 or len(ug.nodes()) == 0:
        return ug.copy()

    dfs_queue = collections.deque()
    dfs_queue.append((start_node, 0, 0))

    seen_nodes = set()
    seen_edges = set()

    width_too_large = False
    max_depth = 0

    while len(dfs_queue) > 0:
        v, v_depth, v_pathlen = dfs_queue.popleft()
        seen_nodes.add(v)

        # The width should be computed with a global max depth if we're using
        # all of seen_edges in the numerator.
        max_depth = max(max_depth, v_depth)
        v_width = 0.0 if max_depth == 0 else float(len(seen_edges)) / float(v_depth)

        if depth_cutoff > 0 and v_depth > depth_cutoff:
            continue
        if length_cutoff > 0 and v_pathlen > length_cutoff:
            continue
        if width_cutoff > 0 and v_width > width_cutoff:
            width_too_large = True
            break
        if stop_on_convergence and v_depth > 0 and len(dfs_queue) == 0: # 0 because we popped.
            continue

        # Extend the DFS.
        for e in ug.out_edges(v, keys = True):
            if e in seen_edges:
                continue
            new_len = v_pathlen + u_edge_data[e][0]
            new_depth = v_depth + 1
            if new_depth > depth_cutoff or new_len > length_cutoff:
                continue
            seen_edges.add(e)
            # uu: source, vv: sink, kk: via node (key).
            uu, vv, kk = e
            if vv in seen_nodes:
                continue
            dfs_queue.append((vv, new_depth, new_len))

        # If the graph is undirected, then traverse the input edges too.
        if undirected == True:
            for e in ug.in_edges(v, keys = True):
                if e in seen_edges:
                    continue
                new_len = v_pathlen + u_edge_data[e][0]
                new_depth = v_depth + 1
                if new_depth > depth_cutoff or new_len > length_cutoff:
                    continue
                seen_edges.add(e)
                # uu: source, vv: sink, kk: via node (key).
                uu, vv, kk = e
                if uu in seen_nodes:
                    continue
                dfs_queue.append((uu, new_depth, new_len))

    if width_too_large:
        seen_nodes = set()
        seen_edges = set()

    local_graph = ug.edge_subgraph(list(seen_edges))

    return local_graph

def find_bundle(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, no_out_edge_printed):

    tips = set()
    bundle_edges = set()
    bundle_nodes = set()

    # Almost the entire runtime of this script is spent in the nx.ego_graph function when
    # the depth_cutoff is large.
    # The local_graph is used in several places in the code belo. In a couple of places
    # the out edges of each node are looked at. All out edges of ug are present in the
    # local graph (given that the depth_cutoff is satisfied), so this condition is ok.
    # There are two places where the in-edges are looked up. In one place, any in-edge
    # which is not in the length_to_node dict is just skipped, so here we don't mind
    # in edges that are not reachable from the start node.
    # The other place where in-edges are looked up to see if a potential node for
    # compound path extension has any edges which weren't looked up before.
    # IMPORTANT: Using the ego_graph would actually allow nodes in the middle of a bubble
    # to have edges with potentially distant regions of the genome, and we could form
    # bubbles that are not clean. By removing the ego_graph we actually make it more
    # stringent, and generate bubbles which shouldn't be able to connect to other places internally.
    #
    local_graph = nx.ego_graph(ug, start_node, depth_cutoff, undirected=False)
    # local_graph = ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = True, undirected = False)
    # local_graph = ug
    length_to_node = {start_node: 0}
    score_to_node = {start_node: 0}

    v = start_node
    end_node = start_node

    LOG.debug(f"\n\nstart {start_node}")

    bundle_nodes.add(v)
    for vv, ww, kk in local_graph.out_edges(v, keys=True):
        max_score = 0
        max_length = 0

        if (vv, ww, kk) not in bundle_edges and\
                reverse_end(ww) not in bundle_nodes:

            bundle_edges.add((vv, ww, kk))
            tips.add(ww)

    for v in list(tips):
        bundle_nodes.add(v)

    depth = 1
    width = 1.0
    converage = False

    while 1:
        LOG.debug(f"# of tips {len(tips)}")

        if len(tips) > 4:
            converage = False
            break

        if len(tips) == 1:
            end_node = tips.pop()

            LOG.debug(f"end {end_node}")

            if end_node not in length_to_node:
                v = end_node
                max_score_edge = None
                max_score = 0
                # The in_edges here don't have to be in Ego graph, because
                # they simply won't be in the length_to_node.
                for uu, vv, kk in local_graph.in_edges(v, keys=True):
                    if uu not in length_to_node:
                        continue

                    score = u_edge_data[(uu, vv, kk)][1]

                    if score > max_score:

                        max_score = score
                        max_score_edge = (uu, vv, kk)

                length_to_node[v] = length_to_node[max_score_edge[0]
                                                   ] + u_edge_data[max_score_edge][0]
                score_to_node[v] = score_to_node[max_score_edge[0]
                                                 ] + u_edge_data[max_score_edge][1]

            converage = True
            break

        depth += 1
        width = 1.0 * len(bundle_edges) / depth

        if depth > 10 and width > width_cutoff:
            converage = False
            break

        if depth > depth_cutoff:
            converage = False
            break

        tips_list = list(tips)

        tip_updated = False
        loop_detect = False
        length_limit_reached = False

        for v in tips_list:
            LOG.debug(f"process {v}")

            if len(local_graph.out_edges(v, keys=True)) == 0:  # dead end route
                if v not in no_out_edge_printed:
                    print("no out edge", v)
                    no_out_edge_printed.add(v)
                continue

            max_score_edge = None
            max_score = 0

            extend_tip = True

            for uu, vv, kk in local_graph.in_edges(v, keys=True):
                LOG.debug(f"in_edges {uu} {vv} {kk}")
                LOG.debug(f"{uu} in length_to_node {uu in length_to_node}")

                # A predecessor of this node was not processed before!
                # Node has incoming edges outside of bundle, or tips which are
                # it's predecessors and have not been processed yet.
                if uu not in length_to_node:
                    extend_tip = False
                    break

                score = u_edge_data[(uu, vv, kk)][1]

                if score > max_score:

                    max_score = score
                    max_score_edge = (uu, vv, kk)

            if extend_tip:

                length_to_node[v] = length_to_node[max_score_edge[0]
                                                   ] + u_edge_data[max_score_edge][0]
                score_to_node[v] = score_to_node[max_score_edge[0]
                                                 ] + u_edge_data[max_score_edge][1]

                if length_to_node[v] > length_cutoff:
                    length_limit_reached = True
                    converage = False
                    break

                v_updated = False
                for vv, ww, kk in local_graph.out_edges(v, keys=True):

                    LOG.debug(f"test {vv} {ww} {kk}")

                    if ww in length_to_node:
                        loop_detect = True
                        LOG.debug(f"loop_detect {ww}")
                        break

                    if (vv, ww, kk) not in bundle_edges and\
                            reverse_end(ww) not in bundle_nodes:

                        LOG.debug(f"add {ww}")

                        tips.add(ww)
                        bundle_edges.add((vv, ww, kk))
                        tip_updated = True
                        v_updated = True

                if v_updated:

                    LOG.debug(f"remove {v}")

                    tips.remove(v)

                    if len(tips) == 1:
                        break

            if loop_detect:
                converage = False
                break

        if length_limit_reached:
            converage = False
            break

        if loop_detect:
            converage = False
            break

        if not tip_updated:
            converage = False
            break

        for v in list(tips):
            bundle_nodes.add(v)

    data = start_node, end_node, bundle_edges, length_to_node[
        end_node], score_to_node[end_node], depth

    data_r = None

    LOG.debug(f"{converage} {data} {data_r}")
    return converage, data, data_r

def init_string_graph(overlap_data):
    sg = StringGraph()

    overlap_set = set()
    for od in overlap_data:
        f_id, g_id, score, identity = od[:4]
        f_s, f_b, f_e, f_l = od[4:8]
        g_s, g_b, g_e, g_l = od[8:12]
        # Valid in-phase options:
        #     i: in phase (keepers)
        #     x: not in phase (scraps)
        #     f: five prime overlaps were all phased - turns off phasing  (keepers)
        #     t: three prime overlaps were all phased - turns off phasing (keepers)
        #     n: no cross phase overlaps were removed (keepers)
        inphase = od[12]
        overlap_pair = [f_id, g_id]
        overlap_pair.sort()
        overlap_pair = tuple(overlap_pair)
        if overlap_pair in overlap_set:  # don't allow duplicated records
            continue
        else:
            overlap_set.add(overlap_pair)

        if g_s == 1:  # revered alignment, swapping the begin and end coordinates
            g_b, g_e = g_e, g_b

        # build the string graph edges for each overlap
        if f_b > 0:
            if g_b < g_e:
                """
                     f.B         f.E
                  f  ----------->
                  g         ------------->
                            g.B           g.E
                """
                if f_b == 0 or g_e - g_l == 0:
                    continue
                sg.add_edge("%s:B" % g_id, "%s:B" % f_id, label="%s:%d-%d"%(f_id, f_b, 0),
                            length=abs(f_b - 0),
                            score=-score,
                            identity=identity,
                            inphase=inphase)
                sg.add_edge("%s:E" % f_id, "%s:E" % g_id, label="%s:%d-%d"%(g_id, g_e, g_l),
                            length=abs(g_e - g_l),
                            score=-score,
                            identity=identity,
                            inphase=inphase)
            else:
                """
                     f.B         f.E
                  f  ----------->
                  g         <-------------
                            g.E           g.B
                """
                if f_b == 0 or g_e == 0:
                    continue
                sg.add_edge("%s:E" % g_id, "%s:B" % f_id, label="%s:%d-%d"%(f_id, f_b, 0),
                            length=abs(f_b - 0),
                            score=-score,
                            identity=identity,
                            inphase=inphase)
                sg.add_edge("%s:E" % f_id, "%s:B" % g_id, label="%s:%d-%d"%(g_id, g_e, 0),
                            length=abs(g_e - 0),
                            score=-score,
                            identity=identity,
                            inphase=inphase)
        else:
            if g_b < g_e:
                """
                                    f.B         f.E
                  f                 ----------->
                  g         ------------->
                            g.B           g.E
                """
                if g_b == 0 or f_e - f_l == 0:
                    continue
                sg.add_edge("%s:B" % f_id, "%s:B" % g_id, label="%s:%d-%d"%(g_id, g_b, 0),
                            length=abs(g_b - 0),
                            score=-score,
                            identity=identity,
                            inphase=inphase)
                sg.add_edge("%s:E" % g_id, "%s:E" % f_id, label="%s:%d-%d"%(f_id, f_e, f_l),
                            length=abs(f_e - f_l),
                            score=-score,
                            identity=identity,
                            inphase=inphase)
            else:
                """
                                    f.B         f.E
                  f                 ----------->
                  g         <-------------
                            g.E           g.B
                """
                if g_b - g_l == 0 or f_e - f_l == 0:
                    continue
                sg.add_edge("%s:B" % f_id, "%s:E" % g_id, label="%s:%d-%d"%(g_id, g_b, g_l),
                            length=abs(g_b - g_l),
                            score=-score,
                            identity=identity,
                            inphase=inphase)
                sg.add_edge("%s:B" % g_id, "%s:E" % f_id, label="%s:%d-%d"%(f_id, f_e, f_l),
                            length=abs(f_e - f_l),
                            score=-score,
                            identity=identity,
                            inphase=inphase)

    sg.init_reduce_dict()
    sg.mark_tr_edges()  # mark those edges that transitive redundant
    return sg

re_label = re.compile(r"(.*):(\d+)-(\d+)")

def init_digraph(sg, chimer_edges, removed_edges, spur_edges):
    nxsg = nx.DiGraph()
    edge_data = {}
    with open("sg_edges_list", "w") as out_f:
        for v, w in sg.edges: # sort, or OrderedDict
            e = sg.edges[(v, w)]
            label = e.attr["label"]
            score = e.attr["score"]
            identity = e.attr["identity"]
            length = e.attr["length"]
            inphase = e.attr["inphase"]
            try:
                mo = re_label.search(label)
                rid = mo.group(1)
                sp = int(mo.group(2))
                tp = int(mo.group(3))
            except Exception:
                msg = 'parsing label="{}"'.format(label)
                LOG.exception(msg)
                raise
            assert length == abs(sp - tp)

            if not sg.e_reduce[(v, w)]:
                type_ = "G"
            elif (v, w) in chimer_edges:
                type_ = "C"
            elif (v, w) in removed_edges:
                type_ = "R"
            elif (v, w) in spur_edges:
                type_ = "S"
            else:
                assert sg.e_reduce[(v, w)]
                type_ = "TR"

            if not sg.e_reduce[(v, w)]:
                assert label == "%s:%d-%d" % (rid, sp, tp)
                nxsg.add_edge(v, w, label=label, length=length, score=score)
                edge_data[(v, w)] = (rid, sp, tp, length, score, identity, type_, inphase)
                if w in sg.best_in:
                    nxsg.nodes[w]["best_in"] = v

            line = '%s %s %s %5d %5d %5d %5.2f %s %s' % (
                v, w, rid, sp, tp, score, identity, type_, inphase)
            print(line, file=out_f)

    return nxsg, edge_data


def yield_from_overlap_file(overlap_file):
    # loop through the overlapping data to load the data in the a python array

    with open(overlap_file) as f:
        for line in f:
            if line.startswith('-'):
                break
            l = line.strip().split()
            f_id, g_id, score, identity = l[:4]

            score = int(score)
            identity = float(identity)
            #contained_etc = l[12]
            f_strand, f_start, f_end, f_len = (int(c) for c in l[4:8])
            g_strand, g_start, g_end, g_len = (int(c) for c in l[8:12])
            inphase = 'u' if len(l) < 15 else l[14]

            yield (f_id, g_id, score, identity,
                                f_strand, f_start, f_end, f_len,
                                g_strand, g_start, g_end, g_len, inphase)

def generate_nx_string_graph(sg, lfc=False, disable_chimer_bridge_removal=False):
    LOG.debug("{}".format(sum([1 for c in sg.e_reduce.values() if c])))
    LOG.debug("{}".format(sum([1 for c in sg.e_reduce.values() if not c])))

    if not disable_chimer_bridge_removal:
        chimer_nodes, chimer_edges = sg.mark_chimer_edges()

        with open("chimers_nodes", "w") as f:
            for n in chimer_nodes:
                print(n, file=f)
        del chimer_nodes
    else:
        chimer_edges = set()  # empty set

    spur_edges = sg.mark_spur_edge()

    removed_edges = set()
    if lfc == True:
        removed_edges = sg.resolve_repeat_edges()
    else:
        # mark those edges that are best overlap edges
        removed_edges = sg.mark_best_overlap()

    spur_edges.update(sg.mark_spur_edge())

    LOG.debug('{}'.format(sum([1 for c in sg.e_reduce.values() if not c])))

    nxsg, edge_data = init_digraph(sg, chimer_edges, removed_edges, spur_edges)
    return nxsg, edge_data

def identify_branch_nodes(ug):

    branch_nodes = set()
    for n in ug.nodes():
        in_degree = len(ug.in_edges(n))
        out_degree = len(ug.out_edges(n))
        if in_degree > 1 or out_degree > 1:
            branch_nodes.add(n)

    return branch_nodes

def construct_compound_paths_0(ug, u_edge_data, branch_nodes, depth_cutoff, width_cutoff, length_cutoff):
    no_out_edge_printed = set()

    compound_paths_0 = []
    for p in list(branch_nodes):
        if ug.out_degree(p) > 1:
            coverage, data, data_r = find_bundle(
                ug, u_edge_data, p, depth_cutoff, width_cutoff, length_cutoff, no_out_edge_printed)
            if coverage == True:
                start_node, end_node, bundle_edges, length, score, depth = data
                compound_paths_0.append(
                    (start_node, "NA", end_node, 1.0 * len(bundle_edges) / depth, length, score, bundle_edges))

    compound_paths_0.sort(key=lambda x: -len(x[6]))
    return compound_paths_0

def construct_compound_paths_1(compound_paths_0):

    edge_to_cpath = {}
    compound_paths_1 = {}
    for s, v, t, width, length, score, bundle_edges in compound_paths_0:
        LOG.debug(f"constructing utg, test  {s} {v} {t}")

        overlapped = False
        for vv, ww, kk in list(bundle_edges):
            if (vv, ww, kk) in edge_to_cpath:
                LOG.debug(f"remove overlapped utg {(s, v, t)} {(vv, ww, kk)}")
                overlapped = True
                break
            rvv = reverse_end(vv)
            rww = reverse_end(ww)
            rkk = reverse_end(kk)
            if (rww, rvv, rkk) in edge_to_cpath:
                LOG.debug(f"remove overlapped r utg {(s, v, t)} {(rww, rvv, rkk)}")
                overlapped = True
                break

        if not overlapped:
            LOG.debug(f"constructing {s} {v} {t}")

            bundle_edges_r = []
            rs = reverse_end(t)
            rt = reverse_end(s)

            for vv, ww, kk in list(bundle_edges):
                edge_to_cpath.setdefault((vv, ww, kk), set())
                edge_to_cpath[(vv, ww, kk)].add((s, t, v))
                rvv = reverse_end(ww)
                rww = reverse_end(vv)
                rkk = reverse_end(kk)
                edge_to_cpath.setdefault((rvv, rww, rkk), set())
                edge_to_cpath[(rvv, rww, rkk)].add(
                    (rs, rt, v))  # assert v == "NA"
                bundle_edges_r.append((rvv, rww, rkk))

            compound_paths_1[(s, v, t)] = width, length, score, bundle_edges
            compound_paths_1[(rs, v, rt)
                             ] = width, length, score, bundle_edges_r
    return compound_paths_1

def construct_compound_paths_2(compound_paths_1):
    compound_paths_2 = {}
    edge_to_cpath = {}
    for s, v, t in compound_paths_1:
        rs = reverse_end(t)
        rt = reverse_end(s)
        if (rs, "NA", rt) not in compound_paths_1:
            LOG.debug(f"non_compliment bundle {s} {v} {t} {len(compound_paths_1[(s, v, t)][-1])}")
            continue
        width, length, score, bundle_edges = compound_paths_1[(s, v, t)]
        compound_paths_2[(s, v, t)] = width, length, score, bundle_edges
        for vv, ww, kk in list(bundle_edges):
            edge_to_cpath.setdefault((vv, ww, kk), set())
            edge_to_cpath[(vv, ww, kk)].add((s, t, v))
    return compound_paths_2, edge_to_cpath

def construct_compound_paths_3(ug, compound_paths_2, edge_to_cpath):
    compound_paths_3 = {}
    for (k, val) in compound_paths_2.items():

        start_node, NA, end_node = k
        rs = reverse_end(end_node)
        rt = reverse_end(start_node)
        assert (rs, "NA", rt) in compound_paths_2

        contained = False
        for vv, ww, kk in ug.out_edges(start_node, keys=True):
            if len(edge_to_cpath.get((vv, ww, kk), [])) > 1:
                contained = True

        if not contained:
            compound_paths_3[k] = val
            LOG.debug(f"compound {k}")
    return compound_paths_3

def construct_compound_paths(ug, u_edge_data, depth_cutoff, width_cutoff, length_cutoff):

    branch_nodes = identify_branch_nodes(ug)

    time_compound_paths_0 = [time.time()]
    compound_paths_0 = construct_compound_paths_0(ug, u_edge_data, branch_nodes, depth_cutoff, width_cutoff, length_cutoff)
    time_compound_paths_0 += [time.time()]
    log_time('  - compound_paths_0', time_compound_paths_0)

    time_compound_paths_1 = [time.time()]
    compound_paths_1 = construct_compound_paths_1(compound_paths_0)
    time_compound_paths_1 += [time.time()]
    log_time('  - compound_paths_1', time_compound_paths_1)

    time_compound_paths_2 = [time.time()]
    compound_paths_2, edge_to_cpath = construct_compound_paths_2(compound_paths_1)
    time_compound_paths_2 += [time.time()]
    log_time('  - compound_paths_2', time_compound_paths_2)

    time_compound_paths_3 = [time.time()]
    compound_paths_3 = construct_compound_paths_3(ug, compound_paths_2, edge_to_cpath)
    time_compound_paths_3 += [time.time()]
    log_time('  - compound_paths_3', time_compound_paths_3)

    time_compound_paths_update = [time.time()]
    compound_paths = {}
    for s, v, t in compound_paths_3:
        rs = reverse_end(t)
        rt = reverse_end(s)
        if (rs, "NA", rt) not in compound_paths_3:
            continue
        compound_paths[(s, v, t)] = compound_paths_3[(s, v, t)]
    time_compound_paths_update += [time.time()]
    log_time('  - compound_paths_update', time_compound_paths_update)

    return compound_paths

def identify_simple_paths(sg2, edge_data):
    # utg construction phase 1, identify all simple paths
    simple_paths = dict()
    s_nodes = set()
    t_nodes = set()
    simple_nodes = set()

    all_nodes = sg2.nodes()
    for n in all_nodes:
        in_degree = len(sg2.in_edges(n))
        out_degree = len(sg2.out_edges(n))
        if in_degree == 1 and out_degree == 1:
            simple_nodes.add(n)
        else:
            if out_degree != 0:
                s_nodes.add(n)
            if in_degree != 0:
                t_nodes.add(n)

    free_edges = set(sg2.edges())

    if LOG.getEffectiveLevel() >= logging.DEBUG:
        for s in list(simple_nodes):
            LOG.debug(f"simple_node {s}")
        for s in list(s_nodes):
            LOG.debug(f"s_node {s}")
        for s in list(t_nodes):
            LOG.debug(f"t_node {s}")

        for v, w in free_edges:
            if (reverse_end(w), reverse_end(v)) not in free_edges:
                LOG.debug(f"bug {v} {w}")
                print(reverse_end(w), reverse_end(v))

    while free_edges:
        if s_nodes:
            n = s_nodes.pop()
            LOG.debug(f"initial utg 1 {n}")
        else:
            e = free_edges.pop()
            free_edges.add(e)
            n = e[0]
            LOG.debug(f"initial utg 2 {n}")

        path = []
        path_length = 0
        path_score = 0
        for v, w in sg2.out_edges(n):
            if (v, w) not in free_edges:
                continue
            rv = reverse_end(v)
            rw = reverse_end(w)

            path_length = 0
            path_score = 0
            v0 = v
            w0 = w
            path = [v, w]
            path_edges = set()
            path_edges.add((v, w))
            path_length += edge_data[(v, w)][3]
            path_score += edge_data[(v, w)][4]
            free_edges.remove((v, w))

            r_path_length = 0
            r_path_score = 0
            rv0 = rv
            rw0 = rw
            r_path = [rv, rw]  # need to reverse again
            r_path_edges = set()
            r_path_edges.add((rw, rv))
            r_path_length += edge_data[(rw, rv)][3]
            r_path_score += edge_data[(rw, rv)][4]
            free_edges.remove((rw, rv))

            while w in simple_nodes:
                w, w_ = list(sg2.out_edges(w))[0]
                if (w, w_) not in free_edges:
                    break
                rw_, rw = reverse_end(w_), reverse_end(w)

                if (rw_, rw) in path_edges:
                    break

                path.append(w_)
                path_edges.add((w, w_))
                path_length += edge_data[(w, w_)][3]
                path_score += edge_data[(w, w_)][4]
                free_edges.remove((w, w_))

                r_path.append(rw_)
                r_path_edges.add((rw_, rw))
                r_path_length += edge_data[(rw_, rw)][3]
                r_path_score += edge_data[(rw_, rw)][4]
                free_edges.remove((rw_, rw))

                w = w_

            simple_paths[(v0, w0, path[-1])] = path_length, path_score, path
            r_path.reverse()
            assert r_path[0] == reverse_end(path[-1])
            simple_paths[(r_path[0], rw0, rv0)
                         ] = r_path_length, r_path_score, r_path

            LOG.debug(f"{path_length} {path_score} {path}")

            #dual_path[ (r_path[0], rw0, rv0) ] = (v0, w0, path[-1])
            #dual_path[ (v0, w0, path[-1]) ] = (r_path[0], rw0, rv0)
    return simple_paths


def identify_spurs(ug, u_edge_data, spur_len):
    # identify spurs in the utg graph
    # Currently, we use ad-hoc logic filtering out shorter utg, but we can
    # add proper alignment comparison later to remove redundant utgs
    # Side-effect: Modifies u_edge_data

    ug2 = ug.copy()

    s_candidates = set()
    for v in ug2.nodes():
        if ug2.in_degree(v) == 0:
            s_candidates.add(v)

    while len(s_candidates) > 0:
        n = s_candidates.pop()
        if ug2.in_degree(n) != 0:
            continue
        n_ego_graph = nx.ego_graph(ug2, n, radius=10)
        n_ego_node_set = set(n_ego_graph.nodes())
        for b_node in n_ego_graph.nodes():
            if ug2.in_degree(b_node) <= 1:
                continue

            with_extern_node = False
            b_in_nodes = [e[0] for e in ug2.in_edges(b_node)]

            if len(b_in_nodes) == 1:
                continue

            for v in b_in_nodes:
                if v not in n_ego_node_set:
                    with_extern_node = True
                    break

            if not with_extern_node:
                continue

            s_path = nx.shortest_path(ug2, n, b_node)
            v1 = s_path[0]
            total_length = 0
            for v2 in s_path[1:]:
                for s, t, v in ug2.out_edges(v1, keys=True):
                    if t != v2:
                        continue
                    length, score, edges, type_ = u_edge_data[(s, t, v)]
                    total_length += length
                v1 = v2

            if total_length >= spur_len:
                continue

            v1 = s_path[0]
            for v2 in s_path[1:]:
                for s, t, v in list(ug2.out_edges(v1, keys=True)):
                    if t != v2:
                        continue
                    length, score, edges, type_ = u_edge_data[(s, t, v)]
                    rs = reverse_end(t)
                    rt = reverse_end(s)
                    rv = reverse_end(v)
                    try:
                        ug2.remove_edge(s, t, key=v)
                        ug2.remove_edge(rs, rt, key=rv)
                        u_edge_data[(s, t, v)] = length, score, edges, "spur:2"
                        u_edge_data[(rs, rt, rv)
                                    ] = length, score, edges, "spur:2"
                    except Exception:
                        pass

                if ug2.in_degree(v2) == 0:
                    s_candidates.add(v2)
                v1 = v2
            break
    return ug2


def remove_dup_simple_path(ug, u_edge_data):
    # identify simple dup path
    # if there are many multiple simple path of length connect s and t, e.g.  s->v1->t, and s->v2->t, we will only keep one
    # Side-effect: Modifies u_edge_data
    ug2 = ug.copy()
    simple_edges = set()
    dup_edges = {}
    for s, t, v in u_edge_data:
        length, score, edges, type_ = u_edge_data[(s, t, v)]
        if len(edges) > 3:
            continue
        if type_ == "simple":
            if (s, t) in simple_edges:
                dup_edges[(s, t)].append(v)
            else:
                simple_edges.add((s, t))
                dup_edges[(s, t)] = [v]
    for (s, t) in dup_edges.keys():
        vl = dup_edges[(s, t)]
        vl.sort()
        for v in vl[1:]:
            ug2.remove_edge(s, t, key=v)
            length, score, edges, type_ = u_edge_data[(s, t, v)]
            u_edge_data[(s, t, v)] = length, score, edges, "simple_dup"
    return ug2


def construct_c_path_from_utgs(ug, u_edge_data, best_in_dict, use_bestin_heuristic):
    # Side-effects: None, I think.

    s_nodes = set()
    simple_nodes = set()
    simple_out = set()
    sources = set()
    sinks = set()

    all_nodes = ug.nodes()
    for n in all_nodes:
        in_degree = len(ug.in_edges(n))
        out_degree = len(ug.out_edges(n))
        if in_degree == 1 and out_degree == 1:
            simple_nodes.add(n)
        else:
            if out_degree != 0:
                s_nodes.add(n)
        if out_degree == 1:
            simple_out.add(n)
        if in_degree == 0 and out_degree > 0:
            sources.add(n)
        if in_degree > 0 and out_degree == 0:
            sinks.add(n)

    c_path = []

    free_edges = set()
    for s, t, v in ug.edges(keys=True):
        free_edges.add((s, t, v))

    while free_edges:
        if s_nodes:
            n = s_nodes.pop()
        else:
            e = free_edges.pop()
            n = e[0]

        for s, t, v in ug.out_edges(n, keys=True):
            path_start = n
            path_end = None
            path_key = None
            path = []
            path_length = 0
            path_score = 0
            path_nodes = set()
            path_nodes.add(s)
            LOG.debug(f"check 1 {s} {t} {v}")
            path_key = t
            t0 = s
            while t in simple_out:
                if t in path_nodes:
                    break
                rt = reverse_end(t)
                if rt in path_nodes:
                    break

                length, score, path_or_edges, type_ = u_edge_data[(t0, t, v)]

                """
                If the next node has two in-edges and the current path has the best overlap,
                we will extend the contigs. Otherwise, we will terminate the contig extension.
                This can help reduce some mis-assemblies but it can still construct long contigs
                when there is an oppertunity (assuming the best overlap has the highest
                likelihood to be correct.)
                """
                if len(ug.in_edges(t, keys=True)) > 1:
                    if use_bestin_heuristic:
                        best_in_node = best_in_dict[t]

                        if type_ == "simple" and best_in_node != path_or_edges[-2]:
                            break
                        if type_ == "compound":
                            t_in_nodes = set()
                            for ss, vv, tt in path_or_edges:
                                if tt != t:
                                    continue
                                length, score, path_or_edges, type_ = u_edge_data[(
                                    ss, vv, tt)]
                                if path_or_edges[-1] == tt:
                                    t_in_nodes.add(path_or_edges[-2])
                            if best_in_node not in t_in_nodes:
                                break
                    else:
                        break
                # ----------------

                path.append((t0, t, v))
                path_nodes.add(t)
                path_length += length
                path_score += score
                # t is "simple_out" node
                assert len(ug.out_edges(t, keys=True)) == 1
                t0, t, v = list(ug.out_edges(t, keys=True))[0]

            path.append((t0, t, v))
            length, score, path_or_edges, type_ = u_edge_data[(t0, t, v)]
            path_length += length
            path_score += score
            path_nodes.add(t)
            path_end = t
            is_spur = True if path_start in sources else False

            c_path.append((path_start, path_key, path_end,
                           path_length, path_score, path, len(path), is_spur))
            LOG.debug(f"c_path {path_start} {path_key} {path_end} {path_length} {path_score} {len(path)}")
            for e in path:
                if e in free_edges:
                    free_edges.remove(e)

    LOG.debug(f"left over edges: {len(free_edges)}")
    return c_path

def extract_contigs(ug, u_edge_data, c_path, circular_path, ctg_prefix):
    free_edges = set()
    for s, t, v in ug.edges(keys=True):
        free_edges.add((s, t, v))

    ctg_id = 0

    for path_start, path_key, path_end, p_len, p_score, path, n_edges, is_spur in c_path:
        length = 0
        score = 0
        length_r = 0
        score_r = 0

        non_overlapped_path = []
        non_overlapped_path_r = []
        for s, t, v in path:
            if v != "NA":
                rs, rt, rv = reverse_end(t), reverse_end(s), reverse_end(v)
            else:
                rs, rt, rv = reverse_end(t), reverse_end(s), "NA"
            if (s, t, v) in free_edges and (rs, rt, rv) in free_edges:
                non_overlapped_path.append((s, t, v))
                non_overlapped_path_r.append((rs, rt, rv))
                length += u_edge_data[(s, t, v)][0]
                score += u_edge_data[(s, t, v)][1]
                length_r += u_edge_data[(rs, rt, rv)][0]
                score_r += u_edge_data[(rs, rt, rv)][1]
            else:
                break

        if len(non_overlapped_path) == 0:
            continue
        s0, t0, v0 = non_overlapped_path[0]
        end_node = non_overlapped_path[-1][1]

        c_type_ = "ctg_linear" if (end_node != s0) else "ctg_circular"

        ctg_name = '%s%06dF' % (ctg_prefix, ctg_id)
        new_contig = (ctg_name, c_type_, s0 + "~" + v0 + "~" + \
                        t0, end_node, length, score, "|".join(
                        [c[0] + "~" + c[2] + "~" + c[1] for c in non_overlapped_path]))
        yield new_contig

        non_overlapped_path_r.reverse()
        s0, t0, v0 = non_overlapped_path_r[0]
        end_node = non_overlapped_path_r[-1][1]

        ctg_name = '%s%06dR' % (ctg_prefix, ctg_id)
        new_contig = (ctg_name, c_type_, s0 + "~" + v0 + "~" + \
                        t0, end_node, length_r, score_r, "|".join(
                        [c[0] + "~" + c[2] + "~" + c[1] for c in non_overlapped_path_r]))
        yield new_contig

        ctg_id += 1
        for e in non_overlapped_path:
            if e in free_edges:
                free_edges.remove(e)
        for e in non_overlapped_path_r:
            if e in free_edges:
                free_edges.remove(e)

    for s, t, v in list(circular_path):
        length, score, path, type_ = u_edge_data[(s, t, v)]
        ctg_name = '%s%d' % (ctg_prefix, ctg_id)
        new_contig = (ctg_name, "ctg_circular", s + \
                        "~" + v + "~" + t, t, length, score, s + "~" + v + "~" + t)
        yield new_contig
        ctg_id += 1

def identify_edges_to_remove(compound_paths, ug2):
    ug2_edges = set(ug2.edges(keys=True))
    edges_to_remove = set()
    with open("c_path", "w") as f:
        for s, v, t in compound_paths:
            width, length, score, bundle_edges = compound_paths[(s, v, t)]
            print(s, v, t, width, length, score, "|".join(
                [e[0] + "~" + e[2] + "~" + e[1] for e in bundle_edges]), file=f)
            for ss, tt, vv in bundle_edges:
                if (ss, tt, vv) in ug2_edges:
                    edges_to_remove.add((ss, tt, vv))
    return edges_to_remove

def generic_nx_to_gfa(fp_out, graph, use_keys=False, node_len_dict=None):
    line = 'H\tVN:Z:1.0'
    fp_out.write(line + '\n')

    if node_len_dict != None:
        for v in graph.nodes():
            line = 'S\t%s\t%s\tLN:i:%d' % (v, '*', node_len_dict[v])
            fp_out.write(line + '\n')
    else:
        for v in graph.nodes():
            line = 'S\t%s\t%s\tLN:i:%d' % (v, '*', 1000)
            fp_out.write(line + '\n')
    for v, w in graph.edges():
        line = 'L\t%s\t+\t%s\t+\t0M' % (v, w)
        fp_out.write(line + '\n')

def unitig_nx_to_gfa(fp_out, ug, u_edge_data):
    # Create a dual graph where ug edges are represented as nodes,
    # and they are connected via edges which connect the first/last nodes
    # each unitig.
    inlets = collections.defaultdict(list)
    outlets = collections.defaultdict(list)
    nodes = collections.defaultdict(list)
    for s, t, v in ug.edges(keys = True):
        length, score, edges, type_ = u_edge_data[(s, t, v)]
        node_name = '~'.join([s, v, t])
        new_node = (s, v, t, length, score, edges, type_)
        nodes[node_name] = new_node
        inlets[s].append(node_name)
        outlets[t].append(node_name)

    for node_name, node_data in nodes.items():
        s, v, t, length, score, edges, type_ = node_data
        line = 'S\t%s\t%s\tLN:i:%d' % (node_name, '*', length)
        fp_out.write(line + '\n')

    edges = {}
    for s, t, v in ug.edges(keys = True):
        node_name = '~'.join([s, v, t])

        for w in inlets[t]:
            edges[(node_name, w)] = 'L\t%s\t+\t%s\t+\t0M' % (node_name, w)

        for w in outlets[s]:
            edges[(w, node_name)] = 'L\t%s\t+\t%s\t+\t0M' % (w, node_name)

    for key, val in edges.items():
        fp_out.write(val + '\n')

def identify_short_edges_to_remove(ug2, u_edge_data):
    edges_to_remove = set()
    for s, t, v in ug2.edges(keys=True):
        if ug2.in_degree(s) == 1 and ug2.out_degree(s) == 2 and \
            ug2.in_degree(t) == 2 and ug2.out_degree(t) == 1:
            length, score, path_or_edges, type_ = u_edge_data[(s, t, v)]
            if length < 60000:
                rs = reverse_end(t)
                rt = reverse_end(s)
                rv = reverse_end(v)
                edges_to_remove.add((s, t, v))
                edges_to_remove.add((rs, rt, rv))
    return edges_to_remove

def init_sg2(edge_data):
    sg2 = nx.DiGraph()
    for (v, w) in edge_data.keys():
        assert (reverse_end(w), reverse_end(v)) in edge_data
        # if (v, w) in masked_edges:
        #    continue
        rid, sp, tp, length, score, identity, type_, inphase = edge_data[(v, w)]
        if type_ != "G":
            continue
        label = "%s:%d-%d" % (rid, sp, tp)
        sg2.add_edge(v, w, label=label, length=length, score=score)
    return sg2

def print_edge_data(u_edge_data):
    with open("utg_data", "w") as f:
        for s, t, v in u_edge_data:
            length, score, path_or_edges, type_ = u_edge_data[(s, t, v)]

            if v == "NA":
                path_or_edges = "|".join(
                    [ss + "~" + vv + "~" + tt for ss, tt, vv in path_or_edges])
            else:
                path_or_edges = "~".join(path_or_edges)
            print(s, v, t, type_, length, score, path_or_edges, file=f)

def print_utg_data0(u_edge_data):
    with open("utg_data0", "w") as f:
        for s, t, v in u_edge_data:
            rs = reverse_end(t)
            rt = reverse_end(s)
            rv = reverse_end(v)
            assert (rs, rt, rv) in u_edge_data
            length, score, path_or_edges, type_ = u_edge_data[(s, t, v)]

            if type_ == "compound":
                path_or_edges = "|".join(
                    [ss + "~" + vv + "~" + tt for ss, tt, vv in path_or_edges])
            else:
                path_or_edges = "~".join(path_or_edges)
            print(s, v, t, type_, length, score, path_or_edges, file=f)

def time_diff_to_str(time_list):
    elapsed_time = time_list[1] - time_list[0]
    return time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

def log_time(label, time_list):
    LOG.info('Time for "{}": {}'.format(label, time_diff_to_str(time_list)))

def ovlp_to_graph(args):
    time_total = [time.time()]

    time_yield_from_overlap = [time.time()]
    overlap_data = yield_from_overlap_file(args.overlap_file)
    time_yield_from_overlap += [time.time()]
    log_time('yield_from_overlap_file', time_yield_from_overlap)

    # transitivity reduction
    time_init_sg = [time.time()]
    sg = init_string_graph(overlap_data)
    time_init_sg += [time.time()]
    log_time('init_string_graph', time_init_sg)

    # remove spurs, remove putative edges caused by repeats
    time_generate_nx = [time.time()]
    nxsg, edge_data = generate_nx_string_graph(sg, args.lfc, args.disable_chimer_bridge_removal)
    del sg, overlap_data
    time_generate_nx += [time.time()]
    log_time('generate_nx_string_graph', time_generate_nx)

    #dual_path = {}
    time_init_sg2 = [time.time()]
    nxsg2 = init_sg2(edge_data)
    time_init_sg2 += [time.time()]
    log_time('init_sg2', time_init_sg2)

    time_ug_simple_paths = [time.time()]
    ug = nx.MultiDiGraph()
    u_edge_data = {}
    circular_path = set()
    simple_paths = identify_simple_paths(nxsg2, edge_data)
    for s, v, t in simple_paths:
        length, score, path = simple_paths[(s, v, t)]
        u_edge_data[(s, t, v)] = (length, score, path, "simple")
        if s != t:
            ug.add_edge(s, t, key=v, type_="simple",
                        via=v, length=length, score=score)
        else:
            circular_path.add((s, t, v))
    if LOG.getEffectiveLevel() >= logging.DEBUG:
        print_utg_data0(u_edge_data)
    time_ug_simple_paths += [time.time()]
    log_time('ug_simple_paths', time_ug_simple_paths)

    time_identify_spurs_1 = [time.time()]
    ug2 = identify_spurs(ug, u_edge_data, 50000)
    time_identify_spurs_1 += [time.time()]
    log_time('identify_spurs-1', time_identify_spurs_1)

    time_remove_dup_simple = [time.time()]
    ug2 = remove_dup_simple_path(ug2, u_edge_data)
    time_remove_dup_simple += [time.time()]
    log_time('remove_dup_simple_path', time_remove_dup_simple)

    # phase 2, finding all "consistent" compound paths
    time_construct_compound_paths = [time.time()]
    compound_paths = construct_compound_paths(ug2, u_edge_data, args.depth_cutoff, args.width_cutoff, args.length_cutoff)
    time_construct_compound_paths += [time.time()]
    log_time('construct_compound_paths', time_construct_compound_paths)

    time_edges_to_remove = [time.time()]
    edges_to_remove = identify_edges_to_remove(compound_paths, ug2)
    for s, t, v in edges_to_remove:
        ug2.remove_edge(s, t, v)
        length, score, edges, type_ = u_edge_data[(s, t, v)]
        if type_ != "spur":
            u_edge_data[(s, t, v)] = length, score, edges, "contained"
    time_edges_to_remove += [time.time()]
    log_time('edges_to_remove', time_edges_to_remove)

    time_compound_add_edges = [time.time()]
    for s, v, t in compound_paths:
        width, length, score, bundle_edges = compound_paths[(s, v, t)]
        u_edge_data[(s, t, v)] = (length, score, bundle_edges, "compound")
        ug2.add_edge(s, t, key=v, via=v, type_="compound",
                     length=length, score=score)
        assert v == "NA"
        rs = reverse_end(t)
        rt = reverse_end(s)
        assert (rs, v, rt) in compound_paths
        #dual_path[ (s, v, t) ] = (rs, v, rt)
        #dual_path[ (rs, v, rt) ] = (s, v, t)
    time_compound_add_edges += [time.time()]
    log_time('compound_add_edges', time_compound_add_edges)

    # remove short utg using local flow consistent rule
    r"""
      short UTG like this can be removed, this kind of utg are likely artifects of repeats
      >____           _____>
           \__UTG_>__/
      <____/         \_____<
    """
    time_short_edges_to_remove = [time.time()]
    short_edges_to_remove = identify_short_edges_to_remove(ug2, u_edge_data)
    for s, t, v in list(short_edges_to_remove):
        ug2.remove_edge(s, t, key=v)
        length, score, edges, type_ = u_edge_data[(s, t, v)]
        u_edge_data[(s, t, v)] = length, score, edges, "repeat_bridge"
    time_short_edges_to_remove += [time.time()]
    log_time('short_edges_to_remove', time_short_edges_to_remove)

    # Repeat the aggresive spur filtering with slightly larger spur length.
    time_identify_spurs_2 = [time.time()]
    ug = identify_spurs(ug2, u_edge_data, 80000)
    print_edge_data(u_edge_data)
    time_identify_spurs_2 += [time.time()]
    log_time('identify_spurs-2', time_short_edges_to_remove)

    time_write_ug = [time.time()]
    with open('ug.final.gfa', 'w') as fp_out:
        generic_nx_to_gfa(fp_out, ug, False, None)
    with open('ug.final.dual.gfa', 'w') as fp_out:
        unitig_nx_to_gfa(fp_out, ug, u_edge_data)
    time_write_ug += [time.time()]
    log_time('write-ug', time_write_ug)

    # Create a dict for every non-trivial unitig node, where the key is the
    # node and the value is the best input node. This is stored in the
    # string graph in the legacy code.
    # This is used to resolve ambiguities during contig extraction by
    # prefering the best scoring path.
    # Here we simply copy the best_in node as it is in the nxsg.
    # For the legacy code, this dict will be used as is.
    # For the haplospur feature, some nodes in this dict will be updated
    # to represent the new best_in node.
    best_in_dict = {}
    for v in nxsg.nodes():
        v_data = nxsg.nodes[v]
        if 'best_in' in v_data:
            best_in_dict[v] = v_data["best_in"]

    if args.haplospur:
        # Contig construction without extending through ambiguous regions to identify forks.
        # These would be simple contigs.
        # This is needed to figure out the path lengths for each branch in an ambiguous node.
        time_haplospur_construct_ctg_paths_1 = [time.time()]
        simple_ctg_paths = construct_c_path_from_utgs(ug, u_edge_data, None, False)
        time_haplospur_construct_ctg_paths_1 += [time.time()]
        log_time('haplospur_construct_c_path_from_utgs_1', time_haplospur_construct_ctg_paths_1)

        # Create a graph of simple contigs, and find ambiguous connections with short spurs.
        # If these are found, then modify the best_in scores for the adjacent reads to prevent
        # primary contig extraction into the spurs.
        time_haplospur_find_best_in = [time.time()]
        find_best_in_for_simple_ctg_paths(simple_ctg_paths, ug, u_edge_data, nxsg, best_in_dict)
        time_haplospur_find_best_in += [time.time()]
        log_time('haplospur_find_best_in_for_simple_ctg_paths', time_haplospur_find_best_in)

        # for key in sorted(best_in_dict.keys()):
        #     sys.stderr.write('(haplospur) v = {} -> best_in = {}\n'.format(key, best_in_dict[key]))

        # Find the final contig paths using the best_in dict filter.
        # This should prevent small spurs (unmerged haplotig bubbles) in the middle
        # of long contigs to break those contigs.
        time_haplospur_construct_ctg_paths_2 = [time.time()]
        c_path = construct_c_path_from_utgs(ug, u_edge_data, best_in_dict, True)
        # Sorting contig paths by length.
        c_path.sort(key=lambda x: -x[3])
        time_haplospur_construct_ctg_paths_2 += [time.time()]
        log_time('haplospur_construct_c_path_from_utgs_2', time_haplospur_construct_ctg_paths_2)
    else:
        # contig construction from utgs
        time_construct_c_path_from_utgs = [time.time()]
        c_path = construct_c_path_from_utgs(ug, u_edge_data, best_in_dict, True)
        # Sorting contig paths by length.
        c_path.sort(key=lambda x: -x[3])
        time_construct_c_path_from_utgs += [time.time()]
        log_time('construct_c_path_from_utgs', time_construct_c_path_from_utgs)

    # Construct the contigs (based on unitigs).
    time_extract_contigs = [time.time()]
    contigs = extract_contigs(ug, u_edge_data, c_path, circular_path, args.ctg_prefix)
    time_extract_contigs += [time.time()]
    log_time('extract_contigs', time_extract_contigs)

    # Write contigs to file.
    time_write_ctg_paths = [time.time()]
    with open('ctg_paths', 'w') as fp_out:
        for contig_tuple in contigs:
            fp_out.write(' '.join([str(val) for val in contig_tuple]))
            fp_out.write('\n')
    time_write_ctg_paths += [time.time()]
    log_time('ctg_paths', time_write_ctg_paths)

    time_total += [time.time()]
    log_time('TOTAL', time_total)

def find_best_in_for_simple_ctg_paths(simple_ctg_paths, ug, u_edge_data, sg, best_in_dict):
    def print_cg_edge_data(ss, tt, vv):
        e_data = cg.get_edge_data(ss, tt, key=vv)
        ss, vv, tt, p_len, p_score, path, n_edges, is_spur = e_data['data']
        length = e_data['length']
        return('(s = {}, t = {}, v = {}), p_len = {}, p_score = {}, n_edges = {}, is_spur = {}, length = {}'.format(ss, vv, tt, p_len, p_score, n_edges, is_spur, length))

    def get_next_to_last_nodes_from_cg_edge(cg, ss, tt, vv):
        predecessor_nodes_in_sg = set()

        e_data = cg.get_edge_data(ss, tt, key=vv)
        s, v, t, p_len, p_score, path, n_edges, is_spur = e_data['data']

        last_utg = path[-1]
        utg_s, utg_t, utg_v = last_utg
        utg_length, utg_score, utg_path_or_edges, utg_type_ = u_edge_data[(utg_s, utg_t, utg_v)]

        # Find all predecessors.
        if utg_type_ == "simple":
            predecessor_nodes_in_sg.add(utg_path_or_edges[-2])
        elif utg_type_ == "compound":
            for ss, vv, tt in utg_path_or_edges:
                if tt != t:
                    continue
                length, score, path_or_edges, type_ = u_edge_data[(
                    ss, vv, tt)]
                if path_or_edges[-1] == tt:
                    predecessor_nodes_in_sg.add(path_or_edges[-2])
        return predecessor_nodes_in_sg

    # Create a graph for the purposes of this function only.
    cg = nx.MultiDiGraph()
    for vals in simple_ctg_paths:
        s, v, t, p_len, p_score, path, n_edges, is_spur = vals
        # is_spur = False
        cg.add_edge(s, t, key=v, type_="simple_ctg",
                    via=v, length=p_len, score=p_score, is_spur=is_spur, data=vals)

    # Collect all non-trivial nodes which will require the best in-edge in the dict.
    nontrivial_nodes = set()
    for v in cg.nodes():
        if len(cg.in_edges(v, keys=True)) > 1 and len(cg.out_edges(v, keys=True)) == 1:
            nontrivial_nodes.add(v)

    for key in sorted(nontrivial_nodes):
        LOG.debug('(before) v = {} -> best_in = {}'.format(key, best_in_dict[key]))

    num_iterations = 0
    converged = False
    while num_iterations < 100 and converged == False:
        converged = True
        num_removed_edges = 0
        for v in nontrivial_nodes:
            in_edges = cg.in_edges(v, keys=True)
            out_edges = cg.out_edges(v, keys=True)
            edges_to_remove = []

            LOG.debug('[it = {}, v = {}] len(in_edges) = {}'.format(num_iterations, v, len(in_edges), len(out_edges)))

            # Only focus on nodes which are still non-trivial.
            # If we reached here, then this node was already resolved at an earlier iteration.
            if len(in_edges) <= 1:
                LOG.debug('    => Not interesting, len(in_edges) <= 1.')
                continue

            # Sort by length in descending order.
            in_edges = sorted(in_edges, key = lambda x: cg.get_edge_data(x[0], x[1], key=x[2])['length'], reverse = True)

            # Longest contig entering this node.
            ss, tt, vv = in_edges[0]
            max_e_data = cg.get_edge_data(ss, tt, key=vv)
            max_len = max_e_data['length']

            # Find other spur contigs which are shorter than the max one and mark
            # them for removal.
            for e in in_edges:
                ss, tt, vv = e
                e_data = cg.get_edge_data(ss, tt, key=vv)

                ### DEBUG.
                LOG.debug('    - in_edge: {}'.format(print_cg_edge_data(ss, tt, vv)))
                pred_nodes_in_sg = get_next_to_last_nodes_from_cg_edge(cg, ss, tt, vv)
                LOG.debug('        => pred_nodes_in_sg: {}'.format(str(pred_nodes_in_sg)))

                if e_data['is_spur'] and e_data['length'] < (max_len / 2.0):
                    edges_to_remove.append(e)
                    converged = False
                    ### DEBUG.
                    LOG.debug('        => To remove.')

            # Remove the edges from the graph.
            LOG.debug('    => Removing {} edges.'.format(len(edges_to_remove)))
            for ss, tt, vv in edges_to_remove:
                cg.remove_edge(ss, tt, key=vv)
                num_removed_edges += 1

            # If the current positon converged so that there is exactly one
            # input and one output edge, increase the out edge so that
            # the next iteration we can improve the next path.
            # Ideally, we should merge the two neighboring contigs, but this is
            # a hack which should hopefully work.
            #
            # What happens here is the following:
            #   - For a previously nontrivial node, now we have only
            #     1 input and 1 output edge left. These edges will
            #     later be merged into one long contig.
            #   - Instead of modifying the graph here, we will only
            #     modify the length of those contigs which should be merged
            #     so that they have the sum of the lengths of them both.
            #   - This allows us to propagate the new contig length up or down
            #     to the next spur in the next iteration.
            v_in_edges = list(cg.in_edges(v, keys=True))
            v_out_edges = list(cg.out_edges(v, keys=True))
            LOG.debug('    => len(in_edges) = {}'.format(len(v_in_edges)))
            LOG.debug('    => len(out_edges) = {}'.format(len(v_out_edges)))
            if len(v_in_edges) == 1 and len(v_out_edges) == 1:
                LOG.debug('    => out_edges = {}'.format(str(v_out_edges)))
                ss, tt, vv = v_out_edges[0]
                curr_out_e_data = cg.get_edge_data(ss, tt, key=vv)
                len_out_before = curr_out_e_data['length']

                LOG.debug('    => in_edges = {}'.format(str(v_in_edges)))
                ss, tt, vv = v_in_edges[0]
                curr_in_e_data = cg.get_edge_data(ss, tt, key=vv)
                len_in_before = curr_in_e_data['length']

                new_len = len_out_before + len_in_before

                curr_out_e_data['length'] = new_len
                curr_in_e_data['length'] = new_len

                ### DEBUG.
                LOG.debug('        => Increasing length of the single out edge. len_out_before = {}, len_in_before = {}, new_len = {}.'.format(len_out_before, len_in_before, new_len))

        LOG.debug('num_iterations = {}, num_removed_edges = {}, converged = {}'.format(num_iterations, num_removed_edges, converged))
        num_iterations += 1

    changed = {}

    for node in nontrivial_nodes:
        in_edges = cg.in_edges(node, keys=True)

        # Find all predecessor nodes to 'node' in the SG.
        # This requires going from contig data to the last unitig,
        # then checking if the last unitig is a simple or a compound unitig.
        # If it's a simple one, then the next to last node in the path is what
        # we're looking for.
        # Otherwise, the compound unitig is composed of a list of simple unitigs,
        # so we need to scan all of those.
        predecessor_nodes_in_sg = set()
        for e in in_edges:
            ss, tt, vv = e
            for val in get_next_to_last_nodes_from_cg_edge(cg, ss, tt, vv):
                predecessor_nodes_in_sg.add(val)

        prev_best_in = best_in_dict[node]

        best_score = None
        new_best_in = best_in_dict[node]
        for pred_node in predecessor_nodes_in_sg:
            score = sg.get_edge_data(pred_node, node)['score']
            # score = sg.edge(pred_node, node)["score"]
            if best_score == None or score > best_score:
                best_score = score
                new_best_in = pred_node
        best_in_dict[node] = new_best_in

        if new_best_in != prev_best_in:
            changed[node] = (prev_best_in, new_best_in)

    # for key in sorted(nontrivial_nodes):
    #     LOG.info('(after) v = {} -> best_in = {}'.format(key, best_in_dict[key]))

    LOG.info('Haplospur changed best_in preferences:')
    for key in sorted(changed.keys()):
        vals = changed[key]
        LOG.info('(changed) v = {}: {} -> {}'.format(key, vals[0], vals[1]))

    return best_in_dict

class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass

def main(argv=sys.argv):
    sys.stderr.write("IPA2 version of ovlp_to_graph.\n")
    sys.stderr.flush()

    epilog = """
Outputs:
    - ctg_paths
    - c_path
    - sg_edges_list
    - chimer_nodes (if not --disable-chimer-bridge-removal)
    - utg_data
    - utg_data0 (maybe)
"""
    parser = argparse.ArgumentParser(
            description='example string graph assembler that is desinged for handling diploid genomes',
            epilog=epilog,
            formatter_class=HelpF)
    parser.add_argument(
        '--overlap-file', default='preads.m4',
        help='a file that contains the overlap information.')

    # These are only for the filter, currently a separate program. They are ignored here.
    parser.add_argument(
        '--min_len', type=int, default=4000,
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--min-len', type=int, default=4000,
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--min_idt', type=float, default=96,
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--min-idt', type=float, default=96,
        help=argparse.SUPPRESS)

    parser.add_argument(
        '--lfc', action="store_true", default=False,
        help='use local flow constraint method rather than best overlap method to resolve knots in string graph')
    parser.add_argument(
        '--disable_chimer_bridge_removal', action="store_true", default=False,
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--disable-chimer-bridge-removal', action="store_true", default=False,
        help='disable chimer induced bridge removal')
    parser.add_argument(
        '--ctg-prefix', default='',
        help='Prefix for contig names.')
    parser.add_argument(
        '--haplospur', action="store_true", default=False,
        help='Apply the haplospur contig extraction algorithm.')


    parser.add_argument(
        '--depth-cutoff', type=int, default=48,
        help='Depth cutoff threshold (number of nodes) for bundle finding.')
    parser.add_argument(
        '--width-cutoff', type=int, default=16,
        help='Width cutoff threshold (number of nodes) for bundle finding.')
    parser.add_argument(
        '--length-cutoff', type=int, default=500000,
        help='Depth cutoff threshold (number of nodes) for bundle finding.')

    args = parser.parse_args(argv[1:])
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='[%(asctime)s %(levelname)s] %(msg)s', datefmt='%Y-%m-%d %H:%M:%S')
    ovlp_to_graph(args)


if __name__ == "__main__":
    main(sys.argv)
