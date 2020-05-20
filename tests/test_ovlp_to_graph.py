import pytest
import ipa2_ovlp_to_graph as uut
import networkx as nx

### Test data 1: Linear chain.
TEST_DATA_1__generic_utg_data = (5, 10, [], 'simple')
TEST_DATA_1__u_edge_data = {
    ('1', '2', '1a'): TEST_DATA_1__generic_utg_data,
    ('2', '3', '2a'): TEST_DATA_1__generic_utg_data,
    ('3', '4', '3a'): TEST_DATA_1__generic_utg_data,
    ('4', '5', '4a'): TEST_DATA_1__generic_utg_data,
    ('5', '6', '5a'): TEST_DATA_1__generic_utg_data,
    ('6', '7', '6a'): TEST_DATA_1__generic_utg_data,
    ('7', '8', '7a'): TEST_DATA_1__generic_utg_data,
    ('8', '9', '8a'): TEST_DATA_1__generic_utg_data,
    ('9', '10', '9a'): TEST_DATA_1__generic_utg_data,
}

### Test data 2: Bubble.
TEST_DATA_2__generic_utg_data = (5, 10, [], 'simple')
TEST_DATA_2__u_edge_data = {
    ('1', '2', '1a'): TEST_DATA_2__generic_utg_data,
    ('2', '3', '2a'): TEST_DATA_2__generic_utg_data,
    ('3', '4', '3a'): TEST_DATA_2__generic_utg_data,

    # One branch of a bubble.
    ('4', '5-bubble_1-branch_1', '4a'): TEST_DATA_2__generic_utg_data,
    ('5-bubble_1-branch_1', '6-bubble_1-branch_1', '5a-bubble_1-branch_1'): TEST_DATA_2__generic_utg_data,
    ('6-bubble_1-branch_1', '10', '6a-bubble_1-branch_1'): TEST_DATA_2__generic_utg_data,

    # The other branch of a bubble
    ('4', '7-bubble_1-branch_2', '4b'): TEST_DATA_2__generic_utg_data,
    ('7-bubble_1-branch_2', '8-bubble_1-branch_2', '7a-bubble_1-branch_2'): TEST_DATA_2__generic_utg_data,
    ('8-bubble_1-branch_2', '9-bubble_1-branch_2', '8a-bubble_1-branch_2'): TEST_DATA_2__generic_utg_data,
    ('9-bubble_1-branch_2', '10', '9a-bubble_1-branch_2'): TEST_DATA_2__generic_utg_data,

    ('10', '11', '10a'): TEST_DATA_2__generic_utg_data,
    ('11', '12', '11a'): TEST_DATA_2__generic_utg_data,
    ('12', '13', '12a'): TEST_DATA_2__generic_utg_data,
    ('13', '14', '13a'): TEST_DATA_2__generic_utg_data,
}

### Test data 3: Spur.
TEST_DATA_3__generic_utg_data = (5, 10, [], 'simple')
TEST_DATA_3__u_edge_data = {
    ('1', '2', '1a'): TEST_DATA_3__generic_utg_data,
    ('2', '3', '2a'): TEST_DATA_3__generic_utg_data,
    ('3', '4', '3a'): TEST_DATA_3__generic_utg_data,

    # This is a spur.
    ('4', '5-bubble_1-branch_1', '4a'): TEST_DATA_3__generic_utg_data,
    ('5-bubble_1-branch_1', '6-bubble_1-branch_1', '5a-bubble_1-branch_1'): TEST_DATA_3__generic_utg_data,
    ('6-bubble_1-branch_1', '7', '6a-bubble_1-branch_1'): TEST_DATA_3__generic_utg_data,

    # Alternative branch which is actually the remaining chain.
    ('4', '8-bubble_1-branch_2', '4b'): TEST_DATA_3__generic_utg_data,
    ('8-bubble_1-branch_2', '9-bubble_1-branch_2', '8a-bubble_1-branch_2'): TEST_DATA_3__generic_utg_data,
    ('9-bubble_1-branch_2', '10-bubble_1-branch_2', '9a-bubble_1-branch_2'): TEST_DATA_3__generic_utg_data,
    ('10-bubble_1-branch_2', '11', '10a-bubble_1-branch_2'): TEST_DATA_3__generic_utg_data,

    ('11', '12', '11a'): TEST_DATA_3__generic_utg_data,
    ('12', '13', '12a'): TEST_DATA_3__generic_utg_data,
    ('13', '14', '13a'): TEST_DATA_3__generic_utg_data,
    ('14', '15', '14a'): TEST_DATA_3__generic_utg_data,
}

### Test data 4: Bubble and a spur forking out at the same node as the bubble.
TEST_DATA_4__generic_utg_data = (5, 10, [], 'simple')
TEST_DATA_4__u_edge_data = {
    # Linear chain at the front, before the bubble and the spur.
    ('1', '2', '1a'): TEST_DATA_4__generic_utg_data,
    ('2', '3', '2a'): TEST_DATA_4__generic_utg_data,
    ('3', '4', '3a'): TEST_DATA_4__generic_utg_data,

    # One branch of a bubble.
    ('4', '5-bubble_1-branch_1', '4a'): TEST_DATA_4__generic_utg_data,
    ('5-bubble_1-branch_1', '6-bubble_1-branch_1', '5a-bubble_1-branch_1'): TEST_DATA_4__generic_utg_data,
    ('6-bubble_1-branch_1', '10', '6a-bubble_1-branch_1'): TEST_DATA_4__generic_utg_data,

    # The other branch of a bubble
    ('4', '7-bubble_1-branch_2', '4b'): TEST_DATA_4__generic_utg_data,
    ('7-bubble_1-branch_2', '8-bubble_1-branch_2', '7a-bubble_1-branch_2'): TEST_DATA_4__generic_utg_data,
    ('8-bubble_1-branch_2', '9-bubble_1-branch_2', '8a-bubble_1-branch_2'): TEST_DATA_4__generic_utg_data,
    ('9-bubble_1-branch_2', '10', '9a-bubble_1-branch_2'): TEST_DATA_4__generic_utg_data,

    # This is a spur.
    ('4', '15-spur-1', '4c'): TEST_DATA_4__generic_utg_data,
    ('15-spur-1', '16-spur-1', '15a-spur-1'): TEST_DATA_4__generic_utg_data,
    ('16-spur-1', '17-spur-1', '16a-spur-1'): TEST_DATA_4__generic_utg_data,
    ('17-spur-1', '18-spur-1', '17a-spur-1'): TEST_DATA_4__generic_utg_data,
    ('18-spur-1', '19-spur-1', '18a-spur-1'): TEST_DATA_4__generic_utg_data,

    # Remaining linear chain.
    ('10', '11', '10a'): TEST_DATA_4__generic_utg_data,
    ('11', '12', '11a'): TEST_DATA_4__generic_utg_data,
    ('12', '13', '12a'): TEST_DATA_4__generic_utg_data,
    ('13', '14', '13a'): TEST_DATA_4__generic_utg_data,
}

### Test data 5: Cyclic graph.
TEST_DATA_5__generic_utg_data = (5, 10, [], 'simple')
TEST_DATA_5__u_edge_data = {
    # Everything is linear except for one cyclic edge below.
    ('1', '2', '1a'): TEST_DATA_5__generic_utg_data,
    ('2', '3', '2a'): TEST_DATA_5__generic_utg_data,
    ('3', '4', '3a'): TEST_DATA_5__generic_utg_data,
    ('4', '5', '4a'): TEST_DATA_5__generic_utg_data,
    ('5', '6', '5a'): TEST_DATA_5__generic_utg_data,
    ('6', '7', '6a'): TEST_DATA_5__generic_utg_data,
    ('7', '8', '7a'): TEST_DATA_5__generic_utg_data,
    ('8', '9', '8a'): TEST_DATA_5__generic_utg_data,
    ('9', '10', '9a'): TEST_DATA_5__generic_utg_data,

    # Cyclic edge.
    ('7', '4', '7b'): TEST_DATA_5__generic_utg_data,
}


def build_ug(ug_edge_data):
    ug = nx.MultiDiGraph()
    for e, vals in ug_edge_data.items():
        s, t, v = e
        length, score, path, type_ = vals
        ug.add_edge(s, t, key=v, type_=type_,
                    via=v, length=length, score=score)
    return ug

def test_ego_dfs_with_convergence_1():
    """
    Test empty input graph.
    """
    # Inputs.
    start_node = ''
    depth_cutoff = 1
    width_cutoff = 1
    length_cutoff = 1
    stop_on_convergence = True
    undirected = False

    u_edge_data = {}
    ug = build_ug(u_edge_data)

    # Expected results
    expected_ego_edges = set()

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges())

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_2a():
    """
    Test ego graph extraction on a simple chain graph, using the directed
    mode and depth_cutoff of 1, but also stop_on_convergence is activated.
    Expected:
        - This should return only one edge, the successor of '4'.
    """
    # Inputs.
    u_edge_data = TEST_DATA_1__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 1
    width_cutoff = 1
    length_cutoff = 10
    stop_on_convergence = True
    undirected = False

    # Expected results
    expected_ego_edges = set([
        ('4', '5', '4a'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_2b():
    """
    Test ego graph extraction on a simple chain graph, using the undirected
    mode and depth_cutoff of 2, without stop_on_convergence. Stopping on convergence
    doesn't make much sense in undirected graphs because we're expanding in all directions
    at the same time.
    Expected:
        - This should return 4 edges: 2 predecessors to node '4' and 2 successors.
    """
    # Inputs.
    u_edge_data = TEST_DATA_1__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 2
    width_cutoff = 100
    length_cutoff = 100
    stop_on_convergence = False
    undirected = True

    # Expected results
    expected_ego_edges = set([
        ('2', '3', '2a'),
        ('3', '4', '3a'),
        ('4', '5', '4a'),
        ('5', '6', '5a'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_3():
    """
    Setup:
        - With convergence
        - Directed graph
        - Linear graph as input
        - Large bubble extraction parameters
    Expected:
        - Only a single edge should be reported because the convergence heuristic
          will stop the DFS.
    """
    # Inputs.
    u_edge_data = TEST_DATA_1__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 100
    width_cutoff = 100
    length_cutoff = 100
    stop_on_convergence = True
    undirected = False

    # Expected results
    expected_ego_edges = set([
        ('4', '5', '4a'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_4():
    """
    Setup:
        - NO convergence (the only difference compared to the previous test).
        - Directed graph
        - Linear graph as input
        - Large bubble extraction parameters
    Expected:
        - All edges starting from node 4 until the end of the directed graph should
          be captured.
    """
    # Inputs.
    u_edge_data = TEST_DATA_1__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 100
    width_cutoff = 100
    length_cutoff = 100
    stop_on_convergence = False
    undirected = False

    # Expected results
    expected_ego_edges = set([
        ('4', '5', '4a'),
        ('5', '6', '5a'),
        ('6', '7', '6a'),
        ('7', '8', '7a'),
        ('8', '9', '8a'),
        ('9', '10', '9a'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_5():
    """
    Setup:
        - NO convergence
        - UNdirected graph (The only difference compared to the previous test).
        - Linear graph as input
        - Large bubble extraction parameters
    Expected:
        - The graph is viewed as undirected, so all edges should be picked.
    """
    # Inputs.
    u_edge_data = TEST_DATA_1__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 100
    width_cutoff = 100
    length_cutoff = 100
    stop_on_convergence = False
    undirected = True

    # Expected results
    expected_ego_edges = set(TEST_DATA_1__u_edge_data.keys())

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_6():
    """
    Regular use case for bubble extraction.

    Setup:
        - With convergence
        - Directed graph (The only difference compared to the previous test).
        - Single bubble graph as input
        - Large bubble extraction parameters as would be used in IPA
    Expected:
        The bubble edges should be extracted, including one additional edge
        after the bubble which reaches the first simple node.
    """
    # Inputs.
    u_edge_data = TEST_DATA_2__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 20000
    width_cutoff = 30000
    length_cutoff = 15000000
    stop_on_convergence = True
    undirected = False

    # Expected results
    expected_ego_edges = set([
        ('4', '5-bubble_1-branch_1', '4a'),
        ('5-bubble_1-branch_1', '6-bubble_1-branch_1', '5a-bubble_1-branch_1'),
        ('6-bubble_1-branch_1', '10', '6a-bubble_1-branch_1'),
        ('4', '7-bubble_1-branch_2', '4b'),
        ('7-bubble_1-branch_2', '8-bubble_1-branch_2', '7a-bubble_1-branch_2'),
        ('8-bubble_1-branch_2', '9-bubble_1-branch_2', '8a-bubble_1-branch_2'),
        ('9-bubble_1-branch_2', '10', '9a-bubble_1-branch_2'),
        ('10', '11', '10a'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_7():
    """
    Case when the ego graph extraction begins at a spur fork. There is no bubble, and
    when the DFS reaches the spur end there will only be one path left, and the
    convergence heuristic will break further local graph extension.

    Setup:
        - With convergence
        - Directed graph (The only difference compared to the previous test).
        - Linear graph with a spur as input
        - Large bubble extraction parameters as would be used in IPA
    Expected:
        DFS forks into two paths (one for spur, and one for the remainder of the chain),
        and it continues into depth until the spur branch reaches the tip. Then, only
        one branch remains, and since it's a linear chaing, the convergence heuristic stops
        further local graph extraction.
    """
    # Inputs.
    u_edge_data = TEST_DATA_3__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 20000
    width_cutoff = 30000
    length_cutoff = 15000000
    stop_on_convergence = True
    undirected = False

    # Expected results
    expected_ego_edges = set([
        ('4', '5-bubble_1-branch_1', '4a'),
        ('5-bubble_1-branch_1', '6-bubble_1-branch_1', '5a-bubble_1-branch_1'),
        ('6-bubble_1-branch_1', '7', '6a-bubble_1-branch_1'),
        ('4', '8-bubble_1-branch_2', '4b'),
        ('8-bubble_1-branch_2', '9-bubble_1-branch_2', '8a-bubble_1-branch_2'),
        ('9-bubble_1-branch_2', '10-bubble_1-branch_2', '9a-bubble_1-branch_2'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_8():
    """
    Test the length cutoff heuristic.

    Setup:
        - With convergence
        - Directed graph
        - Graph with a bubble and a spur at the bubble fork node.
        - Length cutoff is such that one branch is too long and won't be extracted fully. Other
          parameters are large so that they don't affect the  test.
        - Each edge has a length of 5 and a score of 10.
    Expected:
        Length cutoff of 17 should limit extraction to at most 3 edges in each direction, because
        each edge has a length of 5.
    """
    # Inputs.
    u_edge_data = TEST_DATA_4__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 1000
    width_cutoff = 1000
    length_cutoff = 17
    stop_on_convergence = True
    undirected = False

    # Expected results
    expected_ego_edges = set([
        # The first bubble branch is extracted fully because it's short enough to satisfy length_cutoff.
        ('4', '5-bubble_1-branch_1', '4a'),
        ('5-bubble_1-branch_1', '6-bubble_1-branch_1', '5a-bubble_1-branch_1'),
        ('6-bubble_1-branch_1', '10', '6a-bubble_1-branch_1'),

        # The second bubble branch is not extracted completely.
        ('4', '7-bubble_1-branch_2', '4b'),
        ('7-bubble_1-branch_2', '8-bubble_1-branch_2', '7a-bubble_1-branch_2'),
        ('8-bubble_1-branch_2', '9-bubble_1-branch_2', '8a-bubble_1-branch_2'),
        # ('9-bubble_1-branch_2', '10', '9a-bubble_1-branch_2'),

        # The spur is also too long to be extracted fully.
        ('4', '15-spur-1', '4c'),
        ('15-spur-1', '16-spur-1', '15a-spur-1'),
        ('16-spur-1', '17-spur-1', '16a-spur-1'),
        # ('17-spur-1', '18-spur-1', '17a-spur-1'),
        # ('18-spur-1', '19-spur-1', '18a-spur-1'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_9():
    """
    Test the depth cutoff heuristic.

    Setup:
        - With convergence
        - Directed graph
        - Graph with a bubble and a spur at the bubble fork node.
        - Depth cutoff in this test is set to a value of 3 to limit the depth to 3 edges in any direction
    Expected:
        Depth cutoff of 3 should extract at most 3 edges in each reachable direction from the start node.
        In this test case, the output should be identical to the one in the previous case with the length_cutoff.
    """
    # Inputs.
    u_edge_data = TEST_DATA_4__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 3
    width_cutoff = 1000
    length_cutoff = 1000
    stop_on_convergence = True
    undirected = False

    # Expected results
    expected_ego_edges = set([
        # The first bubble branch is extracted fully because it's short enough to satisfy length_cutoff.
        ('4', '5-bubble_1-branch_1', '4a'),
        ('5-bubble_1-branch_1', '6-bubble_1-branch_1', '5a-bubble_1-branch_1'),
        ('6-bubble_1-branch_1', '10', '6a-bubble_1-branch_1'),

        # The second bubble branch is not extracted completely.
        ('4', '7-bubble_1-branch_2', '4b'),
        ('7-bubble_1-branch_2', '8-bubble_1-branch_2', '7a-bubble_1-branch_2'),
        ('8-bubble_1-branch_2', '9-bubble_1-branch_2', '8a-bubble_1-branch_2'),
        # ('9-bubble_1-branch_2', '10', '9a-bubble_1-branch_2'),

        # The spur is also too long to be extracted fully.
        ('4', '15-spur-1', '4c'),
        ('15-spur-1', '16-spur-1', '15a-spur-1'),
        ('16-spur-1', '17-spur-1', '16a-spur-1'),
        # ('17-spur-1', '18-spur-1', '17a-spur-1'),
        # ('18-spur-1', '19-spur-1', '18a-spur-1'),
    ])

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)

def test_ego_dfs_with_convergence_10():
    """
    Test the width cutoff heuristic.
    This heuristic is supposed to protect from graphs that diverge a lot, for example, cause by repeats.
    Width is computed like this:
        v_width = 0.0 if max_depth == 0 else float(len(seen_edges)) / float(v_depth)

    This means that if the ratio of the number of traversed edges over the depth we reached is over a threshold,
    we return an empty subgraph.

    Imagine this: a forking point contains a large N output branches. The DFS traverses 1 edge in every
    direction first, which means that the depth is 1 but we've seen N edges, so the width is (N / 1).

    Setup:
        - With convergence
        - Directed graph
        - Graph with a bubble and a spur at the bubble fork node.
        - Width cutoff in this test is set to a value of 2 because there are 3 out edges for the
          starting node.
    Expected:
        When the width is exceded, the returned subgraph will be empty.
    """
    # Inputs.
    u_edge_data = TEST_DATA_4__u_edge_data
    ug = build_ug(u_edge_data)
    start_node = '4'
    depth_cutoff = 1000
    width_cutoff = 2
    length_cutoff = 1000
    stop_on_convergence = True
    undirected = False

    # Expected results
    expected_ego_edges = set()

    # Run unit under test.
    local_graph = uut.ego_dfs_with_convergence(ug, u_edge_data, start_node, depth_cutoff, width_cutoff, length_cutoff, stop_on_convergence = stop_on_convergence, undirected = undirected)
    result_ego_edges = set(local_graph.edges(keys=True))

    # Evaluate.
    assert(result_ego_edges == expected_ego_edges)
