__author__ = 'drew bailey'
__version__ = 0.1

"""

"""

import string


def str_prep(text):
    """ to lower, remove punctuation """
    punct = string.punctuation
    if text:
        text = text.strip().lower()
        for p in punct:
            text = text.replace(p, '')
    else:
        text = None
    return text


def to_set(text):
    """ splits a string to a set """
    if text:
        return set(text.split())
    return set()


def condense(text_list):
    result = []
    for text in text_list:
        text = to_set(str_prep(text))
        result.append(text)
    return result


def calculate_distances(txn_dict):
    """
    returns minimum sized distance (overlap) matrix dictionary (items below diagonal)
        given a dictionary of transaction sets.
    """
    distance_matrix = {}
    for row_index, row_value in txn_dict.items():
        for col_index, col_value in txn_dict.items():
            if col_index < row_index:
                isect = row_value & col_value
                distance_matrix.update({(row_index, col_index): isect})
    print len(distance_matrix)
    return distance_matrix


def max_dist_keys(distance_matrix):
    """
    returns a list of keys corrisponding to maximum indersection overlap count
        given a distance (overlap) matrix.
    """
    values = list(distance_matrix.values())
    keys = list(distance_matrix.keys())
    lengths = [len(value) for value in values]
    maximum = max(lengths)
    # isect = lengths.index(maximum)
    # indices = [i for i, value in enumerate(values) if len(value) == maximum and value == isect]
    indices = [i for i, value in enumerate(values) if len(value) == maximum]
    node_sets = [set(keys[index]) for index in indices]
    for node in node_sets:
        i = node_sets.index(node)
        while True:
            j = 0
            for node_ in node_sets[i+1:]:
                if node & node_:
                    j = node_sets.index(node_)
                    node_sets[i] |= node_sets.pop(j)
                    node = node_sets[i]
                    break
            if not j:
                break
    return [tuple(sorted(node)) for node in node_sets]


def set_keys(keys):
    return tuple(set([y for x in keys for y in x]))


def print_hierarchy():
    #'/n%s'
    #'/n/t%s'
    pass


def print_txns(txns):
    for k, v in sorted(txns.items()):
        print k, v


def run(strings):
    txn_list = condense(strings)
    # dictionary of transaction sets, key is list index
    txns = dict([((i,), val) for i, val in enumerate(txn_list)])
    node_hierarchy = []
    pops = []
    while len(txns) > 1:
        node_list = []
        distance_matrix = calculate_distances(txns)
        keys = max_dist_keys(distance_matrix)
        #print 'keys', keys
        for i, key in enumerate(keys):
            new_txn = set()
            for txn_key in key:
                new_txn |= txns.pop(txn_key)
                pops.append(txn_key)
            new_key = set_keys(key)
            node_list.append((new_key, new_txn))
            txns[new_key] = new_txn
        node_hierarchy.append(node_list)

    display = ['%5s: ' % str(x) for x in range(len(strings))]
    membership = [(x, []) for x in range(len(strings))]
    cluster_variance = [(i, len(n)) for i, n in enumerate(node_hierarchy)]
    sorted_variance = sorted(cluster_variance, key=lambda var: var[1], reverse=True)
    first_cluster = sorted_variance[0][1]
    second_cluster = sorted_variance[1][1]

    header = '%5s: ' % 'hdr'
    adder = '%3s'
    for i, tier in enumerate(node_hierarchy):
        header += adder % str(i + 1)
        elements = set(range(len(strings)))
        yes = set()
        for j, node in enumerate(tier):
            for k, item in enumerate(display):
                if k in node[0]:
                    display[k] += adder % str(j)
                    membership[k][1].append(j)
                    yes.add(k)
        elements -= yes
        for l in elements:
            display[l] += adder % str('')
            membership[l][1].append(None)
    tier_membership = zip(*zip(*membership)[1])
    first = tier_membership[first_cluster]
    second = tier_membership[second_cluster]
    return membership, tier_membership, first, second


def print_display(header, display):
    print header
    for item in display:
        print item

