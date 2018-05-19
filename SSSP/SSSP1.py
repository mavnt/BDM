# coding=utf-8
import time
from collections import namedtuple
from math import inf
from operator import add

from pyspark import SparkContext

Node = namedtuple('Node', ['node_id', 'distance_from_s', 'adjList'])
Edge = namedtuple('Edge', ['destination', 'weight'])


def group(lst, n):
    return zip(*[lst[i::n] for i in range(n)])


def mapper1(x):
    node_id, node = x
    yield x
    d = node.distance_from_s
    for m in node.adjList:
        yield m.destination, d + m.weight


def mapper2(x):
    m, distances_node = x[0], x[1]
    d_min = inf
    M = None
    for d in distances_node:
        if isinstance(d, float) and d < d_min:
            d_min = d
        elif not isinstance(d, float):
            M: Node = d

    M = Node(M[0], min([d_min, M.distance_from_s]), M.adjList)
    return m, M


def single_source_shortest_paths(source_node='A', file_name='simple_graph.txt'):
    sc = SparkContext('local[4]', 'single_source_shortest_paths')
    sc.setLogLevel("ERROR")
    tf1 = sc.textFile(file_name)

    tuples = tf1.map(lambda s: s.split(' ')) \
        .map(lambda x: (x[0], [x[1], float(x[-1])])) \
        .reduceByKey(add) \
        .map(lambda x: (x[0], list(group(x[1], 2))))

    def pre_processing1(x):
        tmp_ = Node(x[0], inf if x[0] != source_node else 0, [Edge(*y) for y in x[1]])
        yield tmp_.node_id, tmp_
        for y in tmp_.adjList:
            yield y.destination, Node(y.destination, inf, [])

    def pre_processing2(x):
        node_id, nodes = x
        list_ = []
        for n in nodes:
            list_ += n.adjList
        return node_id, Node(node_id, inf if node_id != source_node else 0, list_)

    tuples = tuples.flatMap(pre_processing1).groupByKey().map(pre_processing2)  # .map(lambda x: (x[0], list(x[1])))

    # print('input tuples')
    # g = Graph()
    # for t in tuples.collect():
    #     print(t)
    #     from_ = t[0]
    #     for destination in t[1].adjList:
    #         to_ = destination.destination
    #         label = destination.weight
    #         g.add_edge(from_, to_, label=label)
    # g.write()

    start_time = time.time()
    for _ in range(6):
        tuples = tuples.flatMap(mapper1)  # flapMap for multiple return values
        tuples = tuples.groupByKey()  # .map(lambda x: (x[0], list(x[1])))
        tuples = tuples.map(mapper2)

    total_time = time.time() - start_time
    print(f'Done in {total_time} seconds.')
    print('Results:')
    for t in tuples.collect():
        print(t)


if __name__ == '__main__':
    single_source_shortest_paths()
