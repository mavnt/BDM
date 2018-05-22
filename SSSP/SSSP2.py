# coding=utf-8
import time
from collections import namedtuple
from math import inf
from operator import add

from pyspark import SparkContext

Node = namedtuple('Node', ['node_id', 'distance_from_s', 'vertex_from', 'adjList'])
Edge = namedtuple('Edge', ['destination', 'weight'])


def group(lst, n):
    return zip(*[lst[i::n] for i in range(n)])


def mapper(x):
    node_id, node = x
    yield x
    d = node.distance_from_s
    for m in node.adjList:
        yield m.destination, (node.node_id if d < inf else None, d + m.weight)


def reducer(x, y):
    if type(x) == tuple and type(y) == tuple:
        return x if x[1] < y[1] else y
    else:
        if type(x) == tuple and not type(y) == tuple:
            y: Node
            x: tuple
            return Node(
                    y.node_id,
                    y.distance_from_s if x[1] > y.distance_from_s else x[1],
                    y.vertex_from if x[1] > y.distance_from_s else x[0],
                    y.adjList)
        if type(y) == tuple and not type(x) == tuple:
            y: tuple
            x: Node
            return Node(
                    x.node_id,
                    x.distance_from_s if y[1] > x.distance_from_s else y[1],
                    x.vertex_from if y[1] > x.distance_from_s else y[0],
                    x.adjList)


def single_source_shortest_paths(source_node='A', file_name='simple_graph.txt'):
    sc = SparkContext('local[4]', 'single_source_shortest_paths')
    sc.setLogLevel("ERROR")
    tf1 = sc.textFile(file_name)

    tuples = tf1.map(lambda s: s.split(' ')) \
        .map(lambda x: (x[0], [x[1], float(x[-1])])) \
        .reduceByKey(add) \
        .map(lambda x: (x[0], list(group(x[1], 2))))

    def pre_processing1(x):
        tmp_ = Node(x[0], inf if x[0] != source_node else 0, None, [Edge(*y) for y in x[1]])
        yield tmp_.node_id, tmp_
        for y in tmp_.adjList:
            yield y.destination, Node(y.destination, inf, None, [])

    def pre_processing2(x):
        node_id, nodes = x
        list_ = []
        for n in nodes:
            list_ += n.adjList
        return node_id, Node(node_id, inf if node_id != source_node else 0, None, list_)

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
    # g.view()

    start_time = time.time()
    for _ in range(6):
        tuples = tuples.flatMap(mapper)  # flapMap for multiple return values
        print('after map')
        for t in tuples.collect():
            print(t)

        tuples = tuples.reduceByKey(reducer)
        print('after reduce')
        for t in tuples.collect():
            print(t)

    total_time = time.time() - start_time
    print(f'Done in {total_time} seconds.')
    print('Results:')
    for t in tuples.collect():
        print(t)


if __name__ == '__main__':
    single_source_shortest_paths()
