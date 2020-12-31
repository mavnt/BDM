# coding=utf-8
from collections import namedtuple
from math import inf
from operator import add

from pyspark import SparkContext

Node = namedtuple("Node", ["node_id", "distance_from_s", "adjList"])
Edge = namedtuple("Edge", ["destination", "weight"])


def group(lst, n):
    return zip(*[lst[i::n] for i in range(n)])


def mapper(x):
    node_id, node = x
    yield x
    d = node.distance_from_s
    for m in node.adjList:
        yield m.destination, d + m.weight


def reducer(x, y):
    if type(x) == float and type(y) == float:
        return x if x < y else y
    else:
        if type(x) == float and not type(y) == float:
            y: Node
            return Node(
                y.node_id, y.distance_from_s if x > y.distance_from_s else x, y.adjList
            )
        if type(y) == float and not type(x) == float:
            x: Node
            return Node(
                x.node_id, x.distance_from_s if y > x.distance_from_s else y, x.adjList
            )


def single_source_shortest_paths(source_node="A", file_name="simple_graph.txt"):
    sc = SparkContext("local[4]", "single_source_shortest_paths")
    sc.setLogLevel("ERROR")
    tf1 = sc.textFile(file_name)

    tuples = (
        tf1.map(lambda s: s.split(" "))
        .map(lambda x: (x[0], [x[1], float(x[-1])]))
        .reduceByKey(add)
        .map(lambda x: (x[0], list(group(x[1], 2))))
    )

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

    tuples = tuples.flatMap(pre_processing1).groupByKey().map(pre_processing2)

    for _ in range(6):
        tuples = tuples.flatMap(mapper)
        tuples = tuples.reduceByKey(reducer)

    print("Results:")
    for t in tuples.collect():
        print(t)


if __name__ == "__main__":
    single_source_shortest_paths()
