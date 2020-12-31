# coding=utf-8
from pyspark import SparkContext


def main():
    sc = SparkContext("local[4]", "WordCount")
    sc.setLogLevel("WARN")
    path0 = "Spark00.py"
    path1 = "Spark01.py"
    tf1 = sc.textFile(path0)
    tf2 = sc.textFile(path1)
    words = tf1.union(tf2).flatMap(lambda s: s.split(" "))
    ones = words.map(lambda word: (word, 1))
    result = ones.reduceByKey(lambda x, y: x + y).collectAsMap()
    for k in result:
        print(k, result[k])


if __name__ == "__main__":
    main()
