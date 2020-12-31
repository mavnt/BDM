# coding=utf-8
from pyspark import SparkContext


def main():
    sc = SparkContext("local[4]", "WordByLength")
    sc.setLogLevel("WARN")
    tf1 = sc.textFile("Spark00.py")
    words = tf1.flatMap(lambda s: s.split(" "))
    results = (
        words.map(lambda w: (len(w), 1)).reduceByKey(lambda x, y: x + y).collectAsMap()
    )
    for k in results:
        print(k, results[k])


if __name__ == "__main__":
    main()
