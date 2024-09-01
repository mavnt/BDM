from pyspark import SparkContext


def map(row):
    date, id_b, id_s, price = row
    price = float(price)
    yyyy_mm = date[:7]
    yield (yyyy_mm + "." + id_b, (-price, 1))
    yield (yyyy_mm + "." + id_s, (price, 1))


def main():
    sc = SparkContext("local[4]", "flusso-medio-per-mese")
    tf1 = sc.textFile("transactions.txt")
    rows = tf1.map(lambda s: s.split(";")).flatMap(map)
    print("after map")
    for item in rows.collect():
        print(item)
    rows = rows.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    print("after reduceByKey")
    for item in rows.collect():
        print(item)
    rows = rows.map(lambda x: (x[0], (x[1][0] / x[1][1], x[1][1])))
    print("after map")
    for item in rows.collect():
        print(item)


if __name__ == "__main__":
    main()
