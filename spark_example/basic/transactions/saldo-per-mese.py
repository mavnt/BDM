from pyspark import SparkContext


def map(row):
    date, id_b, id_s, price = row
    price = float(price)
    yyyy_mm = date[:7]
    yield yyyy_mm + "." + id_b, -price
    yield yyyy_mm + "." + id_s, price


def main():
    sc = SparkContext("local[4]", "saldo-per-mese")
    tf1 = sc.textFile("transactions.txt")
    rows = tf1.map(lambda s: s.split(";")).flatMap(map)
    print("after map")
    rows = rows.reduceByKey(lambda x, y: x + y)
    print("after reduceByKey")
    for item in rows.collect():
        print(item)


if __name__ == "__main__":
    main()
