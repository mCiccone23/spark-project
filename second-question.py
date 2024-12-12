from pyspark import SparkContext
import matplotlib.pyplot as plt

# Programma principale
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


wholeFile = sc.textFile("part-00000-of-00001.csv.gz")

entries = wholeFile.map(lambda x: x.split(','))


# Close Spark session

sc.stop()

input("Press Enter ")
