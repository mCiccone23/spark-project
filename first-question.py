from pyspark import SparkContext
import matplotlib.pyplot as plt

# Programma principale
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


wholeFile = sc.textFile("part-00000-of-00001.csv.gz")

entries = wholeFile.map(lambda x: x.split(','))

cpu_capacity_index = 5
cpu_distribution = (
    entries.map(lambda x: (x[cpu_capacity_index], 1))
           .reduceByKey(lambda a, b: a + b)
           .collect()
)

print("Distribuzione della capacità CPU:")
for capacity, count in cpu_distribution:
    print(f"Capacità CPU {capacity}: {count} macchine")

cpu_distribution = sorted(cpu_distribution, key=lambda x: x[0])

filtered_distribution = [(capacity, count) for capacity, count in cpu_distribution if capacity.replace('.', '', 1).isdigit()]
#Representation with Bar chart
capacities = [float(capacity) for capacity, _ in filtered_distribution]
counts = [count for _, count in filtered_distribution]

plt.bar(capacities, counts, width=0.05, edgecolor="black", facecolor="none", linewidth=1.5)
plt.xlabel("CPU capacity")
plt.ylabel("#Machine")
plt.title("Distribution of the machines according to their CPU capacity")
plt.xticks(capacities)
plt.show()
# Close Spark session

sc.stop()

input("Press Enter ")
