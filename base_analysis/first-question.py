from pyspark import SparkContext
import matplotlib.pyplot as plt
import time

sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


wholeFile = sc.textFile("./part-00000-of-00001.csv.gz")

entries = wholeFile.map(lambda x: x.split(',')) # entry splitù

cpu_capacity_index = 4 # here we put the index of the capacity

start = time.time()
# we do a first map to take the distrinct machines
distrinctEntries = entries.map(lambda x: (x[1], x[cpu_capacity_index])).distinct().cache()


# map(capacity, 1) then we aggregate the number by key
cpu_distribution = (
    distrinctEntries.map(lambda x: (x[1], 1))
           .reduceByKey(lambda a, b: a + b)
           .collect()
           
)

print("Execution time: ", time.time() - start )

print("CPU Capacity:")
for capacity, count in cpu_distribution:
    print(f"CPU Capacity {capacity}: {count} machines")


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

input("Press Enter ")

sc.stop()



