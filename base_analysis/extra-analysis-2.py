from pyspark import SparkContext
import matplotlib.pyplot as plt

#Spark contest
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Loading task events
task_events = sc.textFile("./task_events/part-00000-of-00500.csv.gz")

task_events_parsed = task_events.map(lambda x: x.split(','))

# map (cpu required [9], mem required[10])

cpu_memory_requests = task_events_parsed.map(lambda x: (float(x[9]), float(x[10])) if x[9] != '' and x[10] != '' else None).filter(lambda x: x is not None)

#Count task number for cpu values and mem values 
cpu_distribution = cpu_memory_requests.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).collect()
memory_distribution = cpu_memory_requests.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).collect()

# Data for plot
cpu_distribution = sorted(cpu_distribution, key=lambda x: x[0])
memory_distribution = sorted(memory_distribution, key=lambda x: x[0])


cpu_values, cpu_counts = zip(*cpu_distribution)
memory_values, memory_counts = zip(*memory_distribution)

# Plot: Distribution of CPU Requests
plt.figure(figsize=(10, 5))
plt.bar(cpu_values, cpu_counts, width=0.02, edgecolor="black")
plt.xlabel("CPU Requested")
plt.ylabel("Number of Tasks")
plt.title("Distribution of CPU Requests")
plt.grid(True)
plt.show()

# Plot: Distribution of Memory Requests 
plt.figure(figsize=(10, 5))
plt.bar(memory_values, memory_counts, width=0.02, edgecolor="black", color='orange')
plt.xlabel("Memory Requested")
plt.ylabel("Number of Tasks")
plt.title("Distribution of Memory Requests")
plt.grid(True)
plt.show()

input("Press Enter ")

sc.stop()
