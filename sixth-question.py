from pyspark import SparkContext

import matplotlib.pyplot as plt
import math

def compute_correlation(rdd):
    n = rdd.count()
    sum_x = rdd.map(lambda pair: pair[0]).sum()
    sum_y = rdd.map(lambda pair: pair[1]).sum()
    sum_xy = rdd.map(lambda pair: pair[0] * pair[1]).sum()
    sum_x2 = rdd.map(lambda pair: pair[0] ** 2).sum()
    sum_y2 = rdd.map(lambda pair: pair[1] ** 2).sum()

    numerator = n * sum_xy - sum_x * sum_y
    denominator = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))

    return numerator / denominator

# Programma principale
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")



tasksEvents = sc.textFile("./task_events/part-00000-of-00500.csv.gz")

tasksEvents = tasksEvents.map(lambda x: x.split(','))

tasksUsage = sc.textFile("./task_usage/part-00000-of-00500.csv.gz")

tasksUsage = tasksUsage.map(lambda x: x.split(','))

#tasksEvents
#map ((jobid, task index), (CPU, mem))
tasksEventsMap = tasksEvents.map(lambda x: ((x[2], x[3]), (x[9], x[10]))).distinct()
print(tasksEventsMap.takeOrdered(10))
#tasksUsage
#map ((jobid, task index), (CPU, mem))
tasksUsageMap = tasksUsage.map(lambda x: ((x[2], x[3]), (x[5], x[6])))
print(tasksUsageMap.takeOrdered(10))

tasksUsageAggregated = tasksUsageMap.reduceByKey(lambda a, b: (
    (float(a[0]) + float(b[0])) / 2, (float(a[1]) + float(b[1])) / 2))

joinedStat = tasksEventsMap.join(tasksUsageAggregated)

cleanedStat = joinedStat.filter(lambda x: 
    x[1][0][0] != '' and x[1][1][0] != '' and x[1][0][1] != '' and x[1][1][1] != ''
)

formattedStat = cleanedStat.map(lambda x: (
    x[0],  # (job_id, task_index)
    float(x[1][0][0]),  # CPU required
    float(x[1][1][0]),  # CPU used
    float(x[1][0][1]),  # MEM required
    float(x[1][1][1])   # MEM used
))

cpu_rdd = formattedStat.map(lambda x: (x[1], x[2]))
memory_rdd = formattedStat.map(lambda x: (x[3], x[4]))

cpu_corr = compute_correlation(cpu_rdd)
memory_corr = compute_correlation(memory_rdd)

print("CPU correlation: ", cpu_corr)
print("Memory correlation: ", memory_corr)


data = formattedStat.collect()


requested_cpu = [x[1] for x in data]  
used_cpu = [x[2] for x in data]      
requested_memory = [x[3] for x in data]  
used_memory = [x[4] for x in data]     

# CPU scatter plot
plt.figure(figsize=(10, 5))
plt.scatter(requested_cpu, used_cpu, alpha=0.5)
plt.xlabel("CPU Requested")
plt.ylabel("CPU Used")
plt.title("Scatter Plot: CPU Requested vs CPU Used")
plt.grid(True)
plt.show()

# Memory scatter plot
plt.figure(figsize=(10, 5))
plt.scatter(requested_memory, used_memory, color='orange', alpha=0.5)
plt.xlabel("Memory Requested")
plt.ylabel("Memory Used")
plt.title("Scatter Plot: Memory Requested vs Memory Used")
plt.grid(True)
plt.show()


# Close Spark session

sc.stop()

input("Press Enter ")
 