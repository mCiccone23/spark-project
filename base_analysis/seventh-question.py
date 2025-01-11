<<<<<<< HEAD:seventh-question.py
 #• Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
# task usage for resource consumption : CPU, mem (maximum)


from pyspark import SparkContext

import matplotlib.pyplot as plt
import math
import time as t

sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


#Loading
tasksEvents = sc.textFile("./task_events/part-00000-of-00500.csv.gz")

tasksEvents = tasksEvents.map(lambda x: x.split(','))

tasksUsage = sc.textFile("./task_usage/part-00000-of-00500.csv.gz")

tasksUsage = tasksUsage.map(lambda x: x.split(','))

#Column indexes
event_type = 5
machine_id = 4
task_index = 3
job_id = 2
max_mem = 10
max_cpu = 13

start = t.time()

#from task event, we need: (job-ID+task_index,machine_id,(event_type)) so we can have information for each task and machines on which is being runned,the event corresponding
tasksEvents = tasksEvents.filter(lambda x: x[machine_id] != '').map(lambda x : ((x[job_id] + x[task_index], x[machine_id]), (x[event_type])))
#from task usage, we need: ((job-ID+task index,machine id), (max resource consumption per task))
tasksUsage = tasksUsage.filter(lambda x : x[max_mem] != '' and x[max_cpu] != '')\
                            .map(lambda x: ((x[job_id] + x[task_index], x[machine_id]),(x[max_mem] ,x[max_cpu])))
                            
#Find the peaks of resources                          
taskUsagePeaks = tasksUsage.reduceByKey(
    lambda a, b: (max(float(a[0]), float(b[0])), max(float(a[1]), float(b[1])))
)
#joined the 2 tables on the key
taskUsageEvents = taskUsagePeaks.join(tasksEvents)

# Data visualization
data = taskUsageEvents.map(
    lambda x: (float(x[1][0][0]), float(x[1][0][1]), int(x[1][1]))  # (max_mem, max_cpu, event_type)
).collect()

#Filter of evicted event to visualize them in the plot
filtered_data = [item for item in data if item[2] == 2]

filtered_max_mem = [item[0] for item in filtered_data]
filtered_max_cpu = [item[1] for item in filtered_data]
end = t.time() - start
print("Execution time:", end)
#Plot
plt.figure(figsize=(10, 6))
plt.scatter(filtered_max_mem, filtered_max_cpu, c='red', alpha=0.6, label='event_type = 2')
plt.xlabel('Max Memory')
plt.ylabel('Max CPU')
plt.title('Scatter Plot of Max Memory and Max CPU (event_type = 2)')
plt.grid(True)
plt.legend()
plt.show()

input("Press Enter ")

sc.stop()
=======
 #• Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
# task usage for resource consumption : CPU, mem (maximum)
# task events for scheduling class (evicted=='2')

from pyspark import SparkContext

import matplotlib.pyplot as plt
import math
import time as t

def compute_correlation(rdd):
    # Map to compute partial sums for all required statistics
    stats = rdd.map(lambda pair: (
        1,                 # Count
        pair[0],           # Sum of x
        pair[1],           # Sum of y
        pair[0] * pair[1], # Sum of x * y
        pair[0] ** 2,      # Sum of x^2
        pair[1] ** 2       # Sum of y^2
    )).reduce(lambda a, b: (
        a[0] + b[0],  # Total count
        a[1] + b[1],  # Total sum_x
        a[2] + b[2],  # Total sum_y
        a[3] + b[3],  # Total sum_xy
        a[4] + b[4],  # Total sum_x2
        a[5] + b[5]   # Total sum_y2
    ))

    n, sum_x, sum_y, sum_xy, sum_x2, sum_y2 = stats

    # Compute correlation
    numerator = n * sum_xy - sum_x * sum_y
    denominator = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))

    if denominator == 0:
        return 0  # Handle divide-by-zero case

    return numerator / denominator

def safe_float(value):
    return float(value.strip()) if value.strip() != '' else 0.0



# Programma principale
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")



tasksEvents = sc.textFile("../task_events/part-00000-of-00500.csv.gz")

tasksEvents = tasksEvents.map(lambda x: x.split(','))

tasksUsage = sc.textFile("../task_usage/part-00000-of-00500.csv.gz")

tasksUsage = tasksUsage.map(lambda x: x.split(','))


event_type = 5
machine_id = 4
time = 0

start_time = 0 
end_time = 1
max_mem = 10
max_cpu = 13
max_disc = 14

start = t.time()

# 1. Filtra task evicted e mappa in (machine_id, time)
evicted_tasks = tasksEvents.filter(lambda x: x[event_type] == '2') \
                           .map(lambda x: (x[machine_id], int(x[time])))


# 2. Mappa resource usage in (machine_id, (start_time, end_time, (max_mem, max_cpu, max_disc)))
resource_usage = tasksUsage.filter(lambda x: x[max_mem].strip() != '' and x[max_cpu].strip() != '' and x[max_disc].strip() != '').map(lambda x: (x[machine_id], 
                                           (int(x[start_time]), int(x[end_time]), 
                                            (float(x[max_mem]), float(x[max_cpu]), float(x[max_disc])))))

# 3. Join su machine_id
joined_data = evicted_tasks.join(resource_usage)

# 4. Filtro su time compreso tra start_time e end_time
filtered_data = joined_data.filter(lambda x: int(x[1][0]) >= x[1][1][0] and int(x[1][0]) <= x[1][1][1])

# 5. Mappa per ciascuna risorsa (max_mem, max_cpu, max_disc)
resources_evictions = filtered_data.map(lambda x: (x[1][1][2], 1))  # (max_mem, max_cpu, max_disc), 1

# 6. Aggrega per ciascuna risorsa
evictions_by_mem = resources_evictions.map(lambda x: (x[0][0], 1)).reduceByKey(lambda a, b: a + b)  # max_mem
evictions_by_cpu = resources_evictions.map(lambda x: (x[0][1], 1)).reduceByKey(lambda a, b: a + b)  # max_cpu
evictions_by_disc = resources_evictions.map(lambda x: (x[0][2], 1)).reduceByKey(lambda a, b: a + b)  # max_disc

# 7. Calcola la correlazione tra numero di eviction e max per ciascuna risorsa
correlation_mem = compute_correlation(evictions_by_mem.map(lambda x: (x[0], x[1])))
correlation_cpu = compute_correlation(evictions_by_cpu.map(lambda x: (x[0], x[1])))
correlation_disc = compute_correlation(evictions_by_disc.map(lambda x: (x[0], x[1])))

end = t.time() - start 

print("Execution time: ", end)


print(f"Correlation between max memory and eviction events: {correlation_mem}")
print(f"Correlation between max CPU and eviction events: {correlation_cpu}")
print(f"Correlation between max disk and eviction events: {correlation_disc}")

# 8. Visualizzazione dei risultati
# Per memoria
mem, evictions_mem = zip(*evictions_by_mem.collect())
plt.bar(mem, evictions_mem, color='blue')
plt.xlabel("Max Memory Usage")
plt.ylabel("Number of Evictions")
plt.title("Correlation between Memory Peaks and Evictions")
plt.show()

# Per CPU
cpu, evictions_cpu = zip(*evictions_by_cpu.collect())
plt.bar(cpu, evictions_cpu, color='orange')
plt.xlabel("Max CPU Usage")
plt.ylabel("Number of Evictions")
plt.title("Correlation between CPU Peaks and Evictions")
plt.show()

# Per disco
disc, evictions_disc = zip(*evictions_by_disc.collect())
plt.bar(disc, evictions_disc, color='green')
plt.xlabel("Max Disk Usage")
plt.ylabel("Number of Evictions")
plt.title("Correlation between Disk Peaks and Evictions")
plt.show()



>>>>>>> 41afc30520060db5509d52edfe2f52ed1dbf31c1:base_analysis/seventh-question.py
