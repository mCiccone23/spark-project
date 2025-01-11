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
