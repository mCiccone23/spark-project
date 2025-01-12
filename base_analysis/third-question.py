from pyspark import SparkContext
import matplotlib.pyplot as plt
import time

sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

start_time = time.time()

jobsFile = sc.textFile("./job_events/part-00000-of-00500.csv.gz")
jobsEntries = jobsFile.map(lambda x: x.split(',')) #split to take separated fields

tasksFile = sc.textFile("./task_events/part-00000-of-00500.csv.gz")
tasksEntries = tasksFile.map(lambda x: x.split(',')) #split to take separated fields

# we count the number of jobs per schedule_class
schedule_class = 5
job_distribution = (
    jobsEntries.map(lambda x: (x[schedule_class], 1))
               .reduceByKey(lambda a, b: a + b)
)

# we count the number of tasks per schedule_class
schedule_class = 7
task_distribution = (
    tasksEntries.map(lambda x: (x[schedule_class], 1))
                .reduceByKey(lambda a, b: a + b)
)
print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")

print("Jobs distribution: ", job_distribution.collect())
print("Tasks distribution: ", task_distribution.collect())

# single plot of results
classes, counts = zip(*job_distribution.collect())
plt.bar(classes, counts)
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Jobs")
plt.title("Jobs Distribution by Scheduling Class")
plt.show()

classes, counts = zip(*task_distribution.collect())
plt.bar(classes, counts)
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Tasks")
plt.title("Tasks Distribution by Scheduling Class")
plt.show()

# join(schedule class)
joinedRdd = job_distribution.join(task_distribution)
sumRdd = joinedRdd.mapValues(lambda values: values[0] + values[1])

print(sumRdd.collect())

# Convert task_distribution in two list for the graph
classes, counts = zip(*sumRdd.collect())
plt.bar(classes, counts)
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Jobs/Tasks")
plt.title("Jobs/Tasks Distribution by Scheduling Class")
plt.show()

input("Press Enter ")
sc.stop()
