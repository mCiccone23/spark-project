from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import time as t

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Task Usage and Events Analysis") \
    .getOrCreate()


# Load Data
tasksEvents = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)
tasksUsage = spark.read.csv("./task_usage/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Define column indexes
col_event_type = 5
col_machine_id = 4
col_task_index = 3
col_job_id = 2
col_max_mem = 10
col_max_cpu = 13

# Select and rename columns
tasksEvents = tasksEvents.select(
    tasksEvents._c2.alias("job_id"),
    tasksEvents._c3.alias("task_index"),
    tasksEvents._c4.alias("machine_id"),
    tasksEvents._c5.alias("event_type")
)

tasksUsage = tasksUsage.select(
    tasksUsage._c2.alias("job_id"),
    tasksUsage._c3.alias("task_index"),
    tasksUsage._c4.alias("machine_id"),
    tasksUsage._c10.alias("max_mem"),
    tasksUsage._c13.alias("max_cpu")
)

# Start timer
start = t.time()
# Add unique job_task_id
tasksEvents = tasksEvents.withColumn("job_task_id", tasksEvents.job_id.cast("string") + tasksEvents.task_index.cast("string"))
tasksUsage = tasksUsage.withColumn("job_task_id", tasksUsage.job_id.cast("string") + tasksUsage.task_index.cast("string"))

# Find the peaks of resource usage
from pyspark.sql import functions as F

tasksUsagePeaks = tasksUsage.groupBy("job_task_id", "machine_id") \
    .agg(
        F.max("max_mem").alias("peak_max_mem"),
        F.max("max_cpu").alias("peak_max_cpu")
    )

# Join the datasets
taskUsageEvents = tasksUsagePeaks.join(
    tasksEvents,
    on=["job_task_id", "machine_id"],
    how="inner"
)

# Filter for event_type == 2
evicted_events = taskUsageEvents.filter(taskUsageEvents.event_type == 2)

# Collect data for plotting
data = evicted_events.select("peak_max_mem", "peak_max_cpu").collect()
filtered_max_mem = [float(row["peak_max_mem"]) for row in data]
filtered_max_cpu = [float(row["peak_max_cpu"]) for row in data]

# End timer and print execution time
end = t.time()
print(f"Execution Time: {end - start} seconds")

# Plot the data
plt.figure(figsize=(10, 6))
plt.scatter(filtered_max_mem, filtered_max_cpu, c='red', alpha=0.6, label='event_type = 2')
plt.xlabel('Max Memory')
plt.ylabel('Max CPU')
plt.title('Scatter Plot of Max Memory and Max CPU (event_type = 2)')
plt.grid(True)
plt.legend()
plt.show()

# Stop SparkSession
spark.stop()
