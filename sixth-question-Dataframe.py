from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, corr
import matplotlib.pyplot as plt
import time
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Correlation Analysis with DataFrame") \
    .getOrCreate()

start_time = time.time()

# Load the task events data
# Selecting relevant columns: job_id, task_index, cpu_required, memory_required
tasksEvents = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)
tasksEventsDF = tasksEvents.select(
    col("_c2").alias("job_id"),
    col("_c3").alias("task_index"),
    col("_c9").alias("cpu_required"),
    col("_c10").alias("memory_required")
).distinct()

# Load the task usage data
# Selecting relevant columns: job_id, task_index, cpu_used, memory_used, and calculating averages
tasksUsage = spark.read.csv("./task_usage/part-00000-of-00500.csv.gz", header=False, inferSchema=True)
tasksUsageDF = tasksUsage.select(
    col("_c2").alias("job_id"),
    col("_c3").alias("task_index"),
    col("_c5").alias("cpu_used"),
    col("_c6").alias("memory_used")
).groupBy("job_id", "task_index").agg(
    avg(col("cpu_used")).alias("avg_cpu_used"),
    avg(col("memory_used")).alias("avg_memory_used")
)

# Join the two DataFrames on job_id and task_index
joinedDF = tasksEventsDF.join(tasksUsageDF, ["job_id", "task_index"])

# Filter out rows with null values in the relevant columns
cleanedDF = joinedDF.filter(
    (col("cpu_required").isNotNull()) &
    (col("avg_cpu_used").isNotNull()) &
    (col("memory_required").isNotNull()) &
    (col("avg_memory_used").isNotNull())
)

# Calculate the correlation between cpu_required and avg_cpu_used
cpu_corr = cleanedDF.corr("cpu_required", "avg_cpu_used")

# Calculate the correlation between memory_required and avg_memory_used
memory_corr = cleanedDF.corr("memory_required", "avg_memory_used")
print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")

print(f"CPU correlation: {cpu_corr}")
print(f"Memory correlation: {memory_corr}")

# Prepare data for scatter plots
# Collecting required columns to local Python lists for plotting
data = cleanedDF.select(
    col("cpu_required").alias("requested_cpu"),
    col("avg_cpu_used").alias("used_cpu"),
    col("memory_required").alias("requested_memory"),
    col("avg_memory_used").alias("used_memory")
).collect()

requested_cpu = [row["requested_cpu"] for row in data]
used_cpu = [row["used_cpu"] for row in data]
requested_memory = [row["requested_memory"] for row in data]
used_memory = [row["used_memory"] for row in data]

# Scatter plot for CPU
plt.figure(figsize=(10, 5))
plt.scatter(requested_cpu, used_cpu, alpha=0.5)
plt.xlabel("CPU Requested")
plt.ylabel("CPU Used")
plt.title("Scatter Plot: CPU Requested vs CPU Used")
plt.grid(True)
plt.show()

# Scatter plot for Memory
plt.figure(figsize=(10, 5))
plt.scatter(requested_memory, used_memory, color='orange', alpha=0.5)
plt.xlabel("Memory Requested")
plt.ylabel("Memory Used")
plt.title("Scatter Plot: Memory Requested vs Memory Used")
plt.grid(True)
plt.show()

input("Press enter")
# Stop the SparkSession
spark.stop()
