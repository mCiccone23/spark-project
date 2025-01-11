from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, corr, count
import matplotlib.pyplot as plt
import time as t
###RIVEDERE CALCOLO CORRELATION SCORRETTO
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CorrelationResourceUsageEviction") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for better performance (optional but recommended)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Load task_events as DataFrame
task_events_schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("missing_info", StringType(), True),
    StructField("job_id", StringType(), True),
    StructField("task_index", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("event_type", IntegerType(), True),
    StructField("user", StringType(), True),
    StructField("scheduling_class", IntegerType(), True),
    StructField("priority", IntegerType(), True),
    StructField("cpu_request", FloatType(), True),
    StructField("memory_request", FloatType(), True),
    StructField("disk_request", FloatType(), True),
    StructField("different_machine_constraint", StringType(), True)
])

task_events = spark.read.csv("../task_events/part-00000-of-00500.csv.gz", schema=task_events_schema)

# Load task_usage as DataFrame
task_usage_schema = StructType([
    StructField("start_time", IntegerType(), True),
    StructField("end_time", IntegerType(), True),
    StructField("job_id", StringType(), True),
    StructField("task_index", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("mean_cpu_usage", FloatType(), True),
    StructField("canonical_mem_usage", FloatType(), True),
    StructField("assigned_mem_usage", FloatType(), True),
    StructField("unmapped_page_cache", FloatType(), True),
    StructField("total_page_cache", FloatType(), True),
    StructField("max_mem_usage", FloatType(), True),
    StructField("mean_disk_io_time", FloatType(), True),
    StructField("mean_local_disk_space", FloatType(), True),
    StructField("max_cpu_usage", FloatType(), True),
    StructField("max_disk_io_time", FloatType(), True)
])

task_usage = spark.read.csv("../task_usage/part-00000-of-00500.csv.gz", schema=task_usage_schema)

# Filter evicted tasks
evicted_tasks = task_events.filter(col("event_type") == 2).select("machine_id", "time")

# Join task usage with evicted tasks on machine_id
joined_data = evicted_tasks.join(
    task_usage,
    (evicted_tasks.machine_id == task_usage.machine_id) &
    (evicted_tasks.time >= task_usage.start_time) &
    (evicted_tasks.time <= task_usage.end_time)
).select("max_mem_usage", "max_cpu_usage", "max_disk_io_time")

# Compute correlations
#correlation_mem = joined_data.stat.corr("max_mem_usage", "max_disk_io_time")
#correlation_cpu = joined_data.stat.corr("max_cpu_usage", "max_disk_io_time")
#correlation_disk = joined_data.stat.corr("max_mem_usage", "max_cpu_usage")

print(f"Correlation between memory and disk usage: {correlation_mem}") # type: ignore
print(f"Correlation between CPU and disk usage: {correlation_cpu}") # type: ignore
print(f"Correlation between memory and CPU usage: {correlation_disk}") # type: ignore

# Visualization
start_time = t.time()
data_mem = joined_data.groupBy("max_mem_usage").agg(count(lit(1)).alias("evictions"))
data_cpu = joined_data.groupBy("max_cpu_usage").agg(count(lit(1)).alias("evictions"))
data_disk = joined_data.groupBy("max_disk_io_time").agg(count(lit(1)).alias("evictions"))

# Convert to Pandas for visualization
df_mem = data_mem.toPandas()
df_cpu = data_cpu.toPandas()
df_disk = data_disk.toPandas()

# Plot Memory
plt.bar(df_mem["max_mem_usage"], df_mem["evictions"], color='blue')
plt.xlabel("Max Memory Usage")
plt.ylabel("Number of Evictions")
plt.title("Memory Peaks vs Evictions")
plt.show()

# Plot CPU
plt.bar(df_cpu["max_cpu_usage"], df_cpu["evictions"], color='orange')
plt.xlabel("Max CPU Usage")
plt.ylabel("Number of Evictions")
plt.title("CPU Peaks vs Evictions")
plt.show()

# Plot Disk
plt.bar(df_disk["max_disk_io_time"], df_disk["evictions"], color='green')
plt.xlabel("Max Disk Usage")
plt.ylabel("Number of Evictions")
plt.title("Disk Peaks vs Evictions")
plt.show()

end_time = t.time() - start_time
print(f"Execution time for DataFrame: {end_time}")
