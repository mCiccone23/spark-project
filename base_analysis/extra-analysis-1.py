#Correlation between priority and failure
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum
import matplotlib.pyplot as plt
import numpy as np


spark = SparkSession.builder \
    .appName("Task Priority Correlation Analysis") \
    .getOrCreate()
#Loading
task_events = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

columns = [
    "timestamp", "missing_info", "job_id", "task_index", "machine_id",
    "event_type", "user_name", "scheduling_class", "priority",
    "cpu_request", "ram_request", "disk_request", "different_machine_constraint"
]
task_events = task_events.toDF(*columns)

#Filtered only event_type in (FAIL = 3, KILL = 5, EVICT = 2)
failures = task_events.filter(col("event_type").isin(3, 5, 2))

#Total number of tasks per priority and total number of failure per priority. 
task_totals = task_events.groupBy("priority").agg(count("event_type").alias("total_tasks"))
failure_totals = failures.groupBy("priority").agg(count("event_type").alias("failure_count"))

#Join for failure rate computation
result = task_totals.join(failure_totals, "priority", "left")
result = result.withColumn("failure_rate", 
                           (col("failure_count") / col("total_tasks")) * 100)

#Data for plot
result_pd = result.select("priority", "failure_rate", "total_tasks").orderBy("priority").toPandas()

# Plot
fig, ax1 = plt.subplots(figsize=(12, 6))

#Failure rate
bar_width = 0.4
bar_positions = np.arange(len(result_pd))
ax1.bar(bar_positions, result_pd["failure_rate"], bar_width, color='blue', alpha=0.7, label='Failure Rate (%)')

# Task distribution
ax2 = ax1.twinx()
ax2.plot(bar_positions, result_pd["total_tasks"], color='orange', marker='o', label='Total Tasks')


ax1.set_xlabel("Task Priority")
ax1.set_ylabel("Failure Rate (%)", color='blue')
ax2.set_ylabel("Total Tasks", color='orange')
ax1.set_title("Failure Rate and Task Distribution by Priority")
ax1.set_xticks(bar_positions)
ax1.set_xticklabels(result_pd["priority"].astype(str), rotation=45)
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')
plt.tight_layout()
plt.show()


input("Press Enter to exit")

spark.stop()
