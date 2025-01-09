from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as _sum
import matplotlib.pyplot as plt
import time
from pyspark.sql.functions import format_string


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EvictionRateAnalysis") \
    .master("local[1]") \
    .getOrCreate()

# Load data
tasks_df = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Define columns for analysis
event_type = "_c5"  # Column 5
schedule_class = "_c7"  # Column 7

# Start timer
start = time.time()

# Add eviction column: 1 if event_type is '2' (evicted), otherwise 0
tasks_df = tasks_df.withColumn("evicted", when(col(event_type) == "2", 1).otherwise(0))

# Group by scheduling class and calculate total tasks and evicted tasks
result_df = tasks_df.groupBy(col(schedule_class).alias("scheduling_class")) \
    .agg(
        count("*").alias("total_count"),
        _sum("evicted").alias("evicted_count")
    ) \
    .withColumn("eviction_rate", format_string("%.2f%%", (col("evicted_count") / col("total_count")) * 100))

# Show results
result_df.show()

# Collect results for plotting
results = result_df.select("scheduling_class", "eviction_rate").collect()
# Convert eviction rates to percentages for plotting
scheduling_classes = [row["scheduling_class"] for row in results]
eviction_rates = [float(row["eviction_rate"][:-1]) for row in results]  # Rimuove il '%' e converte in float

# Plot
plt.figure(figsize=(10, 6))
plt.bar(scheduling_classes, eviction_rates, color='skyblue')
plt.xlabel('Scheduling Class')
plt.ylabel('Eviction Rate (%)')  # Asse y con percentuale
plt.title('Eviction Rate Comparison by Scheduling Class')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

print("Execution time: ", time.time() - start)

# Compute correlation between event_type and scheduling_class
tasks_df_filtered = tasks_df.filter((col(event_type).cast("float").isNotNull()) & (col(schedule_class).cast("float").isNotNull()))
correlation = tasks_df_filtered.stat.corr(event_type, schedule_class)

print(f"Correlation between event_type and schedule_class: {correlation}")

input("Press Enter to exit")

# Stop Spark session
spark.stop()
