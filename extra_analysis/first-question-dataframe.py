from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import time

spark = SparkSession.builder \
    .appName("CPU Distribution Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
    .getOrCreate() 

df = spark.read.csv("../part-00000-of-00001.csv.gz", header=False, inferSchema=True)

# Index columns
cpu_capacity_index = 4
machine_id_index = 1

df = df.repartition(4, col(f"_c{cpu_capacity_index}"))

start_time = time.time()

# Select distinct machines and their CPU capacity
cpu_distribution_df = (
    df.select(col(f"_c{machine_id_index}").alias("machine_id"), col(f"_c{cpu_capacity_index}").alias("cpu_capacity"))
    .distinct()
    .filter(col("cpu_capacity").isNotNull())
)

# Group by CPU capacity and count machines
cpu_distribution_df = (
    cpu_distribution_df.groupBy("cpu_capacity")
    .count()
    .orderBy("cpu_capacity")
)

print("Execution time: ", time.time() - start_time)
cpu_distribution_df.show()

cpu_distribution_pandas = cpu_distribution_df.toPandas()

# Plot CPU distribution
capacities = cpu_distribution_pandas['cpu_capacity'].astype(float)
counts = cpu_distribution_pandas['count']

plt.bar(capacities, counts, width=0.05, edgecolor="black", facecolor="none", linewidth=1.5)
plt.xlabel("CPU capacity")
plt.ylabel("Number of Machines")
plt.title("Distribution of Machines by CPU Capacity")
plt.xticks(capacities, labels=capacities, rotation=90)
plt.show()

input("Press Enter to exit")

spark.stop()
