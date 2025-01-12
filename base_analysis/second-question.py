from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, lag, lead, sum as spark_sum
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .appName("Computational Power Lost") \
    .getOrCreate()

wholeFile = spark.read.csv("./part-00000-of-00001.csv.gz")

#entries = wholeFile.map(lambda x: x.split(','))

#window for order the data by timestamp for each machine id
window = Window.partitionBy(wholeFile[1]).orderBy(wholeFile[0])
start = time.time()
# we add the column "next_time" and "next_event" to be able to do the analysis.
dfExtended = wholeFile.withColumn("next_time", lead(wholeFile[0]).over(window)).withColumn("next_event", lead(wholeFile[2]).over(window))

# we retain only the line with event type 1 and then next event 0
dfFiltered = dfExtended.filter((col("_c2") == 1) & (col("next_event") == 0))

# we compute the downtime
dfDowntime = dfFiltered.withColumn("downtime", col("next_time") - col("_c0"))

# we compute the downtime weighted on the CPU
dfDowntime = dfDowntime.withColumn("capacity_lost", col("downtime") * col("_c4"))

print("Execution time: ", time.time() - start )

# we collect all the capacity lost
total_lost_capacity = dfDowntime.agg(spark_sum("capacity_lost").alias("total_lost_capacity")).collect()[0][0]

print("Total lost capacity: ", total_lost_capacity)

# we compute the potential total available capacity
total_available_capacity = wholeFile.withColumn("total_time", lead("_c0").over(window) - col("_c0")) \
                             .withColumn("capacity_time", col("total_time") * col("_c4")) \
                             .agg(spark_sum("capacity_time").alias("total_available_capacity")).collect()[0][0]

print("Total available capacity: ", total_available_capacity)
percentage_lost = (total_lost_capacity / total_available_capacity) * 100

# Show result
print(f"Percentage of computation lost: {percentage_lost:.2f}%")

input("Press Enter ")
# Close Spark session
spark.stop()