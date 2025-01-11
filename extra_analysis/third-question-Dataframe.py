from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
import matplotlib.pyplot as plt
import time

spark = SparkSession.builder \
    .appName("Jobs and Tasks Distribution") \
    .getOrCreate()

start_time = time.time()

jobsFile = spark.read.csv("../job_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)
tasksFile = spark.read.csv("../task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# we count the number of jobs per schedule_class
job_distribution = (
    jobsFile.groupBy(col("_c5").alias("schedule_class"))
            .agg(spark_sum(lit(1)).alias("job_count"))
            .orderBy("schedule_class")
)

# we count the number of tasks per schedule_class
task_distribution = (
    tasksFile.groupBy(col("_c7").alias("schedule_class"))
             .agg(spark_sum(lit(1)).alias("task_count"))
             .orderBy("schedule_class")
)
print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")


print("Jobs Distribution:")
job_distribution.show()

print("Tasks Distribution:")
task_distribution.show()

# join(schedule class)
joined_df = job_distribution.join(task_distribution, "schedule_class")
joined_df = joined_df.withColumn("total", col("job_count") + col("task_count"))

print("Joined Distribution:")
joined_df.show()

# single plot of results
job_pandas = job_distribution.toPandas()
plt.bar(job_pandas["schedule_class"], job_pandas["job_count"])
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Jobs")
plt.title("Jobs Distribution by Scheduling Class")
plt.show()

task_pandas = task_distribution.toPandas()
plt.bar(task_pandas["schedule_class"], task_pandas["task_count"])
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Tasks")
plt.title("Tasks Distribution by Scheduling Class")
plt.show()

# Convert task_distribution in two list for the graph
joined_pandas = joined_df.select("schedule_class", "total").toPandas()
plt.bar(joined_pandas["schedule_class"], joined_pandas["total"])
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Jobs/Tasks")
plt.title("Jobs/Tasks Distribution by Scheduling Class")
plt.show()


input("Press enter")
spark.stop()