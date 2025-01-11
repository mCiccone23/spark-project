from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, collect_set
import matplotlib.pyplot as plt
import time

# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("Task Analysis") \
    .master("local[1]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Leggi il file CSV
tasks_df = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Indici delle colonne
scheduling_class = 7
job_id = 2
machine_id = 4

start = time.time()

# Filtra i task con scheduling_class == 1
filtered_df = tasks_df.filter(col(f"_c{scheduling_class}") == 1)

# Seleziona coppie (job_id, machine_id)
job_machine_df = filtered_df.select(col(f"_c{job_id}").alias("job_id"), 
                                    col(f"_c{machine_id}").alias("machine_id"))

# Raggruppa per job_id e conta le macchine distinte
machines_per_job_df = job_machine_df.groupBy("job_id").agg(countDistinct("machine_id").alias("distinct_machines"))
machines_per_job_df.cache()

# Conta i job che girano su una sola macchina
jobs_on_one_machine = machines_per_job_df.filter(col("distinct_machines") == 1).count()

# Conta il numero totale di job
total_jobs = machines_per_job_df.count()

# Calcola la percentuale
percentage_same_machine = (jobs_on_one_machine / total_jobs) * 100
print(f"Jobs on one machine: {jobs_on_one_machine}, Total jobs: {total_jobs}")
print(f"Percentage of jobs running on the same machine: {percentage_same_machine:.2f}%")

# Campiona i dati per visualizzazione
sample = machines_per_job_df.limit(100).collect()
jobs = [row["job_id"] for row in sample]
machines = [row["distinct_machines"] for row in sample]

end = time.time() - start
print("Execution time:", end)

# Visualizza la distribuzione
plt.scatter(jobs, machines, color='red')
plt.xlabel("Job ID")
plt.ylabel("Number of Machines")
plt.title("Sample of Task Distribution by Machine")
plt.show()

input("Press Enter to exit")

# Chiudi la sessione Spark
spark.stop()
