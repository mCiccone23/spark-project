from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import time
##RIVEDERE RISULTATI DIVERSI
# Inizializzare la sessione Spark
spark = SparkSession.builder \
    .appName("Task Distribution Analysis") \
    .getOrCreate()

# Configurare il livello di log
spark.sparkContext.setLogLevel("ERROR")

# Caricare i dati come DataFrame
tasks_df = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Definire gli indici delle colonne
scheduling_class_col = 7
job_id_col = 2
machine_id_col = 4

start = time.time()

# Filtrare per task schedulati (scheduling_class == 1) e selezionare job_id, machine_id
job_machine_df = tasks_df.filter(tasks_df[scheduling_class_col] == 1) \
    .select(tasks_df[job_id_col].alias("job_id"), tasks_df[machine_id_col].alias("machine_id"))
    


# Raggruppare per job_id e contare macchine uniche
machines_per_job = job_machine_df.distinct().groupBy("job_id") \
    .agg({"machine_id": "count"}) \
    .withColumnRenamed("count(machine_id)", "unique_machines")

# Contare i job su una sola macchina
jobs_on_one_machine = machines_per_job.filter(machines_per_job["unique_machines"] == 1).count()

# Contare i job totali
total_jobs = machines_per_job.count()
print("1, tot:", jobs_on_one_machine, total_jobs)
# Calcolare la percentuale
percentage_same_machine = (jobs_on_one_machine / total_jobs) * 100

print(f"Jobs on one machine: {jobs_on_one_machine}, Total jobs: {total_jobs}")
print(f"Percentage of jobs running on the same machine: {percentage_same_machine:.2f}%")

# Visualizzare un campione della distribuzione
sample = machines_per_job.limit(100).toPandas()
jobs = sample["job_id"]
machines = sample["unique_machines"]

end = time.time() - start
print("Execution time:", end)

# Grafico
plt.scatter(jobs, machines, color='red')
plt.xlabel("Job ID")
plt.ylabel("Number of Machines")
plt.title("Sample of Task Distribution by Machine")
plt.show()


# Chiudere la sessione Spark
spark.stop()
