from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import matplotlib.pyplot as plt
import pandas as pd

# Inizializza la SparkSession
spark = SparkSession.builder \
    .appName("Machine Performance Analysis") \
    .getOrCreate()

# Carica i dati del task_events (sostituisci 'path_to_task_events' con il percorso corretto)
task_events = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Assegna nomi ai campi rilevanti basandoti sullo schema (adatta se necessario)
columns = [
    "timestamp", "missing_info", "job_id", "task_index", "machine_id",
    "event_type", "user_name", "scheduling_class", "priority",
    "cpu_request", "ram_request", "disk_request", "different_machine_constraint"
]
task_events = task_events.toDF(*columns)

# Filtra solo gli eventi di interesse (FAIL = 3, KILL = 5, EVICT = 2) e verifica che machine_id non sia nullo
failures = task_events.filter((col("event_type").isin(3, 5, 2)) & (col("machine_id").isNotNull()))

# Conta il numero di fallimenti per macchina
failures_per_machine = failures.groupBy("machine_id").agg(count("event_type").alias("failure_count"))

# Ordina per numero di fallimenti in ordine decrescente
failures_per_machine_sorted = failures_per_machine.orderBy(col("failure_count").desc())

# Converti i risultati in un DataFrame Pandas per il plot
failures_pd = failures_per_machine_sorted.limit(20).toPandas()

# Plot della distribuzione dei fallimenti per le prime 20 macchine (line plot)
plt.figure(figsize=(12, 6))
plt.plot(failures_pd["machine_id"].astype(str), failures_pd["failure_count"], marker='o', linestyle='-', color='red', alpha=0.7)
plt.xlabel("Machine ID")
plt.ylabel("Failure Count")
plt.title("Top 20 Machines by Failure Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Salva i risultati su disco (opzionale)
failures_pd.to_csv("machine_failures_distribution.csv", index=False)
