
#L'analisi effettuata consiste nel verificare la relazione tra la priorità dei task e due aspetti principali:

#Tasso di fallimento dei task:

#Calcola la percentuale di task che falliscono (eventi FAIL, KILL, EVICT) rispetto al totale dei task per ciascun livello di priorità.
#Questo permette di osservare se alcune priorità sono più vulnerabili a interruzioni o fallimenti.
#Distribuzione dei task per priorità:

#Mostra il numero totale di task per ciascun livello di priorità.
#Questo evidenzia come i task siano distribuiti tra le varie priorità, indicando quali priorità sono più utilizzate.
#Risultati osservati:
#Alcune priorità hanno alti tassi di fallimento (ad esempio, priorità 0) che potrebbero indicare problemi di gestione o risorse insufficienti per task a bassa priorità.
#Altre priorità con molti task (come 9) mostrano un basso tasso di fallimento, suggerendo un'alta affidabilità o stabilità.
#Priorità meno comuni, come 1, hanno pochi task e un basso tasso di fallimento, probabilmente perché sono riservate a task particolari o critici.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum
import matplotlib.pyplot as plt
import numpy as np

# Inizializza la SparkSession
spark = SparkSession.builder \
    .appName("Task Priority Correlation Analysis") \
    .getOrCreate()

task_events = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Assegna nomi ai campi rilevanti basandoti sullo schema (adatta se necessario)
columns = [
    "timestamp", "missing_info", "job_id", "task_index", "machine_id",
    "event_type", "user_name", "scheduling_class", "priority",
    "cpu_request", "ram_request", "disk_request", "different_machine_constraint"
]
task_events = task_events.toDF(*columns)

# Filtra solo gli eventi di interesse (FAIL = 3, KILL = 5, EVICT = 2)
failures = task_events.filter(col("event_type").isin(3, 5, 2))

# Calcola il totale dei task e il totale dei fallimenti per ciascuna priorità
task_totals = task_events.groupBy("priority").agg(count("event_type").alias("total_tasks"))
failure_totals = failures.groupBy("priority").agg(count("event_type").alias("failure_count"))

# Unisci i dati e calcola il tasso di fallimento
result = task_totals.join(failure_totals, "priority", "left")
result = result.withColumn("failure_rate", 
                           (col("failure_count") / col("total_tasks")) * 100)

# Converti i risultati in un DataFrame Pandas per il plot
result_pd = result.select("priority", "failure_rate", "total_tasks").orderBy("priority").toPandas()

# Plot dei risultati
fig, ax1 = plt.subplots(figsize=(12, 6))

# Grafico a barre per il tasso di fallimento
bar_width = 0.4
bar_positions = np.arange(len(result_pd))
ax1.bar(bar_positions, result_pd["failure_rate"], bar_width, color='blue', alpha=0.7, label='Failure Rate (%)')

# Aggiungi una seconda asse per la distribuzione dei task
ax2 = ax1.twinx()
ax2.plot(bar_positions, result_pd["total_tasks"], color='orange', marker='o', label='Total Tasks')

# Configura gli assi
ax1.set_xlabel("Task Priority")
ax1.set_ylabel("Failure Rate (%)", color='blue')
ax2.set_ylabel("Total Tasks", color='orange')
ax1.set_title("Failure Rate and Task Distribution by Priority")
ax1.set_xticks(bar_positions)
ax1.set_xticklabels(result_pd["priority"].astype(str), rotation=45)

# Aggiungi legenda
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')

# Mostra il grafico
plt.tight_layout()
plt.show()

# Salva i risultati su disco (opzionale)
result_pd.to_csv("failure_rate_and_distribution_by_priority.csv", index=False)
