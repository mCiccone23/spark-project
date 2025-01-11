#analizzare il tasso di fallimento dei task in base alla classe di scheduling. 
# Questo calcola il numero di fallimenti per ogni classe di scheduling, il totale dei task per classe,
# e il tasso di fallimento, rappresentandolo poi in un grafico a barre.

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, count, when

# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("FailedTasksSchedulingClass") \
    .master("local[1]") \
    .getOrCreate()

# Carica i dati degli eventi di task
tasks_events_df = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)
tasks_events_df = tasks_events_df.withColumnRenamed("_c0", "time") \
                                 .withColumnRenamed("_c2", "job_id") \
                                 .withColumnRenamed("_c3", "task_index") \
                                 .withColumnRenamed("_c4", "machine_id") \
                                 .withColumnRenamed("_c5", "event_type") \
                                 .withColumnRenamed("_c7", "scheduling_class")

# Filtra solo i task falliti (event_type == 3)
tasks_events_failed = tasks_events_df.filter(tasks_events_df.event_type == 3)

# Conta i task falliti per classe di scheduling
failed_by_class = tasks_events_failed.groupBy("scheduling_class").agg(count("event_type").alias("failed_tasks"))

# Conta il totale dei task per classe di scheduling
total_by_class = tasks_events_df.groupBy("scheduling_class").agg(count("event_type").alias("total_tasks"))

# Unisce i dati di fallimenti e totali
tasks_failure_rate = failed_by_class.join(total_by_class, "scheduling_class")

# Calcola il tasso di fallimento per classe di scheduling
tasks_failure_rate = tasks_failure_rate.withColumn("failure_rate", 
                                                  col("failed_tasks") / col("total_tasks"))

# Ordina per classe di scheduling
tasks_failure_rate = tasks_failure_rate.orderBy("scheduling_class")

# Converte in Pandas DataFrame per la visualizzazione
tasks_failure_rate_pd = tasks_failure_rate.toPandas()

# Visualizza i risultati
plt.figure(figsize=(10, 6))
plt.bar(tasks_failure_rate_pd["scheduling_class"], tasks_failure_rate_pd["failure_rate"], color='skyblue')
plt.title("Tasso di fallimento per classe di scheduling")
plt.xlabel("Classe di scheduling")
plt.ylabel("Tasso di fallimento")
plt.grid(axis='y')
plt.show()

# Stampa i risultati
print(tasks_failure_rate_pd)

# Arresta SparkSession
spark.stop()
