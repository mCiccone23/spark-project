from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit
import matplotlib.pyplot as plt

# Inizializza la SparkSession
spark = SparkSession.builder \
    .appName("Machine Performance Analysis") \
    .getOrCreate()

# Carica i dati del task_events
task_events = spark.read.csv("./task_events/part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Assegna nomi ai campi rilevanti
columns = [
    "timestamp", "missing_info", "job_id", "task_index", "machine_id",
    "event_type", "user_name", "scheduling_class", "priority",
    "cpu_request", "ram_request", "disk_request", "different_machine_constraint"
]
task_events = task_events.toDF(*columns)

# Filtra solo gli eventi di interesse (FAIL = 3, KILL = 5, EVICT = 2) e verifica che machine_id non sia nullo
failures = task_events.filter((col("event_type").isin(3, 5, 2)) & (col("machine_id").isNotNull()))

# Definisci le fasce di CPU e RAM
cpu_bins = [
    when(col("cpu_request") <= 0.1, lit("0.0-0.1"))
    .when((col("cpu_request") > 0.1) & (col("cpu_request") <= 0.3), lit("0.1-0.3"))
    .when((col("cpu_request") > 0.3) & (col("cpu_request") <= 0.5), lit("0.3-0.5"))
    .when((col("cpu_request") > 0.5) & (col("cpu_request") <= 0.7), lit("0.5-0.7"))
    .when((col("cpu_request") > 0.7) & (col("cpu_request") <= 1.0), lit("0.7-1.0"))
    .otherwise(lit("1.0+"))
]
ram_bins = [
    when(col("ram_request") <= 0.1, lit("0.0-0.1"))
    .when((col("ram_request") > 0.1) & (col("ram_request") <= 0.3), lit("0.1-0.3"))
    .when((col("ram_request") > 0.3) & (col("ram_request") <= 0.5), lit("0.3-0.5"))
    .when((col("ram_request") > 0.5) & (col("ram_request") <= 0.7), lit("0.5-0.7"))
    .when((col("ram_request") > 0.7) & (col("ram_request") <= 1.0), lit("0.7-1.0"))
    .otherwise(lit("1.0+"))
]

# Aggiungi le fasce come nuove colonne
failures = failures.withColumn("CPU_Bin", cpu_bins[0]) \
                   .withColumn("RAM_Bin", ram_bins[0])

# Raggruppa per fasce e conta i fallimenti
failures_by_bins = (
    failures.groupBy("CPU_Bin", "RAM_Bin")
    .agg(count("event_type").alias("failure_count"))
    .orderBy(col("failure_count").desc())
)

# Converti i dati in un DataFrame Pandas
failures_by_bins_pd = failures_by_bins.toPandas()

# Visualizza i dati con un bar plot
plt.figure(figsize=(12, 6))
for ram_bin in failures_by_bins_pd["RAM_Bin"].unique():
    subset = failures_by_bins_pd[failures_by_bins_pd["RAM_Bin"] == ram_bin]
    plt.bar(
        subset["CPU_Bin"],
        subset["failure_count"],
        label=f"RAM: {ram_bin}",
        alpha=0.7
    )

plt.xlabel("CPU Bin")
plt.ylabel("Failure Count")
plt.title("Failures Grouped by CPU and RAM Bins")
plt.legend(title="RAM Bins")
plt.tight_layout()
plt.show()

# Salva i risultati su disco (opzionale)
failures_by_bins_pd.to_csv("failures_by_bins.csv", index=False)
