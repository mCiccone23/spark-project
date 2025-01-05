from pyspark.sql import SparkSession

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("ClusterData2019Analysis") \
    .getOrCreate()

# Percorso ai file task_usage nel bucket
data_path = "gs://clusterdata-2019/task_usage/*.csv.gz"

# Lettura dei dati
df = spark.read.csv(data_path, header=False, inferSchema=True)

# Aggiungi i nomi delle colonne (usando lo schema della documentazione)
columns = ["start_time", "end_time", "job_id", "task_index", "machine_id",
           "mean_cpu_usage", "canonical_mem_usage", "assigned_mem_usage",
           "unmapped_page_cache", "total_page_cache", "max_mem_usage",
           "mean_disk_io_time", "mean_local_disk_space", "max_cpu_usage",
           "max_disk_io_time", "cycles_per_instruction", "memory_accesses_per_instruction",
           "sample_portion", "aggregation_type"]
df = df.toDF(*columns)

# Analisi: Calcolare il consumo medio di CPU
cpu_usage = df.groupBy("job_id").avg("mean_cpu_usage")
cpu_usage.show()

# Salvataggio dei risultati nel bucket
output_path = "gs://spark-project-ciccone/clusterdata2019/cpu_usage_results.csv"
cpu_usage.write.csv(output_path, header=True)

spark.stop()
