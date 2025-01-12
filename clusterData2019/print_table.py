from pyspark.sql import SparkSession

# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("Simple Test Script with Save") \
    .getOrCreate()

# Percorso del file di test
data_path = "gs://clusterdata_2019_a/collection_events-000000000000.json.gz"  # Modifica con un file esistente
output_path = "gs://spark-project-ciccone/test_output/"  # Percorso per salvare il risultato

try:
    # Legge il file JSON
    df = spark.read.json(data_path)
    print("File letto con successo!")

    # Mostra i primi 10 record per verifica
    df.show(10, truncate=False)
    limited_df = df.limit(10)
    print(f"Numero di righe nel DataFrame limitato: {limited_df.count()}")
    limited_df.write.csv(output_path, header=True, mode="overwrite")
    print(f"File salvato con successo nel bucket: {output_path}")

    limited_df.write.csv(output_path, header=True, mode="overwrite")


    print(f"File salvato con successo nel bucket: {output_path}")

except Exception as e:
    print("Errore durante l'elaborazione:", e)

# Chiude la sessione Spark
spark.stop()
