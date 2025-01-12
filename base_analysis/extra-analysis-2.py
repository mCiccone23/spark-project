from pyspark import SparkContext
import matplotlib.pyplot as plt

# Inizializza il contesto Spark
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# Carica i file CSV dei task
task_events = sc.textFile("./task_events/part-00000-of-00500.csv.gz")

# Suddividi le righe in colonne
task_events_parsed = task_events.map(lambda x: x.split(','))

# Estrai le risorse richieste (CPU e memoria)
# Indici: CPU richiesta (10), Memoria richiesta (11)
cpu_memory_requests = task_events_parsed.map(lambda x: (float(x[9]), float(x[10])) if x[9] != '' and x[10] != '' else None).filter(lambda x: x is not None)

# Conta il numero di task per valori unici di CPU e Memoria richiesti
cpu_distribution = cpu_memory_requests.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).collect()
memory_distribution = cpu_memory_requests.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).collect()

# Ordina i dati per creare i grafici
cpu_distribution = sorted(cpu_distribution, key=lambda x: x[0])
memory_distribution = sorted(memory_distribution, key=lambda x: x[0])

# Separazione delle chiavi (capacità) e dei valori (conteggi)
cpu_values, cpu_counts = zip(*cpu_distribution)
memory_values, memory_counts = zip(*memory_distribution)

# Grafico: Distribuzione delle richieste di CPU
plt.figure(figsize=(10, 5))
plt.bar(cpu_values, cpu_counts, width=0.02, edgecolor="black")
plt.xlabel("CPU Requested")
plt.ylabel("Number of Tasks")
plt.title("Distribution of CPU Requests")
plt.grid(True)
plt.show()

# Grafico: Distribuzione delle richieste di Memoria
plt.figure(figsize=(10, 5))
plt.bar(memory_values, memory_counts, width=0.02, edgecolor="black", color='orange')
plt.xlabel("Memory Requested")
plt.ylabel("Number of Tasks")
plt.title("Distribution of Memory Requests")
plt.grid(True)
plt.show()

# Chiudi la sessione Spark
sc.stop()
