#• In general, do tasks from the same job run on the same machine?
# filtro task events per ottenere solo i task SCHEDULATI == 1
# map (job_id,machine_id) --> distinct per il count delle machine diverse
# 
#

from pyspark import SparkContext

import matplotlib.pyplot as plt
 

sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

tasksFile = sc.textFile("./task_events/part-00000-of-00500.csv.gz")

entries = tasksFile.map(lambda x: x.split(','))


scheduling_class = 7
job_id = 2
machine_id = 4


#filter scheduling == 1 [SCHEDULED], map (job,machine)

job_machine_pairs = entries.filter(lambda x: x[scheduling_class] == '1').map(lambda x: (x[job_id], x[machine_id]))


#raggruppo per job e conto il numero di machine diverse per job (diverse perchè sto usando un set)
machines_per_job = job_machine_pairs.groupByKey().mapValues(lambda m: len(set(m)))

#conto quanti jobs sono runnati su una sola macchina
jobs_on_one_machine = machines_per_job.filter(lambda x: x[1] == 1).count()
#conto i jobs totali
total_jobs = machines_per_job.count()
#rapporto 
percentage_same_machine = (jobs_on_one_machine / total_jobs) * 100
print(f"Percentage of jobs running on the same machine: {percentage_same_machine:.2f}%")


#stampo la distribuzione dei task dei jobs per il numero macchine
sample = machines_per_job.take(100)  # Prendi solo un campione
jobs = [x[0] for x in sample]
machines = [x[1] for x in sample]

plt.scatter(jobs, machines, color='red')
plt.xlabel("Job ID")
plt.ylabel("Number of machine")
plt.title("Sample of Task Distribution by Machine")
plt.show()


# Close Spark session

sc.stop()

input("Press Enter ")
 