#• Do tasks with a low scheduling class have a higher probability of being evicted?
#scheduling class[7] and event type[5] are the two columns of interest for the analysis
#to answer the question we group the task for scheduling class and then calculate the eviction rate (task evicted/total task) per class
#map (scheduling, event) , [count di tutti / count dei soli evicted]= rate
#mettere in rapporto i vari rates ottenuti per classe 

from pyspark import SparkContext

import matplotlib.pyplot as plt
import math 
def compute_correlation(rdd):
    n = rdd.count()
    sum_x = rdd.map(lambda pair: pair[0]).sum()
    sum_y = rdd.map(lambda pair: pair[1]).sum()
    sum_xy = rdd.map(lambda pair: pair[0] * pair[1]).sum()
    sum_x2 = rdd.map(lambda pair: pair[0] ** 2).sum()
    sum_y2 = rdd.map(lambda pair: pair[1] ** 2).sum()

    numerator = n * sum_xy - sum_x * sum_y
    denominator = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))

    return numerator / denominator


sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

tasksFile = sc.textFile("./task_events/part-00000-of-00500.csv.gz")

entries = tasksFile.map(lambda x: x.split(','))

event_type = 5
schedule_class = 7
# Map (scheduling_class, (event_type, 1))
task_per_schedule = entries.map(lambda x: (x[schedule_class], ((x[event_type]), 1)))

# Totale eventi per scheduling class
total_per_class = task_per_schedule.mapValues(lambda x: x[1]).reduceByKey(lambda a, b: a + b)

# Totale evicted (event_type == '2') per scheduling class
total_evicted_per_class = task_per_schedule.filter(lambda x: x[1][0] == '2').mapValues(lambda x: x[1]).reduceByKey(lambda a, b: a + b)
print(f"Total per class: {total_per_class.collect()}")
print(f"Total evicted per class: {total_evicted_per_class.collect()}")

# combino i totali per scheduling class (schedule,( total_per_class, total_evicted_per_class))
combined = total_per_class.join(total_evicted_per_class)

#tasso di eviction
eviction_rate_per_class = combined.mapValues(lambda x: x[1] / x[0])


results = eviction_rate_per_class.collect()
for schedule_class, rate in results:
    print(f"Scheduling Class: {schedule_class}, Eviction Rate: {rate:.2%}")


scheduling_classes = [schedule_class for schedule_class, _ in results]
eviction_rates = [rate for _, rate in results]

#grafico per mostrare il confronto tra i tassi di eviction
plt.figure(figsize=(10, 6))
plt.bar(scheduling_classes, eviction_rates, color='skyblue')
plt.xlabel('Scheduling Class')
plt.ylabel('Eviction Rate')
plt.title('Eviction Rate Comparison by Scheduling Class')
plt.xticks(rotation=45)
plt.tight_layout()  
plt.show()

schedule_class = 7
data_for_correlation = entries.map(lambda x: (float(x[event_type]), float(x[schedule_class])))
correlation = compute_correlation(data_for_correlation)

print(f"Correlation between event_type and schedule_class: {correlation}")


# Close Spark session

sc.stop()

input("Press Enter ")
 