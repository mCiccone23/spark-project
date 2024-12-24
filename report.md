# Report Spark Project
Alessandra Cicciarelli, Matteo Ciccone

## 1. Analyses conducted

#### What is the distribution of the machines according to their CPU capacity?
For the first question, we analyzed the machine_events file, specifically focusing on the CPU field, which represents the CPU capacity of the machine.
To conduct this analysis, we first mapped each capacity to the value 1 and then summed the occurrences for each capacity. All the computation are done with RDD. Below are the results.

##### CPU Capacity:
CPU Capacity 0.5: 35015 machines\
CPU Capacity 0.25: 510 machines\
CPU Capacity 1: 2223 machines

![distribution plot](./images/CPU%20distribution.png)

#### What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
For the second question, we used the same file, machine_events. Specifically, we analyzed cases where the event type = 1, indicating that the machine was removed from the cluster.
First, we applied a window function to order the data by timestamp, partitioned by machine id.
Next, we added two new columns to the DataFrame: the next timestamp and the next event type, allowing us to filter rows where an event type = 1 was followed by an event type = 0 (indicating downtime).
By calculating the difference between these timestamps, we determined the downtime, which we then weighted by the CPU capacity.
Finally, to compute the percentage relative to the total capacity, we divided the total lost capacity by the potential capacity. Below are the result.

| Metric                      | Value                |
|-----------------------------|----------------------|
| **Total lost capacity**     | 65,292,876,776,135.75 |
| **Total available capacity**| 3,517,654,828,229,501.5 |
| **Percentage of computation lost** | 1.86%             |


#### What is the distribution of the number of jobs/tasks per scheduling class?
#### Do tasks with a low scheduling class have a higher probability of being evicted?
#### In general, do tasks from the same job run on the same machine?
#### Are the tasks that request the more resources the one that consume the more resources?
#### Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?


### Fourth question:
Do tasks with a low scheduling class have a higher probability of being evicted? 

    •  Map (scheduling_class, (event_type, 1))
    •  Totale eventi per scheduling class
    •  Totale evicted (event_type == '2') per scheduling class 
    •  combino i totali per scheduling class (schedule,    (total_per_class, total_evicted_per_class))
    •  tasso di eviction
    •  grafici con tasso
    •  calcolo e stampa correlazione

### Fifth question:
In general, do tasks from the same job run on the same machine?

    •  filter scheduling == 1 [SCHEDULED], map (job,machine)
    •  raggruppo per job e conto il numero di machine diverse per job (diverse perchè sto usando un set)
    •  conto quanti jobs sono runnati su una sola macchina
    •  conto i jobs totali
    •  rapporto
    •  stampo la distribuzione dei task dei jobs per il numero macchine



