# Report Spark Project
Alessandra Cicciarelli, Matteo Ciccone

## 1. Analyses conducted

#### What is the distribution of the machines according to their CPU capacity?
For the first question we have to analyze the machine_events file and specificly the field CPU that explain the CPU capacity of the machine.
To manage this analysis we first did a map with capacity and 1 and then we sum the quantity by capacity. In the following there are the results.

##### CPUCapacity:
CPU Capacity 0.5: 35015 machines\
CPU Capacity 0.25: 510 machines\
CPU Capacity 1: 2223 machines

![distribution plot](./images/CPU%20distribution.png)

#### What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?


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



