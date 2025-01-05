# Report Spark Project
Alessandra Cicciarelli, Matteo Ciccone

## 1. Analyses conducted

### 1.1 What is the distribution of the machines according to their CPU capacity?

For this question, we analyzed the `machine_events` file, focusing on the **CPU** field, which represents the CPU capacity of each machine. 

#### Methodology:
- Mapped each capacity value to `1` and summed the occurrences for each capacity using RDD transformations.
- Computed the distribution of machines by CPU capacity.

#### Results:
| **CPU Capacity** | **Number of Machines** |
|------------------|-------------------------|
| 0.5              | 35,015                 |
| 0.25             | 510                    |
| 1                | 2,223                  |

![CPU Distribution Plot](./images/CPU%20distribution.png)

---

### 1.2 What is the percentage of computational power lost due to maintenance?

For this question, we again used the `machine_events` file, analyzing cases where `event_type = 1` (indicating that a machine was removed from the cluster).

#### Methodology:
1. **Data Preparation**:  
   - Used a window function to order data by `timestamp`, partitioned by `machine_id`.
   - Added two new columns to the DataFrame: the **next timestamp** and the **next event type**.
2. **Downtime Calculation**:  
   - Filtered rows where `event_type = 1` was followed by `event_type = 0` (indicating downtime).
   - Computed downtime as the difference between these timestamps.
   - Weighted downtime by CPU capacity.
3. **Percentage Calculation**:  
   - Divided the total lost computational power by the total available capacity to calculate the percentage of computational power lost.

#### Results:
| Metric                           | Value                      |
|----------------------------------|----------------------------|
| **Total Lost Capacity**          | 65,292,876,776,135.75      |
| **Total Available Capacity**     | 3,517,654,828,229,501.5    |
| **Percentage of Computation Lost** | 1.86%                     |

---

### 1.3 What is the distribution of the number of jobs/tasks per scheduling class?

This question focuses on the `job_events` and `task_events` tables, specifically analyzing the **schedule_class** field.

#### Methodology:
1. **Data Loading**:  
   - Read both `job_events` and `task_events` files.
2. **Mapping and Aggregation**:  
   - Mapped both datasets with `(schedule_class, 1)`.
   - Aggregated values using `reduceByKey()`.
3. **Analysis**:  
   - Generated individual plots for jobs and tasks.
   - Performed a `join()` operation to combine the analysis for jobs and tasks.
   - Computed the sum of values.
   - Created a final plot to display the distribution of tasks and jobs per class.

#### Results:
| Metric                | Distribution                                  |
|-----------------------|-----------------------------------------------|
| **Jobs Distribution** | ('3', 1,885), ('2', 3,030), ('1', 3,610), ('0', 2,179) |
| **Tasks Distribution**| ('3', 56,586), ('2', 97,482), ('1', 58,109), ('0', 237,969) |
| **Jobs/Tasks Combined**| ('3', 58,471), ('2', 100,512), ('1', 61,719), ('0', 240,148) |

#### Visualizations:
- **Jobs Distribution**  
  ![Jobs Distribution](./images/jobs_distribution.png)  
  *The most used class for jobs is **1**.*

- **Tasks Distribution**  
  ![Tasks Distribution](./images/tasks_distribution.png)  
  *The most used class for tasks is **0**.*

- **Jobs and Tasks Combined Distribution**  
  ![Jobs and Tasks Distribution](./images/Jobs_tasks_distribution.png)  
  *When combined, the most used class remains **0**.*
---

### 1.4 Do tasks with a low scheduling class have a higher probability of being evicted?

This question focuses on the `task events` table, in particular on the **schedule_class** and **event_type** fields.

#### Methodology:
1. **Data Loading**:
  - Read the `task_event` file.
2. **Mapping and Aggregation**:
  - Mapped the entries with `(scheduling_class, (event_type, 1))` obtaining the `task_per_schedule` RDD.
  - Aggregated values per scheduling_class using `reduceByKey()`.
  - Filtered the `task_per_schedule` RDD to obtain an RDD with the evicted tasks.
  - Aggregated values per per scheduling_class using `reduceByKey()` for the only evicted tasks.
3. **Analysis**:
  - Performed a `join()` for scheduling_class to combine the total events per class and the total evicted events per class.
  - Computed the `eviction rate` per scheduling_class
  - Plotted the comparison between `eviction rate` and  `scheduling class`
  - Computed and printed the correlation between the `event type` and  the`scheduling class`

#### Results:
| Scheduling Class            | Eviction Rate                      |
|----------------------------------|----------------------------|
| **3**    |   0.04%  |
| **2**    |   0.68%  |
| **1**    |   1.36%  |
| **0**    |   1.20%  |
- **Eviction Rate for different Scheduling Class**  
  ![Eviction Rate by scheduling_class](./images/eviction_rate_by_scheduling_class.png)  
  *The most evicted tasks are that with lower scheduling class: 0 and 1*

Another analysis done is the computation of the correlation between event_type and schedule_class: -0.19884144277296448. 

---
#### 1.5 In general, do tasks from the same job run on the same machine?

    •  filter scheduling == 1 [SCHEDULED], map (job,machine)
    •  raggruppo per job e conto il numero di machine diverse per job (diverse perchè sto usando un set)
    •  conto quanti jobs sono runnati su una sola macchina
    •  conto i jobs totali
    •  rapporto
    •  stampo la distribuzione dei task dei jobs per il numero macchine
#### 1.6 Are the tasks that request the more resources the one that consume the more resources?
To address this question, we analyzed the `task_usage` and `task_events` tables. The sequence of transformations and actions performed was as follows:

1. **Mapping**: Extracted CPU and memory used/requested values, grouped by `jobid` and `task_index`.  
   Format: `[(jobid, task), (CPU, MEM)]`
2. **Aggregation**: Computed the average CPU and memory values using `reduceByKey()`.
3. **Joining**: Combined the tables to associate CPU and memory usage/request data.
4. **Cleaning and Formatting**: Processed the data to standardize and map it for analysis.
5. **Correlation Analysis**: Calculated the correlation between requested and used resources.
6. **Visualization**: Created scatter plots to visualize the relationship between resource requests and actual usage.

##### Results

##### Scatter Plots

- **CPU Request vs CPU Used**  
  ![Scatter Plot - CPU Request vs CPU Used](./images/Scatter_plot_CPU_Request_CPU_used.png)

- **Memory Requested vs Memory Used**  
  ![Scatter Plot - Memory Requested vs Memory Used](./images/Scatter_plot_memory_requested_memory_used.png)

##### Correlation Metrics

| Metric                | Value                |
|-----------------------|----------------------|
| **CPU Correlation**   | 0.4469849108504573  |
| **Memory Correlation**| 0.5678103841836577  |

#### Analysis

From the scatter plots and the correlation values, we observe a moderate positive correlation between the requested and used resources for both CPU and memory. This indicates that while resource requests partially reflect actual usage, there is potential for optimization in resource allocation.

#### 1.7 Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
    • Filtra task evicted e mappa in (machine_id, time)
    • Mappa resource usage in (machine_id, (start_time, end_time, (max_mem, max_cpu, max_disc))) --> usato safe_float per rimuovere valori nulli
    • join su machine_id
    • Filtro su time compreso tra start_time e end_time
    • Mappa per ciascuna risorsa (max_mem, max_cpu, max_disc)
    • Aggrega per ciascuna risorsa e calcola la correlazione per ciascuna risorsa
    • stampa e visualizzazione dei risultati
Computazione lenta in questo modo --> ottimizzare 




# 2. Performance Evaluation and improvements

### 2.1 First analysis evaluation

#### Studies at the application level:

Trying to add caching on the most used RDD the result are the same this is due to the fact that few operation are executed on the RDD and the effect of caching are minimum.

```python
distrinctEntries = entries.map(lambda x: (x[1], x[cpu_capacity_index])).distinct().cache()

start = time.time()
# map(capacity, 1) then we aggregate the number by key
cpu_distribution = (
    distrinctEntries.map(lambda x: (x[1], 1))
            .reduceByKey(lambda a, b: a + b)
        .collect()
)

print("Execution time: ", time.time() - start)
```
#### RDD vs DataFrame

##### **Execution Times**
- **RDD Execution Time:** `2.11 seconds`
- **DataFrame Execution Time:** `0.12 seconds`


##### **RDD DAG**
![RDDPipeline](./images/pipelineEs1.png)

  - The DAG shows multiple stages with intermediate shuffles due to transformations like `distinct` and `reduceByKey`.

##### **DataFrame DAG**
![DataFrame Pipeline](./images/jobsEs1DataFrame.png)
  - The DAG is simpler with fewer stages, as Spark optimized the entire query plan.

##### **Conclusion**
DataFrames demostrate better performance due to Spark's optimizations and they provide a more concise and declarative way for working with data.

#### Other Improvements

To further improve the performance of DataFrame operations, the following changes was implemented:

   - Repartitioned the DataFrame based on the `cpu_capacity` column to reduce shuffle and optimize parallelism
   - Filtered out null values from the `cpu_capacity` column early in the pipeline to avoid processing unnecessary data.
   - Enabled Adaptive Query Execution (AQE) to allow Spark to dynamically optimize the query plan.
   
![stagesDataFrame1](./images/stagesDataFrame1.png)

