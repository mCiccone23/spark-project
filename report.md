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



