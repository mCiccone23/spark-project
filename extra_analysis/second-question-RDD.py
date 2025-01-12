from pyspark import SparkContext
import time

sc = SparkContext("local[1]", "Computational Power Lost")
sc.setLogLevel("ERROR")

wholeFile = sc.textFile("../part-00000-of-00001.csv.gz")
entries = wholeFile.map(lambda x: x.split(','))

start = time.time()

# Filter out entries with missing values
entries = entries.filter(lambda x: x[0] != '' and x[1] != '' and x[2] != '' and x[4] != '')


entries = entries.map(lambda x: (x[1], x))
entries_grouped = entries.groupByKey().mapValues(list)
entries_sorted = entries_grouped.mapValues(lambda rows: sorted(rows, key=lambda r: float(r[0])))
# Extend each row with the next row's start time and end time
def extend_with_next(rows):
    extended = []
    for i in range(len(rows) - 1):
        row = rows[i]
        next_row = rows[i + 1]
        extended.append(row + [next_row[0], next_row[2]])
    return extended


# Extend each entry with additional information from the next entry in the sequence,
# then extract the relevant values (using map to only keep the value part of the key-value pair).
entries_extended = entries_sorted.flatMapValues(extend_with_next).map(lambda x: x[1])


# third element (x[2]) is '1' (indicating a maintenance or downtime start).
# last element (x[-1]) is '0' (indicating no ongoing maintenance).
filtered_entries = entries_extended.filter(lambda x: x[2] == '1' and x[-1] == '0')

# Add a new field to each filtered entry that calculates the downtime duration as:
# (end_time - start_time), using the second-to-last field (x[-2]) as the end time
# and the first field (x[0]) as the start time.
downtime_entries = filtered_entries.map(lambda x: x + [float(x[-2]) - float(x[0])])

# Calculate the downtime weighted by capacity (e.g., lost computational resources).
downtime_weighted = downtime_entries.map(lambda x: float(x[-1]) * float(x[4]))

# Sum up all the weighted downtimes to get the total lost capacity due to maintenance.
total_lost_capacity = downtime_weighted.sum()

# Calculate the total available capacity during all time intervals.
total_available_capacity = entries_extended.filter(
    lambda x: x[0] != '' and x[-2] != '' and x[4] != ''
).map(
    lambda x: (float(x[-2]) - float(x[0])) * float(x[4])
).sum()

# Calculate the percentage of capacity lost due to maintenance as:
# (total_lost_capacity / total_available_capacity) * 100.
percentage_lost = (total_lost_capacity / total_available_capacity) * 100


print("Execution time: ", time.time() - start)
print(f"Total lost capacity: {total_lost_capacity}")
print(f"Total available capacity: {total_available_capacity}")
print(f"Percentage of computation lost: {percentage_lost:.2f}%")

input("Press Enter to exit")
sc.stop()
