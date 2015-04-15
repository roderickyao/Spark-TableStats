# Spark Table Stats
Spark Table Stats is intended to provide summary statistics by column in an efficient manner. As designed now the intent is to generate the following statistics in only two passes by making use of repartitionAndSortWithinPartitions leveraging custom partitioning and foreachPartition leveraging custom accumulators.

# Summary Statistics By Column:
 - Sum
 - Avgerage
 - Standard Deviation
 - Max
 - Min
 - Carnality (The number of records of frequence / total records)
 - Count Nulls
 - Count Empties
 - Top (K) Values by Frequence (NOT COMPLETED)

# TODO:
 - Top(K) - Evaluate oppertunity to use combineByKey and create an empty min queue for each key. Merge values into the queue if its size is < K. If >= K, only merge the value if it exceeds the smallest element; if so add it and remove the smallest element. 

# Collaborators:
   Eric, Roderick, Brad
