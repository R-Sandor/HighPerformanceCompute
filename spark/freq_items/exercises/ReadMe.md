# Algorithm Comparison 
Both algorithms will produce the same results the freq_item_pyspark.py has the advantage that it can handle
larger datasets and be parallelized and thus it scales better. However, it also requires that the developer
understand that there are potential side effects such as uneven buckets and that if the scan phase of the 
algorithm requires too much support that on the reduce phase that valid results may be excluded from the results. 
I would argue that market_basket.py algorithm is better for small datasets and when looking for sets that require 
high support in the small dataset. 

The second major advantage of the second algorithm over the first as it should also have a lower storage requirement as the 
data is being divided and sorted resulting in smaller foot print during execution time on the driver.
