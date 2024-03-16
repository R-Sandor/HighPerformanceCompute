# Streaming with Hadoop. 
To run the application I have intended for two passes as is SON algorithm. The first pass to get the pairs the meet the cateria, 
then the second pass then take that input get the sets that meet 12 or more baskets with that pair. 

## Commands 
``` sh
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar   -input /market/input   -output /market/output_part1   -mapper "python3 ./MAP_1.py"   -reducer "python3 ./REDUCE_1.py"

hadoop fs -get /market/output_part1 && cp output_part1/part-00000 .

/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar   -input /market/input   -output /market/output_part1   -mapper "python3 ./MAP_2.py"   -reducer "python3 ./REDUCE_2.py"

hadoop fs -get /market/output_part2
```

