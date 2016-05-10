# update_pagerank_output/update: the whole updated data path
# update_pagerank_output/delta: delta input
# iter_pagerank_output/preserve/convergeState: the preserved final result
# iter_pagerank_output/preserve: the preserved intermediate result
# incr_pagerank_output: the updated result
# -p: number of partitions
# -t: termination threshold for change propagation control
# -I: number of max iterations
# -c: cache type (0,1,2,3,4,5,6)
# -i: checkpoint interval for fault tolerance
# -T: incr support type

../../bin/hadoop jar ../../hadoop-examples-1.0.2.jar incrpagerank update_pagerank_output/update update_pagerank_output/delta iter_pagerank_output/preserve/convergeState iter_pagerank_output/preserve incr_pagerank_output -p 4 -t 0.1 -I 5 -c 6 -i 10 -T 2
