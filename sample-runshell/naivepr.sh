# pg_dataset: input data path
# naive_pagerank_output: output path
# 2: number of workers
# 5: number of iterations

../../bin/hadoop jar ../../hadoop-i2mapreduce.jar naivepagerank pg_dataset naive_pagerank_output 2 5
