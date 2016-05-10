# pg_dataset: input path
# iter_pagerank_output: output_path
# -p: number of partitions
# -I: number of max iterations
# -s: true or false, whether to save the intermediate results for further usage
# -f: the input data format

../../bin/hadoop jar ../../hadoop-examples-1.0.2.jar iterpagerank pg_dataset iter_pagerank_output -p 4 -I 5 -s true -f TextInputFormat
