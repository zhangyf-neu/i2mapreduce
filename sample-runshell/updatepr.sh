# iter_pagerank_output/substatic: input structure data path
# update_pagerank_output/update: updated structure data path
# update_pagerank_output/delta: random generated delta input path
# 4: number of partitions
# 0.01: updated percent
# false: no deleted links
# 10000000: total number of nodes

../../bin/hadoop jar ../../hadoop-examples-1.0.2.jar genprupdate iter_pagerank_output/substatic update_pagerank_output/update update_pagerank_output/delta 4 0.01 false 1000000
