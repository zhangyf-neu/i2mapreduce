# i2mapreduce
incremental MapReduce for mining evolving big data

We extended iMapReduce and developed i2MapReduce, which supports fine-grain incremental processing (kv-pair level rather than task level in Incoop), general-purpose iterative processing (that extends the original iMapReduce and supports more iterative algorithms), and incremental processing for iterative computation.

For more infomation, please refer to our i2MapReduce paper in TKDE 2015.

* Yanfeng Zhang, Shimin Chen, Qiang Wang, Ge Yu. i2MapReduce: Incremental MapReduce for Mining Evolving Big Data [J]. IEEE Transactions on Knowledge and Data Engineering (TKDE), 27(7), July, 2015, pp. 1906-1919. 

This project is a prototype implementation of the iMapReduce idea. The prototype is based on Hadoop 1.0.3. It is better used for research perspective, but we don't recommend to use it in production. Of course, we welcome any feedback on i2MapReduce.
