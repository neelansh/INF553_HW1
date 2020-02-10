import os
import sys
from pyspark import SparkContext, SparkConf
import json
import time

def count_partitions(iterator):
    yield sum([1 for i in iterator])

def main(input_file_path, output_file_path, NUMBER_OF_PARTITIONS):
    
    appName = 'assignment1'
    master = 'local[*]'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    output = {}
    jsonRDD = sc.textFile(input_file_path).map(json.loads).map(lambda x: (x['business_id'], 1))
    
    
    # default
    # jsonRDD = jsonRDD.repartition(jsonRDD.getNumPartitions())
    output['default'] = {}
    output['default']['n_partition'] = jsonRDD.getNumPartitions()
    output['default']['n_items'] = jsonRDD.mapPartitions(count_partitions).collect()
    start_time = time.time()
    jsonRDD.reduceByKey(lambda a, b: a+b).sortByKey(ascending=True).sortBy(lambda x: x[1], ascending=False).take(10)
    output['default']['exe_time'] = time.time() - start_time
    
    
    # customized
    jsonRDD = jsonRDD.partitionBy(NUMBER_OF_PARTITIONS, lambda k: hash(k))
    output['customized'] = {}
    output['customized']['n_partition'] = jsonRDD.getNumPartitions()
    output['customized']['n_items'] = jsonRDD.mapPartitions(count_partitions).collect()
    start_time = time.time()
    jsonRDD.reduceByKey(lambda a, b: a+b).sortByKey(ascending=True).sortBy(lambda x: x[1], ascending=False).take(10)
    output['customized']['exe_time'] = time.time() - start_time
    
    
    with open(output_file_path, 'wt') as f:
        f.write(json.dumps(output))
    return

if __name__ == '__main__': 
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))