import os
import sys
from pyspark import SparkContext, SparkConf
import json

NUMBER_OF_PARTITIONS= 16

def main(input_file_path, output_file_path):
    
    appName = 'assignment1'
    master = 'local[*]'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    output = {}
    jsonRDD = sc.textFile(input_file_path).map(json.loads).map(lambda x: {'date': x['date'], 'user_id': x['user_id'], 'business_id': x['business_id']}).repartition(NUMBER_OF_PARTITIONS).persist()
    #Task A
    output['n_review'] = jsonRDD.count()
    #task B
    output['n_review_2018'] = jsonRDD.filter(lambda x: x["date"].partition('-')[0].strip() == "2018").count()
    #Task C
    
    userRDD = jsonRDD.map(lambda x: (x['user_id'], 1)).reduceByKey(lambda a, b: a+b)
    
    output['n_user'] = userRDD.count()
    #Task D
    output['top10_user'] = userRDD.sortByKey(ascending=True).sortBy(lambda x: x[1], ascending=False).take(10)
    #Task E
    
    businessRDD = jsonRDD.map(lambda x: (x['business_id'], 1)).reduceByKey(lambda a, b: a+b)
    
    output['n_business'] = businessRDD.count()
    #Task F
    output['top10_business'] = businessRDD.sortByKey(ascending=True).sortBy(lambda x: x[1], ascending=False).take(10)
    
    with open(output_file_path, 'wt') as f:
        f.write(json.dumps(output))
    return

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
    