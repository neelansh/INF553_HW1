import os
import sys
from pyspark import SparkContext, SparkConf
import json
import time
import csv

NUMBER_OF_PARTITIONS= 16

def main(review_file_path, business_file_path, output_a_path, output_b_path):
	appName = 'assignment1'
	master = 'local[*]'
	conf = SparkConf().setAppName(appName).setMaster(master)
	sc = SparkContext(conf=conf)
	sc.setLogLevel("ERROR")
	output = {}
	reviewRDD = sc.textFile(review_file_path).map(json.loads).map(lambda x: (x['business_id'], x['stars'])).repartition(NUMBER_OF_PARTITIONS)
	businessRDD = sc.textFile(business_file_path).map(json.loads).map(lambda x: (x['business_id'], x['city'])).repartition(NUMBER_OF_PARTITIONS)
	
	averageStarsRDD = reviewRDD.join(businessRDD).map(lambda x: (x[1][1], (x[1][0], 1))).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).persist()
	
	start_time = time.time()
	city_rating = averageStarsRDD.collect()
	city_rating = sorted(city_rating, reverse=True, key=lambda x: (x[1], x[0]))
	m1_time = time.time()-start_time
	
	start_time = time.time()
	sorted_city_rating = averageStarsRDD.sortBy(lambda x: x[1], ascending=False).take(10)
	m2_time = time.time()-start_time
	
	with open(output_a_path, 'wt') as f:
		csv_writer = csv.writer(f, delimiter=',')
		csv_writer.writerow(['city', 'stars'])
		for c, s in city_rating:
			csv_writer.writerow([c, s])
	
	with open(output_b_path, 'wt') as f:
		f.write(json.dumps({'m1': m1_time, 'm2': m2_time}))
	return

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])