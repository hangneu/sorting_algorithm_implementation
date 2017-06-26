# spark-submit --master yarn-rm --num-executors=10 
# --conf "spark.kryoserializer.buffer.max = 128M" 
# --executor-memory 8g 
# --executor-cores 5 
# --driver-memory 8g xxxxxxxxxxxx.py 
import numpy as np
import pandas as pd
import sys
import random
import string
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeansModel, KMeans
from pyspark.mllib.feature import Word2Vec, Word2VecModel
from scipy.spatial import distance
from math import sqrt

sc = SparkContext(appName="qa_pair_debug")

# data_raw1 = sc.textFile("/user/hive/warehouse/aiva.db/chats_all_clean_relaxed_qa_stage")
# data_raw = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat2_qa_clean")
# aiva.xiao_chat_track_1b_qa_clean

data_raw = sc.textFile("/user/a601705/project_first/text_6.txt")



# sep_sign = '|'
# input_path = "/user/a601705/project_first/word2vec_sample_1000.csv"
max_iter = 100
minFreq = 5
vector_length = 100
sep_sign = '\x01'

# qa_pair = sc.textFile(input_path).map(lambda x: x.split(sep_sign))
# "/user/hive/warehouse/aiva.db/xiao_chat2_qa_clean"
# qa_pair = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat2_qa_clean").map(lambda x: x.split(sep_sign))
# qa_pair = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat_track_1b_qa_clean").map(lambda x: x.split(sep_sign))
qa_pair = sc.textFile("/user/a601705/project_first/text_6.txt").map(lambda x: x.split(sep_sign))

word2vec_train_set_with_index = qa_pair.map(lambda x: [str(x[0])+"-"+str(x[1]),x[3].split(" ")])
print word2vec_train_set_with_index.take(2)

word2vec_training_dataset = word2vec_train_set_with_index.map(lambda x: x[1])

word_list = word2vec_training_dataset.map(lambda x:([(item,1) for item in x])).flatMap(lambda x:x).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>minFreq).map(lambda x:x[0]).collect()
print "Obtained word vocab"

def word_frequency_process(x):
	tem = []
	for item in x[1]:
		if item in word_list:
			tem.append(item)
		else:
			tem.append("unknown")
	return [x[0], tem]

word2vec_train_set_with_index = word2vec_train_set_with_index.map(word_frequency_process)
word2vec_training_dataset = word2vec_train_set_with_index.map(lambda x: x[1])
word2vec_train_set_with_index = word2vec_train_set_with_index.map(lambda x: [(item,x[0]) for item in x[1]]).flatMap(lambda x:x)
print "Start word2vec training ...."
word2vec_model = Word2Vec().setVectorSize(vector_length).setSeed(42).fit(word2vec_training_dataset)
print "Finished word2vec training ...."

word_list = word2vec_training_dataset.map(lambda x:([(item,1) for item in x])).flatMap(lambda x:x).reduceByKey(lambda x,y:x+y).map(lambda x:x[0]).collect()
print "Obtained word vocab"
word_list_vector_dic = []
for item in word_list:
	word_list_vector_dic.append([item,word2vec_model.transform(item)])

word_list_vector_dic = sc.parallelize(word_list_vector_dic).map(lambda x:(x[0], x[1]))
print "Obtained word local embedding ...."
word2vec_vector_with_index = word2vec_train_set_with_index.leftOuterJoin(word_list_vector_dic).map(lambda x:(x[1][0], x[1][1])).groupByKey()

def calculate_vector(x):
	vector = list(x[1])
	result = np.zeros(vector_length)
	for item in vector:
		result += item
	return [x[0],result/len(vector)]

word2vec_vector_with_index = word2vec_vector_with_index.map(calculate_vector)
print "Obtained document feature vector ...."

word2vec_vector_kmeans_training_data = word2vec_vector_with_index.map(lambda x:x[1])

kmeans_clusters = [5,10]
# kmeans_clusters = [10, 20, 50, 100, 200, 300, 400, 500, 600, 700 ,800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000]
result = []
for kmeans_cluster in kmeans_clusters:
	model_kmeans = KMeans.train(word2vec_vector_kmeans_training_data, kmeans_cluster, maxIterations = max_iter, initializationMode="random")

	def error(point):
		center = model_kmeans.centers[model_kmeans.predict(point)]
		return sqrt(sum([x**2 for x in (point - center)]))

	WSSSE = word2vec_vector_kmeans_training_data.map(lambda point: error(point)).reduce(lambda x, y: x + y)
	# model_kmeans = KMeans.train(word2vec_vector_kmeans_training_data, 20, maxIterations = max_iter, initializationMode="random")
	result.append([kmeans_cluster, WSSSE])
print result
file = open("data_error2.csv", "w")
file.write("cluster_number,error\n")
for item in result:
	file.write(str(item[0])+","+str(item[1])+"\n")
file.close()
# question_cluster_info = word2vec_vector_with_index.map(lambda x: (x[0], model_kmeans.predict(x[1])))
# qa_with_index = qa_pair.map(lambda x: [str(x[0])+"-"+str(x[1]), [x[2], x[3]]])
# qa_with_index = qa_with_index.leftOuterJoin(question_cluster_info).map(lambda x:[x[0].split("-")[0], x[0].split("-")[1], x[1][0][0], x[1][0][1], x[1][1]])
