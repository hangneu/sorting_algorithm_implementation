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
max_iter = 100
minFreq = 5
vector_length = 100
sep_sign = '\x01'

sc = SparkContext(appName="qa_pair_debug")

# data_raw1 = sc.textFile("/user/hive/warehouse/aiva.db/chats_all_clean_relaxed_qa_stage")
# data_raw = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat2_qa_clean")
# data_raw = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat2_qa_clean_small")
# xiao_chat2_qa_clean_small

# data_raw = sc.textFile("/user/a601705/project_first/text_5.csv")
# data_raw = sc.textFile("/user/a601705/project_first/text_6.txt")

# qa_pair = sc.textFile(input_path).map(lambda x: x.split(sep_sign))
# "/user/hive/warehouse/aiva.db/xiao_chat2_qa_clean"
# qa_pair = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat_track_1b_qa_clean").map(lambda x: x.split(sep_sign))
# qa_pair = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat_track_1b_qa_clean").map(lambda x: x.split(sep_sign))
qa_pair = sc.textFile("/user/a601705/project_first/text_6.txt").map(lambda x: x.split(sep_sign))
# aiva.xiao_chat_track_1b_qa_clean

# qa_pair = sc.textFile("/user/hive/warehouse/aiva.db/xiao_chat2_qa_clean_small").map(lambda x: x.split(sep_sign))
# qa_pair = sc.textFile("/user/a601705/project_first/vocabulary_1000.txt").map(lambda x: x.split(sep_sign))
word2vec_train_set_with_index = qa_pair.map(lambda x: [[str(x[0])+"-"+str(x[1]),x[2].split(" ")],[str(x[0])+"-"+str(x[1]),x[3].split(" ")]]).flatMap(lambda x:x)
word2vec_training_dataset = word2vec_train_set_with_index.map(lambda x: x[1])
word_list = word2vec_training_dataset.map(lambda x:([(item,1) for item in x])).flatMap(lambda x:x).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=minFreq).map(lambda x:x[0]).collect()
# word_list = sc.textFile("/user/a601705/project_first/vocabulary.txt").collect()
print "Obtained word vocab"

def word_frequency_process(x):
	tem = []
	for item in x[1]:
		if item in word_list:
			tem.append(item)
		else:
			tem.append("UNK")
	return [x[0], tem]

word2vec_train_set_with_index = word2vec_train_set_with_index.map(word_frequency_process)
word2vec_training_dataset = word2vec_train_set_with_index.map(lambda x: x[1])
word2vec_train_set_with_index = word2vec_train_set_with_index.map(lambda x: [(item,x[0]) for item in x[1]]).flatMap(lambda x:x)
print "Start word2vec training ...."
word2vec_model = Word2Vec().setVectorSize(vector_length).setSeed(42).fit(word2vec_training_dataset)
print "Finished word2vec training ...."

word_list1 = word2vec_training_dataset.map(lambda x:([(item,1) for item in x])).flatMap(lambda x:x).reduceByKey(lambda x,y:x+y).map(lambda x:x[0]).collect()
print "Obtained word vocab"
file = open("big_dataset_voca.txt","w")
file1 = open("big_dataset_voca_dic.txt",'w')
for item in word_list1:
	file.write(item+"\n")
	string = item + " "
	for item1 in word2vec_model.transform(item):
		string += str(item1) + " "
	string = string[:-1]
	file1.write(string + "\n")
file.close()
file1.close()
