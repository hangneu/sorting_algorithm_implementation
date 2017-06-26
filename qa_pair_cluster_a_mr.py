# Run example:
# spark-submit --master yarn-rm --num-executors=100 
# --conf "spark.kryoserializer.buffer.max = 128M" 
# --executor-memory 8g --executor-cores 5 
# --driver-memory 8g qa_pair_cluster_a_mr.py  

from nltk.stem.porter import PorterStemmer
from nltk import data,FreqDist
from nltk.corpus import stopwords
import re
import numpy as np
import pandas as pd
import unicodedata
import sys
import random
import string
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeansModel, KMeans
from pyspark.mllib.feature import Word2Vec, Word2VecModel
from scipy.spatial import distance

minChar = 10
maxLen = 96
# input_path = "/user/hive/warehouse/aiva.db/xiao_chat3_small"
input_path = "/user/a601705/project_first/text_6.txt"
output_path = "/user/hive/warehouse/aiva.db/xiao_chat3_qa_cluster_a"

sc = SparkContext(appName="qa_pair_debug")
data_raw = sc.textFile(input_path)

max_iter = 100
cachedStopWords = set(stopwords.words("english"))
tbl = dict.fromkeys(i for i in xrange(sys.maxunicode) if unicodedata.category(unichr(i)).startswith('P'))
minFreq = 5
vector_length = 100
kmeans_cluster = 1000
# kmeans_cluster = 5
method = "long"
sep_sign = '\x01'

def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))
    return stemmed

def SymbolFilter(x):
    x = re.sub('\\+', '  ', x)
    x = re.sub('\\-','  ',x)
    x = re.sub('=','  ',x)
    x = re.sub('>','  ',x)
    x = re.sub('<','  ',x)
    x = re.sub('\$', ' ', x)
    x = re.sub('\[name\]',' ', x)
    x = x.translate(tbl)
    x = x.lower()
    x = re.sub("\s\s+" , " ", x)
    x = x.strip()
    x = ' '.join([i for i in x.split(' ') if i not in cachedStopWords])
    # add noise spelling noise
    return x.strip()

def tokenize(x):
    text = SymbolFilter(x)
    tokens = text.split(' ')
    stems = stem_tokens(tokens, PorterStemmer())
    return ' '.join(stems)

def text_preproc(x):
    out_x = x[0:2] + map(tokenize, x[2:])
    return out_x

def seg_session(x):
	x_sep = x.split(sep_sign)
	k = x_sep[0]
	v = [x_sep[1], x_sep[2], x_sep[3]]
	return (k,v)

def qa_pair_prep(x):
	df_raw = pd.DataFrame(list(x[1]))
	df_raw[0] = df_raw[0].astype(int)
	df_raw = df_raw.sort(0).reset_index(drop=True)
	if(len(set(df_raw.loc[:,1]))==1 or (len(set(df_raw.loc[:,1]))==2 and ("CLIENT" not in set(df_raw.loc[:,1])))):
		return []
	k = x[0]
	answer = ""
	times = 0
	question = ""
	qa_pairs = []
	sid = x[0]
	start_index = np.where(df_raw.loc[:,1]=="CLIENT")[0][0]
	finish_index = np.where(df_raw.loc[:,1] != "CLIENT")[0][-1]+1
	if(start_index!=0):
		for item in df_raw.loc[range(start_index),2]:
			question += item+" "
	if(finish_index<= start_index):
		return []
	for item in np.array(df_raw.loc[range(start_index,finish_index),:]):
		if(pd.notnull(item[2])):
			if(item[1]=="CLIENT"):
				if(answer!=""):
					qa_pairs.append([sid,times,question,answer])
					question,times = question + " " + answer,times+1
				question += " " + item[2]
				answer = ""
			else:
				answer += item[2]
	qa_pairs.append([sid,times,question,answer])
	return qa_pairs

raw_data_id = data_raw.map(seg_session).groupByKey()
qa_pair_id = raw_data_id.map(qa_pair_prep)
qa_pair = qa_pair_id.flatMap(lambda x: x).filter(lambda x: len(x)>0 and x is not None)

qa_pair_clean = qa_pair.map(text_preproc).filter(lambda x: (len(x[2]) >minChar) & (len(x[3]) >minChar))
qa_pair1 = qa_pair_clean.filter(lambda x: ( len(x[2].split(" "))<=maxLen) & ( len(x[3].split(" "))<=maxLen))
word2vec_train_set_with_index = qa_pair1.map(lambda x: [str(x[0])+"-"+str(x[1]),x[3].split(" ")])
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
model_kmeans = KMeans.train(word2vec_vector_kmeans_training_data, kmeans_cluster, maxIterations = max_iter, initializationMode="random")

cluster_center = model_kmeans.clusterCenters
question_cluster_info = word2vec_vector_with_index.map(lambda x: (x[0], model_kmeans.predict(x[1]), x[1]))
question_cluster_info = question_cluster_info.map(lambda x: (x[0] ,[x[1], distance.euclidean(x[2], cluster_center[x[1]])]))

qa_with_index = qa_pair.map(lambda x: [str(x[0])+"-"+str(x[1]), [x[2], x[3]]])
qa_with_index = qa_with_index.join(question_cluster_info).map(lambda x:[x[0].split("-")[0], x[0].split("-")[1], x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]])

def sort_ind(x):
	ss_ind = x[0]
	df_raw = pd.DataFrame(list(x[1])).sort(0).reset_index(drop=True)
	df_raw.loc[:,0] = range(df_raw.shape[0])
	df_list = np.array(df_raw).tolist()
	return map(lambda x: [ss_ind] + x, df_list)

qa_pair_sort = qa_with_index.map(lambda x: (x[0],x[1:])).groupByKey().map(sort_ind).flatMap(lambda x:x)
print qa_pair_sort.count()
# qa_pair_sort.map(lambda x: sep_sign.join([ str(y.encode("utf-8")) if isinstance(y, basestring) else str(y) for y in x])).saveAsTextFile(output_path)





