# Created by :
# Team 10
# lmokada - Laxmikant Kishor Mokadam
# dgupta22 - Deepak Gupta
# rgchanda - Rohan Girish Chandavarkar 

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector
from pyspark.ml.feature import CountVectorizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import StorageLevel
import time
import atexit, copy, collections, itertools
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import arrays_zip

from conf import state


class TFIDF:
    def __init__(self):

        self.sparkSession = SparkSession\
            .builder\
            .appName("DocumentSearchEngine")\
            .getOrCreate()

        self.sc = self.sparkSession.sparkContext
        self.tf = None

        # create vocabulary
        vocab_rdd = self.sparkSession.sparkContext.wholeTextFiles("data/word_list.txt")
        wordlist = self.sparkSession.createDataFrame(vocab_rdd, ["text","data"])
        tokenizer = Tokenizer(inputCol="data", outputCol="words")
        wordsData = tokenizer.transform(wordlist)
        self.vocabModel = CountVectorizer(inputCol="words", outputCol="rawFeatures").fit(wordsData)
        self.word_to_id = dict()

        for id, word in enumerate(self.vocabModel.vocabulary):
            self.word_to_id[word]=id;

        self.id_to_path = dict()
        # destructor
        atexit.register(self.cleanup)

    def get_vocabulary(self):
        return self.vocabModel.vocabulary


    def get_tf_idf(self,stage_folder):
        """

        :param stage_folder_queue:
        :return: sqlobject of path and features
        """

        print(stage_folder)
        documents_rdd = self.sparkSession.sparkContext.wholeTextFiles(stage_folder+"/*")
        #documents_rdd.show(20, False)
        #state.logger.debug("documents : %s",[each for each in documents_rdd.collect()])

        documents = self.sparkSession.createDataFrame(documents_rdd,["path", "text"])
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        wordsData = tokenizer.transform(documents)
        #wordsData.select("path", "words").show(20,False)

        current_tf = self.vocabModel.transform(wordsData)
        #current_tf.show(20, False)


        if self.tf is not None:
            self.tf = self.tf.union(current_tf)
        else:
            self.tf = current_tf

        #self.tf.show(20, False)

        idf = IDF(inputCol="rawFeatures", outputCol="tfidf")
        idfModel = idf.fit(self.tf)


        self.tfidf = idfModel.transform(self.tf)
        #rescaledData.select("path", "features")

        ans = []
        state.logger.debug("TFIDF : %s", [each for each in self.tfidf.select("path", "tfidf").collect()])
        for each in self.tfidf.select("path", "tfidf").collect():
            ans.append(each)


        return ans


    def get_tf_idf_map(self,tfidf):

        # create mapping dict
        tf_idf_map= dict()
        file_to_id_map = dict()
        id_to_file_map = dict()
        index = 0
        for row in tfidf:
            doc = row.path
            if doc not in file_to_id_map:
                id_to_file_map[index] = doc
                file_to_id_map[doc] = index
                index+=1
            for word, score in zip(row.tfidf.indices,row.tfidf.values):
                if score > 0:
                    if word not in tf_idf_map:
                        tf_idf_map[word] = []
                    tf_idf_map[word].append((file_to_id_map[doc],score))

        for key in tf_idf_map:
            tf_idf_map[key].sort(key = lambda x : -x[1])

        return tf_idf_map, file_to_id_map ,id_to_file_map, self.vocabModel.vocabulary , self.word_to_id



    def cleanup(self):
        self.sparkSession.stop()




if __name__ == "__main__":
    t = TFIDF()
    b = t.get_tf_idf(["data/test_data/whole"])
    tf_idf_map, file_to_id_map, id_to_file_map, id_to_word, word_to_id = t.get_tf_idf_map(b)

    print(TFIDF.get_files_of_query(query=" man ",tf_idf_map = tf_idf_map, id_to_file_map=id_to_file_map,word_to_id=word_to_id))