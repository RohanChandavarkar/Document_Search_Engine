import unittest
import tfidf


def tfidf_calculation_multistage(path_list_1, path_list_2):
    t = tfidf.TFIDF()
    for folder in path_list_1:
        a = t.get_tf_idf(folder)
    t = tfidf.TFIDF()
    for folder in path_list_2:
        b = t.get_tf_idf(folder)
    a_features = [t.tfidf for t in a]
    b_features = [t.tfidf for t in b]
    print(a_features)
    assert (a_features == b_features)

def basic_tfidf_obj(path):
    t = tfidf.TFIDF()
    b = t.get_tf_idf(path)
    print(b[0].tfidf)


def test_local_TFIDF():
    basic_tfidf_obj("data/test_data/stage1")

def test_local_tfidf_calculation_multistage():
    path_list_1 = ["data/test_data/stage1", "data/test_data/stage2"]
    path_list_2 = ["data/test_data/whole"]
    tfidf_calculation_multistage(path_list_1,path_list_2)






def test_hdfs_tfidf_calculation_multistage():
    path_list_1 = ["hdfs://152.7.99.46:9000/searchData/test_data/stage1",
                   "hdfs://152.7.99.46:9000/searchData/test_data/stage2"]
    path_list_2 = ["hdfs://152.7.99.46:9000/searchData/test_data/whole"]
    tfidf_calculation_multistage(path_list_1, path_list_2)


def test_hdfs_TFIDF():
    basic_tfidf_obj("hdfs://152.7.99.46:9000/searchData/test_data/stage1")
