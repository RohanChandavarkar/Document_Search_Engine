import unittest
import tfidf, glob,os


def tfidf_obj_creation_perf(dataset_path):
    t = tfidf.TFIDF()
    dirs = [dataset_path]
    for dir in dirs:
        ans = t.get_tf_idf(dir)
    with open("/tmp/result.txt",'w') as f:
        f.write(str(ans))

def test_local_TFIDF_perf():
    path = "data/dataset/bbc/total"

    tfidf_obj_creation_perf(path)

def test_hdfs_TFIDF_perf():
    path = "hdfs://152.7.99.46:9000/searchData/dataset/bbc/total"
    tfidf_obj_creation_perf(path)

