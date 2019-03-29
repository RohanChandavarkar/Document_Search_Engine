
import queue, pickle
from os import listdir
from os.path import isfile, join
import tfidf, shutil, time, os, timeit
import socket,multiprocessing, threading
import subprocess, os, sys
from argparse import ArgumentParser


class Service:

    def __init__(self,ftp_folder,data_dir,tfidf_storage_dir, interval):

        self.queue = multiprocessing.SimpleQueue()
        self.ftp_folder = ftp_folder
        self.data_dir = data_dir
        self.tfidf_storage_dir = tfidf_storage_dir
        self.interval = interval

        #if not os.path.exists(self.data_dir ):
        #    os.makedirs(self.data_dir)
        
        #if not os.path.exists(self.tfidf_storage_dir ):
        #    os.makedirs(self.tfidf_storage_dir)

        self.tfidf_obj = tfidf.TFIDF()
        self.hostname = socket.gethostbyname(socket.gethostname())
        self.daemon = threading.Thread(target=self.tfidf_generater, args=())
        self.do_exit = False
        self.daemon.start()

    def tfidf_generater(self):
        while not self.do_exit:
            # create lock on todo_folder
            # access data from the todo_folder
            # read files and rename to ip_macaddress_filenum
            # move to new_folder hdfs with folder as time
            # unlock folder
            # read hdfs folder
            data_folder, folder_name = self.queue.get()
            b = self.tfidf_obj.get_tf_idf(data_folder)
            tf_idf_map, file_to_id_map, id_to_file_map, id_to_word, word_to_id = self.tfidf_obj.get_tf_idf_map(b)
            
            data_dict = {"tf_idf_map": tf_idf_map, "file_to_id_map": file_to_id_map,
                         "id_to_file_map": id_to_file_map, "id_to_word": id_to_word, "word_to_id": word_to_id}
            start_time = time.time()
            
            tfidfFileName = "/tmp/"+folder_name+".tfidf"
            with open(tfidfFileName, 'wb') as tfidf_file:
                pickle.dump(data_dict,tfidf_file)
                
            os.system( "hdfs dfs -put "+ tfidfFileName + "  /tfidfDir  ")
            os.remove(tfidfFileName)
            print("--- %s seconds ---" % (time.time() - start_time))
            print("Sleeping for %s time" % self.interval)
            time.sleep(self.interval)

    def accept_data(self,interval):
        # read data from ftp_folder to todo_folder
        while True:
          files = [f for f in listdir(self.ftp_folder) if isfile(join(self.ftp_folder, f))]
          if len(files) == 0:
              print("No files in this interval")
              time.sleep(interval)
              continue
          folder_name = self.hostname+"_"+ time.strftime("%Y%m%d-%H%M%S")
          todo_folder = "/dataDir/" + folder_name
          tmpFolder = "/tmp/" + folder_name
  
          os.system("hdfs dfs -mkdir " + todo_folder)
  
          if os.path.exists(tmpFolder):
              shutil.rmtree(tmpFolder)
          os.makedirs(tmpFolder)
          for f in files:
              shutil.move(self.ftp_folder + f, tmpFolder)
  
          os.system("hdfs dfs -put "+tmpFolder +"/* "+todo_folder)
  
          shutil.rmtree(tmpFolder)
          self.queue.put((self.data_dir+folder_name,folder_name))

    def run_cmd(self, args_list):
            """
            run linux commands
            """
            # import subprocess
            print('Running system command: {0}'.format(' '.join(args_list)))
            #client_hdfs = InsecureClient('http://152.7.99.46:9000')
            #client_hdfs.write(hdfs_path='/user/hdfs/wiki/helloworld.csv', encoding = 'utf-8') 
            proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize = -1)
            s_output, s_err = proc.communicate()
            s_return =  proc.returncode
            return s_return, s_output, s_err


parser = ArgumentParser(description='help for command')
parser.add_argument("--staging_dir", required=True,type=str,
                    help="State the role of system : master / worker")
parser.add_argument("--interval",  type=int,
                    help="interval to update tfidf values")
args = parser.parse_args()
if not os.path.exists(args.staging_dir ):
    os.makedirs(args.staging_dir)
    
s = Service(ftp_folder=args.staging_dir,
            data_dir="hdfs://152.7.99.46:9000/dataDir/",
            tfidf_storage_dir="hdfs://152.7.99.46:9000/tfidfDir/", interval=args.interval)
s.accept_data(args.interval)
s.do_exit = True

