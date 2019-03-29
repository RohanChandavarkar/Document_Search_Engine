import glob, pickle, time, re
import os, itertools, threading, queue
import conf
from conf import state
from argparse import ArgumentParser
from flask import Flask, request
from flask_restful import Resource, Api
import http.server as SimpleHTTPServer
import socketserver, atexit, socket
socketserver.TCPServer.allow_reuse_address=True
from flask import Flask, request, render_template, Response
import socket
from kazoo.client import KazooClient


serving_obj = None
serving_obj_lock = threading.Lock()
do_exit = False

zk = KazooClient(socket.gethostbyname(socket.gethostname()).strip()+":2181")
zk.start()


class Serving_server:

    def __init__(self,latest_tfidf_file, last_loaded_path):
        self.latest_tfidf_file = latest_tfidf_file
        with open(latest_tfidf_file,'rb') as tfidf_file:
            tfidf_dict = pickle.load(tfidf_file)

        self.tf_idf_map = tfidf_dict["tf_idf_map"]
        self.id_to_file_map = tfidf_dict["id_to_file_map"]
        self.word_to_id = tfidf_dict["word_to_id"]


    def get_files_of_query(self,query, tfid_filter_value=0.0):

        def get_query_ans(splited_query):
            ans = set()

            for word in splited_query:
                if word not in self.word_to_id:
                    continue

                t = set()
                word_id = self.word_to_id[word]
                print(word_id)
                if word_id in self.tf_idf_map:
                    for f in self.tf_idf_map[word_id]:
                        if f[1] > tfid_filter_value:
                            t.add(f[0])

                if len(ans) == 0:
                    ans = t
                else:
                    ans = ans & t

            files_list = set()
            for doc_id in ans:
                files_list.add(self.id_to_file_map[doc_id])

            return files_list

        splited_query = query.split()
        ans = set()
        query_combinations = []
        for i in range(1, len(splited_query)+1):
            l = list(itertools.combinations(splited_query, i))
            query_combinations += l
        while (len(ans) < 100) and len(query_combinations)>0:
            ans = ans.union(get_query_ans(query_combinations.pop()))

        return list(ans)


def change_serving_obj(frequency_in_sec):
    global do_exit, serving_obj_lock, serving_obj
    get_latest_tfidf_cmd = '''hadoop fs -ls -R /tfidfDir | awk -F" " '{print $6" "$7" "$8}' | sort -nr | head -1 | cut -d" " -f3'''
    latest_file_hadoop_path = ""
    
    latest_path = ""
     
    last_loaded_hadoop_path = ""
    try:
      os.makedirs("/tmp/tfidf_data/")
    except:
      pass
    while not do_exit:

        latest_file_hadoop_path = os.popen(get_latest_tfidf_cmd).read().strip()
        if not latest_file_hadoop_path.endswith(".tfidf"):
            continue

        if latest_file_hadoop_path != last_loaded_hadoop_path:
        
            # latest_tfidf_file = "/tmp/"
            latest_file_hadoop_path
            cmd = "hdfs dfs -get "+latest_file_hadoop_path+ " /tmp/tfidf_data/ "
            print(cmd)
            os.system(cmd)
            
            _, tmp_file_name = os.path.split(latest_file_hadoop_path.strip()) 
            tmp_file_path = "/tmp/tfidf_data/" + tmp_file_name
            print(tmp_file_path)
            servering_obj_temp = Serving_server(latest_tfidf_file=tmp_file_path,
                                                last_loaded_path=last_loaded_hadoop_path)

            last_loaded_hadoop_path = latest_file_hadoop_path
            #print(t.tf_idf_map)
            #print(print(t.tf_idf_map[70096]))
            print("locked")
            serving_obj_lock.acquire()
            serving_obj = servering_obj_temp
            serving_obj_lock.release()
            print("unlocked")
            

        time.sleep(frequency_in_sec)
    print(ans)


app = Flask(__name__)
api = Api(app)


class REST_Server(Resource):

    #@app.route('/query/<string:query>')
    def get(self, query):
        return self.calculate(query=query)

    @app.route('/files/<file_folder>/<file_path>')
    def get_file(file_folder,file_path):
        # to check file exists
        cmd = "hdfs dfs -get /dataDir/"+file_folder+ "/"+ file_path+" /tmp/"

        
        with open("/tmp/"+file_path) as f:
            data = f.read()
        os.remove("/tmp/"+file_path)
        return data

    def read_file(self,file_folder_list,file_path_list):
        # to check file exists
        path_list = ""
        for file_folder,file_path in zip(file_folder_list,file_path_list):
            path_list+= "  /dataDir/"+file_folder+"/"+file_path+" "
        
        if len(path_list) == 0:
            return []
        cmd = "hdfs dfs -get "+path_list+" /tmp/"
        os.system(cmd)
        print(cmd)
        print("")

        data = []
        for file_path in file_path_list:
          with open("/tmp/"+file_path) as f:
              data.append(f.read())
        return data

    def calculate(self, query):
        print(query)
        global serving_obj,serving_obj_lock
        while not serving_obj:
            time.sleep(0.1)
        serving_obj_lock.acquire()
        print(serving_obj)
        result = serving_obj.get_files_of_query(query=query)
        processed_result = []
        preview_1 = []
        preview_2 = []
        file_folder_list = []
        file_path_list = []
        for r in result[:40]:
            file_folder,file_path = r.split("/")[-2], r.split("/")[-1]
            file_folder_list.append(r.split("/")[-2])
            file_path_list.append(r.split("/")[-1])
            processed_result.append(r.split("/")[-2]+"/"+r.split("/")[-1])
        data_list = self.read_file(file_folder_list,file_path_list )
        for data in data_list:
            data = re.split(r"\.|\?|\!\n",data)
            
            preview_1.append(data[0])
            if len(data)>1:
                preview_2.append(data[1])
            else:
                preview_2.append(data[0])
                
        serving_obj_lock.release()
        if len(processed_result)>0:
            return Response(render_template('index.html', files=zip(processed_result,preview_1,preview_2), mimetype='text/html'))
        return Response("Not files matching term : "+query, mimetype='text/html')

api.add_resource(REST_Server, '/query/<string:query>')



# Server Code starts
worker_index = 0
def start_load_balancer(list_of_ip,port,worker_port):

    class myHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
        def do_GET(self):
            global worker_index
            self.send_response(301)
            is_available = False
            new_path = ""
            while not is_available:
                ip = list_of_ip[worker_index]
                print(ip,list_of_ip)
                status_file_path = str("/app/" + ip).strip()
                if zk.exists(status_file_path):
                    is_available = True
                worker_index = (worker_index+1) % len(list_of_ip)
                new_path = 'http://%s:%s%s' % (ip,worker_port, self.path)
            print(new_path)
            self.send_header('Location', new_path)
            self.end_headers()

    PORT = int(port)
    handler = socketserver.TCPServer(("", PORT), myHandler)
    print("serving at port "+str(port))
    handler.serve_forever()



def main():
    parser = ArgumentParser(description='help for command')
    parser.add_argument("--role", required=True,type=str,choices=['master','worker'],
                        help="State the role of system : master / worker")
    parser.add_argument("--list_of_ip", type=str, default = None,
                        help="if master provide ip of the workers")
    parser.add_argument("--port", required=True,type=int,
                        help="Port number to use")
    parser.add_argument("--worker_port",  type=int,
                        help="if master, provide worker's port")
    args = parser.parse_args()
    if args.role == "master":
        if not args.list_of_ip:
            raise Exception("please provide list of worker IPs")
        if not args.worker_port:
            raise Exception("please provide worker_port")
        start_load_balancer(list_of_ip=args.list_of_ip.split(','),port=args.port,worker_port=args.worker_port)
    elif args.role == "worker":
        
        status_file_path = "/app/" + socket.gethostbyname(socket.gethostname()).strip()
        if not zk.exists(status_file_path):
            zk.create(status_file_path, None, ephemeral=True, makepath=True)
        t_tfidf_generator = threading.Thread(target=change_serving_obj, args=(5,))
        t_tfidf_generator.start()
        app.run(host='0.0.0.0',port=args.port, debug=True, threaded=True)

if __name__ == '__main__':
    main()



