import sys
import socket               
import pickle
import Queue
import threading

s = socket.socket() 
s1 =socket.socket()

server_host = socket.gethostname()
server_ip = socket.gethostbyname(server_host)
print 'Server IP is: '+ server_ip
server_port = int(sys.argv[1])
client_port = int(sys.argv[2])
#slave_host = 'localhost'
#slave_port = 5555

request_queue = Queue.Queue()
response_queue = request_queue



s.bind((server_ip, server_port))        
s.listen(50) 


		

def extractData():
	global response_queue
	
	while True:
		d1 = response_queue.get()
		client_host = d1['user_ip']
		client_query = d1['query']
		result_dictionary = {}
		result_list = []
		result_dictionary['query'] = client_query
		result_dictionary['result'] = 	result_list
		result_data = pickle.dumps(result_dictionary)
		s1.connect((client_host,client_port))
		s1.sendall(result_data)
		s1.close


#acceptData_thread = threading.Thread(target=acceptData, args=(request_dict,))
#acceptData_thread.start()
extractData_thread = threading.Thread(target=extractData,args=())
extractData_thread.start()

while True:                    
    conn, addr = s.accept()     
    client_query = conn.recv(65535)
    request_dict = pickle.loads(client_query)
    request_queue.put(request_dict)

    #s1.connect((slave_host, slave_port))
    #s1.sendall(client_query)
    s.close
    
