import socket   
import sys 
import pickle          
s = socket.socket() 
s1 = socket.socket()        
master_host = sys.argv[1]
master_port = int(sys.argv[2])               


slave_host = 'localhost'
client_port = int(sys.argv[3])	
s.connect((master_host, master_port))
query = sys.argv[4]
client_host = socket.gethostname()
client_ip = socket.gethostbyname(client_host)
query_data = {}

query_data['user_ip'] = client_host
query_data['query'] = query

dictionary_data = pickle.dumps(query_data)

s.sendall(dictionary_data)

s1.bind((client_ip, client_port))        
s1.listen(50) 
conn, addr = s1.accept() 
data = conn.recv(65535)
print pickle.loads(data)


s.close 
s1.close
