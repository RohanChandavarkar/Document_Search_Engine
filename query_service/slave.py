import socket               

s = socket.socket() 
s1 =socket.socket()
client_host = 'localhost'
client_port = 4444

master_host = 'localhost'
master_port = 5555


s.bind((master_host, master_port))        
s.listen(1) 

while True:                    
    conn, addr = s.accept()     
    master_query = conn.recv(65535)


    s1.connect((client_host, client_port))
    s1.sendall(master_query)
    s.close
    s1.close




