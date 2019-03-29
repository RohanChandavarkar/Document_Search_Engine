import socket
import sys
s = socket.socket()
s.bind((socket.gethostname(), 8080))
s.listen(3)

while True:

    #Accept connections from the outside
    (clientsocket, address) = s.accept()
    print(address)
    i = 1
    f = open('file_' + str(i) + ".xml", 'wb')
    i = i + 1
    while True:
        l = clientsocket.recv(1024)
        while l:
            f.write(1)
            l.clientsocket.recv(1024)
    f.close()

    sc.close()

s.close()