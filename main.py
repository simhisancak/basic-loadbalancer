import socket
from datetime import date
import time
import sys
from threading import Thread

HOST = "0.0.0.0"
HOST_PORT = 8080

BUFFER_SIZE = 1000000

SERVERS = [
    {
        "ip" : "0.0.0.0",
        "port" : 5001
    },
    {
        "ip" : "0.0.0.0",
        "port" : 5002
    },
    {
        "ip" : "0.0.0.0",
        "port" : 5003
    },
    {
        "ip" : "0.0.0.0",
        "port" : 5004
    }   
]

LOAD_COUNTER = 0


def convert(seconds,gmt):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    if hour+gmt >= 24:
        hour = hour - 24
    year, mouth, day = str(date.today()).split("-")
    return "%02d-%02d-%04d %02d-%02d-%02d" % (int(day) ,int(mouth) ,int(year) ,hour+gmt, minutes, seconds)



def log_writer(text):
    print(convert(time.time(),3) + " " + text)



def start_server():

    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    log_writer(f"Socket created {HOST}:{HOST_PORT}")


    try:
        soc.bind((HOST, HOST_PORT))
    except:
        log_writer(f"Bind failed. Error : {str(sys.exc_info())}")
        sys.exit()

    soc.listen(50) 
    log_writer(f"Socket now listening {HOST}:{HOST_PORT}")

    while True:
        connection, address = soc.accept()
        ip, port = str(address[0]), str(address[1])
        log_writer(f"Client Connected with {ip}:{port}")
        Thread(target=proxy, args=(connection,ip, port)).start()
        
    soc.close()

def proxy(c:socket.socket, ip, port):
    backend = chooseBackend()
    while True:
        bc = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        try:
            bc.connect(backend)
            Thread(target=soc_communication, args=(bc,c)).start()
            Thread(target=soc_communication, args=(c,bc)).start()
            log_writer(f"Porixed {ip}:{port} to {backend[0]}:{backend[1]}")
            break
        except ConnectionRefusedError:
            log_writer(f"Backend Connection Refused : {backend[0]}:{backend[1]}")
            backend = chooseBackend()
            pass
        except:
            log_writer(f"Connection failed. Error :  {str(sys.exc_info())}")
            pass


def soc_communication(a:socket.socket, b:socket.socket):
    a.sendall(b.recv(BUFFER_SIZE))    

def chooseBackend():
    global LOAD_COUNTER
    ret = (SERVERS[LOAD_COUNTER]["ip"], SERVERS[LOAD_COUNTER]["port"])
    LOAD_COUNTER += 1
    if LOAD_COUNTER >= len(SERVERS): LOAD_COUNTER = 0  
    return ret


if __name__ == "__main__":
    start_server()