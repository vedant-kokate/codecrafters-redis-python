import socket  # noqa: F401
import threading

def handle(conn):
    while data := conn.recv(1024):
        msg = data.decode("utf-8").strip()
        print(f"Received: {msg}")
        conn.sendall(b"+PONG\r\n") 

def main():
    with socket.create_server(("localhost", 6379), reuse_port=True) as server:
        while True:
            connection, _ = server.accept() 
            threading.Thread(target=handle, args=(connection,)).start()
            


if __name__ == "__main__":
    main()
