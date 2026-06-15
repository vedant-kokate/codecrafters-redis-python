import socket  # noqa: F401
import threading

def parse_command(data):
    parts = data.decode().split("\r\n")
    command = parts[2].upper()
    print(f"Parsed command: {command}")
def handle(conn):
    while data := conn.recv(1024):
        parse_command(data)
        conn.sendall(b"+PONG\r\n") 

def main():
    with socket.create_server(("localhost", 6379), reuse_port=True) as server:
        while True:
            connection, _ = server.accept() 
            threading.Thread(target=handle, args=(connection,)).start()
            


if __name__ == "__main__":
    main()
