import socket  # noqa: F401
import threading
global_store = {}

def parse_command(conn, data):
    parts = data.decode().split("\r\n")
    command = parts[2].upper()
    
    print(f"Parsed command: {command}")
    match command:
        case "PING":
            conn.sendall(b"+PONG\r\n") 
        case "ECHO":
            message = parts[4]
            conn.sendall(f"${len(message)}\r\n{message}\r\n".encode())
        case "SET":
            key = parts[4]
            value = parts[6]
            global_store[key] = value
            conn.sendall(b"+OK\r\n")
        case "GET":
            key = parts[4]
            value = global_store.get(key)
            if value is not None:
                conn.sendall(f"${len(value)}\r\n{value}\r\n".encode())
            else:
                conn.sendall(b"$-1\r\n")  # Null bulk string
        case _:
            print(f"Unknown command: {command}")

def handle(conn):
    while data := conn.recv(1024):
        parse_command(conn, data)


def main():
    with socket.create_server(("localhost", 6379), reuse_port=True) as server:
        while True:
            connection, _ = server.accept() 
            threading.Thread(target=handle, args=(connection,)).start()
            


if __name__ == "__main__":
    main()
