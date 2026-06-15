import socket  # noqa: F401
import threading

def parse_command(conn, data):
    parts = data.decode().split("\r\n")
    command = parts[2].upper()
    
    print(f"Parsed command: {command}")
    match command:
        case "PING":
            conn.sendall(b"+PONG\r\n") 
        case "ECHO":
            print(f"Echoing back: {parts[4]}")
            conn.sendall(b"+" + parts[4].encode() + b"\r\n")
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
