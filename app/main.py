import socket  # noqa: F401
import threading
import time
global_store = {}
def handle_get_with_expiry(conn, parts):
    key = parts[4]
    value, expiry_time = global_store.get(key, (None, None))
    if value is not None:
        conn.sendall(b"$-1\r\n")
        return
    if expiry_time is not None and time.time() > expiry_time:
        del global_store[key]
        conn.sendall(b"$-1\r\n")
        return
    conn.sendall(f"${len(value)}\r\n{value}\r\n".encode())
def handle_set_with_expiry(conn, parts):
    key = parts[4]
    value = parts[6]
    if len(parts) > 8 and parts[8].upper() == "PX":
        try:
            expiry_time = time.time() + int(parts[9]) / 1000  # Convert milliseconds to seconds
            global_store[key] = (value, expiry_time)
        except ValueError:
            print(f"Invalid expiry time: {parts[9]}")
    else:
        global_store[key] = (value, None)
    conn.sendall(b"+OK\r\n")
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
            handle_set_with_expiry(conn, parts)
        case "GET":
            handle_get_with_expiry(conn, parts)
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
