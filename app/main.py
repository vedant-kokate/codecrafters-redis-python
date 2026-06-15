import socket  # noqa: F401
import threading
import time
from queue import deque
global_store = {}

def handle_get_with_expiry(conn, parts):
    key = parts[4]
    value, expiry_time = global_store.get(key, (None, None))
    if value is None:
        conn.sendall(b"$-1\r\n")
        return
    if expiry_time is not None and time.time() > expiry_time:
        del global_store[key]
        conn.sendall(b"$-1\r\n")
        return
    conn.sendall(f"${len(value)}\r\n{value}\r\n".encode())\
    
def handle_set_with_expiry(conn, parts):
    key = parts[4]
    value = parts[6]
    if len(parts) >= 10 and parts[8].upper() == "PX":
        try:
            print(f"parts: {parts}")
            expiry_time = time.time() + int(parts[10]) / 1000  # Convert milliseconds to seconds
            global_store[key] = (value, expiry_time)
        except ValueError:
            print(f"Invalid expiry time: {parts[10]}")
    else:
        global_store[key] = (value, None)
    conn.sendall(b"+OK\r\n")

def handle_rpush(conn, parts):
    key = parts[4]
    print(f"parts: {parts}")
    value = parts[6:len(parts):2]  # Get all values to be pushed
    print(f"value: {value}")
    if key not in global_store:
        global_store[key] = deque()   # Initialize as a list
    elif not isinstance(global_store[key], deque):
        conn.sendall(b"-ERR wrong type\r\n")
        return
    global_store[key].extend(value)
    conn.sendall(f":{len(global_store[key])}\r\n".encode())

def handle_lrange(conn, parts):
    key, left, right = parts[4], int(parts[6]), int(parts[8])
    if key not in global_store or not isinstance(global_store[key], list):
        conn.sendall(f"*0\r\n".encode())
        return
    if right == -1:
        right = len(global_store[key]) - 1
    lst = global_store[key][left:right + 1]
    conn.sendall(f"*{len(lst)}\r\n".encode())
    for item in lst:
        conn.sendall(f"${len(item)}\r\n{item}\r\n".encode())

def handle_lpush(conn, parts):
    key = parts[4]
    value = parts[6:len(parts):2]  # Get all values to be pushed
    if key not in global_store:
        global_store[key] = deque()  # Initialize as a list
    elif not isinstance(global_store[key], deque):
        conn.sendall(b"-ERR wrong type\r\n")
        return
    global_store[key].extendleft(value)  # Add to the left of the deque
    conn.sendall(f":{len(global_store[key])}\r\n".encode())

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
        case "RPUSH":
            handle_rpush(conn, parts)
        case "LRANGE":
            handle_lrange(conn, parts)
        case "LPUSH":
            handle_lpush(conn, parts)
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
