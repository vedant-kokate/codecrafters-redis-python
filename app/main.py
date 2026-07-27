import socket  # noqa: F401
import threading
import time
global_store = {}

conditions = {}
conditions_lock = threading.Lock()

def get_condition(key):
    with conditions_lock:
        if key not in conditions:
            conditions[key] = threading.Condition()
        return conditions[key]

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
            expiry_time = time.time() + int(parts[10]) / 1000  # Convert milliseconds to seconds
            global_store[key] = (value, expiry_time)
        except ValueError:
            print(f"Invalid expiry time: {parts[10]}")
    else:
        global_store[key] = (value, None)
    conn.sendall(b"+OK\r\n")

def handle_rpush(conn, parts):
    key = parts[4]
    value = parts[6:len(parts):2]  # Get all values to be pushed
    cond = get_condition(key)
    with cond:
        if key not in global_store:
            global_store[key] = []   # Initialize as a list
        elif not isinstance(global_store[key], list):
            conn.sendall(b"-ERR wrong type\r\n")
            return
        global_store[key].extend(value)
        cond.notify()
    conn.sendall(f":{len(global_store[key])}\r\n".encode())

def handle_lrange(conn, parts):
    key, left, right = parts[4], int(parts[6]), int(parts[8])
    if key not in global_store or not isinstance(global_store[key], list):
        conn.sendall(f"*0\r\n".encode())
        return
    if right == -1:
        right = len(global_store[key]) - 1
    print(f"global_store[{key}]: {global_store[key]}")
    lst = global_store[key][left:right + 1]
    conn.sendall(f"*{len(lst)}\r\n".encode())
    for item in lst:
        conn.sendall(f"${len(item)}\r\n{item}\r\n".encode())

def handle_lpush(conn, parts):
    key = parts[4]
    value = parts[6:len(parts):2]  # Get all values to be pushed
    if key not in global_store:
        global_store[key] = [] # Initialize as a list
    elif not isinstance(global_store[key], list):
        conn.sendall(b"-ERR wrong type\r\n")
        return
    global_store[key] = value[::-1] + global_store[key]  # Prepend values
    print(f"global_store[{key}]: {global_store[key]}")
    conn.sendall(f":{len(global_store[key])}\r\n".encode())

def handle_llen(conn, parts):
    key = parts[4]
    if key not in global_store or not isinstance(global_store[key], list):
        conn.sendall(f":0\r\n".encode())
        return
    conn.sendall(f":{len(global_store[key])}\r\n".encode())

def handle_lpop(conn, parts):
    key = parts[4]
    count = int(parts[6]) if len(parts) > 6 else 1
    if key not in global_store or not isinstance(global_store[key], list) or len(global_store[key]) == 0:
        conn.sendall(b"$-1\r\n")
        return
    popped_items = []

    for _ in range(min(count, len(global_store[key]))):
        popped_items.append(global_store[key].pop(0))
    print(f"Popped items from {key}: {popped_items}")
    response = [f"*{len(popped_items)}\r\n"]
    if count == 1:
        item = popped_items[0]
        conn.sendall(f"${len(item)}\r\n{item}\r\n".encode())
        return
    
    for item in popped_items:
        response.append(f"${len(item)}\r\n{item}\r\n")
    conn.sendall("".join(response).encode())

def handle_blpop(conn, parts):
    key = parts[4]
    timeout =  None if float(parts[6]) == 0 else float(parts[6])
    cond = get_condition(key)

    with cond:
        success = cond.wait_for(
            lambda: key in global_store and isinstance(global_store[key], list) and len(global_store[key]) > 0,
            timeout=timeout
        )
        print("success =", success,"timeout =", timeout)

        if not success:
            conn.sendall(b"*-1\r\n")
            return

        item = global_store[key].pop(0)
        conn.sendall(
            f"*2\r\n"
            f"${len(key)}\r\n{key}\r\n"
            f"${len(item)}\r\n{item}\r\n".encode()
        )
def handle_type(conn, parts):
    key = parts[4]

    if key not in global_store:
        conn.sendall(b"+none\r\n")
        return

    value = global_store[key]

    if isinstance(value, list):
        if all(isinstance(item, dict) and "id" in item for item in value):
            conn.sendall(b"+stream\r\n")
        else:
            conn.sendall(b"+list\r\n")
    else:
        conn.sendall(b"+string\r\n") 

def validate_xadd_id(key, id):
    if len(global_store[key]) == 0:
        return True
    last_id = global_store[key][-1].get("id") 
    last_id_time, last_id_seq = map(int, last_id.split("-"))
    new_id_time, new_id_seq = map(int, id.split("-"))
    return (new_id_time > last_id_time) or (new_id_time == last_id_time and new_id_seq > last_id_seq)

def handle_xadd(conn, parts):
    key = parts[4]
    id = parts[6]
    field_value_pairs = parts[8:len(parts):2]  # Get all field-value pairs

    if key not in global_store:
        global_store[key] = []  # Initialize as a list for stream entries
    elif not isinstance(global_store[key], list):
        conn.sendall(b"-ERR wrong type\r\n")
        return
    if not validate_xadd_id(key, id):
        conn.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        return  
    entry = {"id": id}
    for i in range(0, len(field_value_pairs), 2):
        field = field_value_pairs[i]
        value = field_value_pairs[i + 1]
        entry[field] = value

    global_store[key].append(entry)
    conn.sendall(f"${len(id)}\r\n{id}\r\n".encode())

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
        case "LLEN":
            handle_llen(conn, parts)
        case "LPOP":
            handle_lpop(conn, parts)
        case "BLPOP":
            handle_blpop(conn, parts)  
        case "TYPE":
            handle_type(conn, parts)
        case "XADD":
            handle_xadd(conn, parts)
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
