import argparse
import socket  # noqa: F401
import threading
import time
import base64

EMPTY_RBD_FILE_64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
COMMAND_HANDLERS = {
    "PING": lambda conn, parts, transaction: (b"+PONG\r\n", False),
    "ECHO": lambda conn, parts, transaction: (bulk(parts[4]), False),
    "SET": lambda conn, parts, transaction: (handle_set_with_expiry(parts), False),
    "GET": lambda conn, parts, transaction: (handle_get_with_expiry(parts), False),
    "RPUSH": lambda conn, parts, transaction: (handle_rpush(parts), False),
    "LRANGE": lambda conn, parts, transaction: (handle_lrange(parts), False),
    "LPUSH": lambda conn, parts, transaction: (handle_lpush(parts), False),
    "LLEN": lambda conn, parts, transaction: (handle_llen(parts), False),
    "LPOP": lambda conn, parts, transaction: (handle_lpop(parts), False),
    "BLPOP": lambda conn, parts, transaction: (handle_blpop(parts), False),
    "TYPE": lambda conn, parts, transaction: (handle_type(parts), False),
    "XADD": lambda conn, parts, transaction: (handle_xadd(parts), False),
    "XRANGE": lambda conn, parts, transaction: (handle_xrange(parts), False),
    "XREAD": lambda conn, parts, transaction: (handle_xread(parts), False),
    "INCR": lambda conn, parts, transaction: (handle_incr(parts), False),
    "MULTI": lambda conn, parts, transaction: (handle_multi(conn, transaction), False),
    "EXEC": lambda conn, parts, transaction: (handle_exec(conn, transaction), False),
    "DISCARD": lambda conn, parts, transaction: (handle_discard(conn, transaction), False),
    "WATCH": lambda conn, parts, transaction: (handle_watch(conn, parts, transaction), False),
    "UNWATCH": lambda conn, parts, transaction: (handle_unwatch(conn, transaction), False),
    "INFO": lambda conn, parts, transaction: (handle_info(conn, parts), False),
    "PSYNC": lambda conn, parts, transaction: (handle_psync(conn, parts), False),
    "WAIT": lambda conn, parts, transaction: (handle_wait(parts), False),
}
global_store = {}

conditions = {}
conditions_lock = threading.Lock()

key_versions = {}
key_versions_lock = threading.Lock()
server = {
    "role": "master",
    "master_conn": None,
    "offset": 0,
}

replicas_lock = threading.Lock()
replicas = []

def bulk(s):
    return f"${len(s)}\r\n{s}\r\n".encode()

def array(n):
    return f"*{n}\r\n".encode()

def integer(n):
    return f":{n}\r\n".encode()

def get_condition(key):
    with conditions_lock:
        if key not in conditions:
            conditions[key] = threading.Condition()
        return conditions[key]

def get_key_version(key):
    with key_versions_lock:
        return key_versions.get(key, 0)

def increment_key_version(key):
    with key_versions_lock:
        key_versions[key] = key_versions.get(key, 0) + 1

def handle_get_with_expiry(parts):
    key = parts[4]
    value, expiry_time = global_store.get(key, (None, None))
    if value is None:
        return b"$-1\r\n"
    if expiry_time is not None and time.time() > expiry_time:
        del global_store[key]
        return b"$-1\r\n"
    return bulk(value)

def handle_set_with_expiry(parts):
    key = parts[4]
    value = parts[6]
    if len(parts) >= 10 and parts[8].upper() == "PX":
        try:
            expiry_time = time.time() + int(parts[10]) / 1000  # Convert milliseconds to seconds
            global_store[key] = (value, expiry_time)
            increment_key_version(key)
            propogate_to_replicas(parts)
        except ValueError:
            print(f"Invalid expiry time: {parts[10]}")
    else:
        global_store[key] = (value, None)
        increment_key_version(key)
        propogate_to_replicas(parts)
    return b"+OK\r\n"

def handle_rpush(parts):
    key = parts[4]
    value = parts[6:len(parts):2]  # Get all values to be pushed
    cond = get_condition(key)
    with cond:
        if key not in global_store:
            global_store[key] = []   # Initialize as a list
        elif not isinstance(global_store[key], list):
            return b"-ERR wrong type\r\n"
        global_store[key].extend(value)
        increment_key_version(key)
        cond.notify()
    return integer(len(global_store[key]))

def handle_lrange(parts):
    key, left, right = parts[4], int(parts[6]), int(parts[8])
    if key not in global_store or not isinstance(global_store[key], list):
        return array(0)
    if right == -1:
        right = len(global_store[key]) - 1
    print(f"global_store[{key}]: {global_store[key]}")
    lst = global_store[key][left:right + 1]
    return array(len(lst)) + b"".join(bulk(item) for item in lst)

def handle_lpush(parts):
    key = parts[4]
    value = parts[6:len(parts):2]  # Get all values to be pushed
    if key not in global_store:
        global_store[key] = [] # Initialize as a list
    elif not isinstance(global_store[key], list):
        return b"-ERR wrong type\r\n"
    global_store[key] = value[::-1] + global_store[key]  # Prepend values
    print(f"global_store[{key}]: {global_store[key]}")
    increment_key_version(key)
    return integer(len(global_store[key]))

def handle_llen(parts):
    key = parts[4]
    if key not in global_store or not isinstance(global_store[key], list):
        return integer(0)
    return integer(len(global_store[key]))

def handle_lpop(parts):
    key = parts[4]
    count = int(parts[6]) if len(parts) > 6 else 1
    if key not in global_store or not isinstance(global_store[key], list) or len(global_store[key]) == 0:
        return b"$-1\r\n"
    popped_items = []

    for _ in range(min(count, len(global_store[key]))):
        popped_items.append(global_store[key].pop(0))
    print(f"Popped items from {key}: {popped_items}")
    response = [array(len(popped_items))]
    if count == 1:
        item = popped_items[0]
        return bulk(item)
    
    for item in popped_items:
        response.append(bulk(item))
    increment_key_version(key)
    return  b"".join(response)

def handle_blpop(parts):
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
            return b"*-1\r\n"

        item = global_store[key].pop(0)
        increment_key_version(key)
        return(
            array(2)
            + bulk(key)
            + bulk(item)
        )
def handle_type(parts):
    key = parts[4]

    if key not in global_store:
        return b"+none\r\n"

    value = global_store[key]

    if isinstance(value, list):
        if all(isinstance(item, dict) and "id" in item for item in value):
            return b"+stream\r\n"
        else:
            return b"+list\r\n"
    else:
        return b"+string\r\n" 

def validate_xadd_id(key, id):
    if len(global_store[key]) == 0:
        return True
    last_id = global_store[key][-1].get("id") 
    last_id_time, last_id_seq = map(int, last_id.split("-"))
    new_id_time, new_id_seq = map(int, id.split("-"))
    return (new_id_time > last_id_time) or (new_id_time == last_id_time and new_id_seq > last_id_seq)

def generate_xadd_id(key, id):
    if '*' not in id:
        return id

    last_id = global_store[key][-1].get("id") if global_store[key] else None
    if id == '*':
        print(f"Generating new ID for key '{key}' with '*'")
        id = f"{int(time.time() * 1000)}-0"  # Use current time in milliseconds
        if last_id:
            last_id_time, last_id_seq = map(int, last_id.split("-"))
            new_id_time, new_id_seq = map(int, id.split("-"))
            if new_id_time == last_id_time:
                id = f"{new_id_time}-{last_id_seq + 1}"
        return id
    else:
        pre, seq = id.split('-') 
        if last_id:
            last_id_time, last_id_seq = map(int, last_id.split("-"))
            if int(pre) > last_id_time:
                return f"{pre}-0"
            return f"{pre}-{last_id_seq + 1}"
        return f"{pre}-0" if pre != '0' else "0-1"
def handle_xadd(parts):
    key = parts[4]
    id = parts[6]
    field_value_pairs = parts[8:len(parts):2]  # Get all field-value pairs
    if id == "0-0":
        return f"-ERR The ID specified in XADD must be greater than {id}\r\n".encode()
    if key not in global_store:
        global_store[key] = []  # Initialize as a list for stream entries
    elif not isinstance(global_store[key], list):
        return b"-ERR wrong type\r\n"
    id = generate_xadd_id(key, id)
    if not validate_xadd_id(key, id):
        return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    entry = {"id": id}
    for i in range(0, len(field_value_pairs), 2):
        field = field_value_pairs[i]
        value = field_value_pairs[i + 1]
        entry[field] = value
    cond = get_condition(key)
    with cond:
        global_store[key].append(entry)
        increment_key_version(key)
        cond.notify()
    return f"${len(id)}\r\n{id}\r\n".encode()

def handle_xrange(parts):
    key = parts[4]
    start_id = parts[6]
    end_id = parts[8]
    
    if not (key in global_store and isinstance(global_store[key], list) and all(isinstance(entry, dict) and "id" in entry for entry in global_store[key])):
        return array(0)

    entries = []
    for entry in global_store[key]:
        entry_id = entry["id"]
        if (start_id == "-" or entry_id >= start_id) and (end_id == "+" or entry_id <= end_id):
            entries.append(entry)
        
    response = [array(len(entries))]
    for entry in entries:
        response.append(array(2))
        response.append(bulk(entry['id']))

        flat = []
        for k, v in entry.items():
            if k != "id":
                flat.extend([k, v])

        response.append(array(len(flat)))
        for item in flat:
            response.append(bulk(item))

    return b"".join(response)
def handle_xread(parts):
    streams_index = parts.index("streams")
    args = parts[streams_index + 2::2]  # Skip "$len" elements
    n = len(args) // 2
    keys = args[:n]
    ids = args[n:]
    dollar_id = None
    if parts[4].upper() == "BLOCK":
        block_time = int(parts[6])
        timeout = block_time / 1000.0 if block_time > 0 else None
        key = keys[0]  # Assuming only one key for simplicity
        last_id = ids[0]  # Corresponding ID for the key
        if  last_id == '$':
            last_id = global_store[key][-1]["id"] if key in global_store and global_store[key] else "0-0"
            dollar_id = last_id
        cond = get_condition(key)
        print(f"Waiting for new entries in stream '{key}' after ID '{last_id}' with timeout {timeout} seconds")
        with cond:
            success = cond.wait_for(
                lambda: (
                    key in global_store
                    and any(entry["id"] > last_id for entry in global_store[key])
                ),
                timeout=timeout,
            )
            print(global_store[key])
            print("success =", success,"timeout =", timeout)
    
            if not success:
                return b"*-1\r\n"
            

    response = [array(len(keys))]
    for key, last_id in zip(keys, ids):
        if last_id == '$' and dollar_id is not None:
            last_id = dollar_id
        print(f"Fetching entries from stream '{key}' after ID '{last_id}'")
        if not (key in global_store and isinstance(global_store[key], list) and all(isinstance(entry, dict) and "id" in entry for entry in global_store[key])):
                return array(0)
        entries = []
        print(f"global_store[{key}]: {global_store[key]}")
        for entry in global_store[key]:
            entry_id = entry["id"]
            if (entry_id > last_id):
                entries.append(entry)
        
        response.append(array(2))                  # [stream name, entries]
        response.append(bulk(key))
        response.append(array(len(entries)))
        for entry in entries:
            response.append(array(2))
            response.append(bulk(entry['id']))

            flat = []
            for k, v in entry.items():
                if k != "id":
                    flat.extend([k, v])

            response.append(array(len(flat)))

            for item in flat:
                response.append(bulk(item))
    return b"".join(response)
def handle_incr(parts):
    key = parts[4]
    value, expiry_time = global_store.get(key, (None, None))
    if value is None:
        new_value = 1
    else:
        try:
            new_value = int(value) + 1
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
    global_store[key] = (str(new_value), expiry_time)
    increment_key_version(key)
    return integer(new_value)

def handle_multi(conn, transaction):
    transaction["in_multi"] = True
    transaction["queue"] = []
    return b"+OK\r\n"

def handle_exec(conn, transaction):
    if not transaction["in_multi"]:
        return b"-ERR EXEC without MULTI\r\n"

    for key, watched_version in transaction["watched_keys"].items():
        if get_key_version(key) != watched_version:
            transaction["in_multi"] = False
            transaction["queue"].clear()
            transaction["watched_keys"].clear()

            return b"*-1\r\n"

    transaction["in_multi"] = False

    responses = []

    for parts in transaction["queue"]:
        response, _ = parse_command(
            conn,
            ("\r\n".join(parts) + "\r\n").encode(),
            transaction,
        )
        responses.append(response)

    transaction["queue"].clear()
    transaction["watched_keys"].clear()

    return array(len(responses)) + b"".join(responses)

def handle_discard(conn, transaction):
    if not transaction["in_multi"]:
        return b"-ERR DISCARD without MULTI\r\n"

    transaction["in_multi"] = False
    transaction["queue"].clear()
    transaction["watched_keys"].clear()

    return b"+OK\r\n"

def handle_watch(conn, parts, transaction):
    if transaction["in_multi"]:
        return b"-ERR WATCH inside MULTI is not allowed\r\n"

    keys = parts[4::2]

    for key in keys:
        transaction["watched_keys"][key] = get_key_version(key)

    return b"+OK\r\n"

def handle_unwatch(conn, transaction):
    transaction["watched_keys"].clear()
    return b"+OK\r\n"

def handle_info(conn, parts):
    response = ["# Replication", "role:"+server["role"],"master_replid:"+"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", "master_repl_offset:"+"0"]
    return bulk("\r\n".join(response))

def handle_psync(conn, parts):
    rdb = base64.b64decode(EMPTY_RBD_FILE_64)
    with replicas_lock:
        replicas.append(conn)
    return (b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
            + f"${len(rdb)}\r\n".encode()
            + rdb
            )

def check_replica_sync(target_offset, num_replicas):
    synced = 0
    print(f"Checking if {num_replicas} replicas have acknowledged offset {target_offset}")
    # print(f"Replicas: {replicas}, Target Offset: {target_offset}, Required Replicas: {num_replicas}")
    for replica in replicas:
        try:
            print(f"Checking replica sync for target offset {target_offset}")
            replica.sendall(array(3) + bulk("REPLCONF") + bulk("GETACK") + bulk("*")))
            response = replica.recv(1024)
            if not response.startswith(b":"):
                print(f"Unexpected response from replica: {response}")
                return False
            ack_offset = int(response[1:-2])  # Remove ':' and '\r\n'
            if ack_offset >= target_offset:
                synced += 1
        except Exception as e:
            print(f"Error checking replica sync: {e}")
            return False
    return synced >= num_replicas


def handle_wait(parts):
    num_replicas = int(parts[4])
    timeout = int(parts[6]) / 1000.0  # Convert milliseconds to seconds
    start_time = time.time()

    while True:
        with replicas_lock:
            if len(replicas) >= num_replicas and check_replica_sync(target_offset=server["offset"], num_replicas=num_replicas):
                return integer(len(replicas))

        elapsed_time = time.time() - start_time
        if elapsed_time >= timeout:
            with replicas_lock:
                return integer(len(replicas))
        time.sleep(0.01)  # Sleep for a short duration to avoid busy waiting

def propogate_to_replicas(data):
    command = data[2::2]
    with replicas_lock:
        for replica in replicas:
            try:
                replica.sendall(array(len(command)) + b"".join(bulk(cmd) for cmd in command))
            except Exception as e:
                print(f"Error sending data to replica: {e}")
                
def handle_replconf(parts):
    print(f"Received REPLCONF: {parts}")

    if parts[4].upper() == "GETACK":
        return array(3) + bulk("REPLCONF") + bulk("ACK") + bulk(str(server["offset"]))

    return b"+OK\r\n"


    
def parse_command(conn, data, transaction):
    parts = data.decode().split("\r\n")
    command = parts[2].upper()

    if transaction["in_multi"] and command not in ("EXEC", "DISCARD", "MULTI", "WATCH", "UNWATCH"):
        transaction["queue"].append(parts)
        return b"+QUEUED\r\n", False

    if command == "REPLCONF":
        response = handle_replconf(parts)
        override = parts[4].upper() == "GETACK"

    else:
        handler = COMMAND_HANDLERS.get(command)

        if not handler:
            print(f"Unknown command: {command}")
            return None, False

        response, override = handler(conn, parts, transaction)

    if command != "PSYNC":
        server["offset"] += len(data)

    return response, override

def split_commands(data):
    commands = []
    pos = 0
    while pos < len(data):
        start = data.find(b"*", pos)

        if start == -1:
            break

        line_end = data.find(b"\r\n", start)
        if line_end == -1:
            break

        count = int(data[start + 1:line_end])
        command_pos = line_end + 2

        for _ in range(count):
            if command_pos >= len(data) or data[command_pos:command_pos + 1] != b"$":
                break

            length_end = data.find(b"\r\n", command_pos)
            if length_end == -1:
                break

            length = int(data[command_pos + 1:length_end])
            command_pos = length_end + 2 + length + 2

        else:
            commands.append(data[start:command_pos])
            pos = command_pos
            continue

        break

    return commands


def handle_data(conn, data, transaction, is_master):
    print(f"RECEIVED FROM {'MASTER' if is_master else 'CLIENT'}: {data!r}")

    if is_master:
        commands = split_commands(data)
        print(f"Split into {len(commands)} command(s): {[cmd.decode() for cmd in commands]}")
        for command in commands:
            response, should_respond = parse_command(conn, command, transaction)
            if should_respond and response:
                conn.sendall(response)
    else:
        response, _ = parse_command(conn, data, transaction)

        if response:
            conn.sendall(response)


def handle(conn):
    transaction = {
        "in_multi": False,
        "queue": [],
        "watched_keys": {}
    }
    is_master = conn == server["master_conn"]
    while True:
        data = conn.recv(1024)
        if not data:
            break
        handle_data(conn, data, transaction, is_master)

def replication_handling(args):
    args_replicaof = args.replicaof
    if not args_replicaof:
        return
    server["role"] = "slave" if args_replicaof else "master"
    master_host, master_port = args_replicaof.split(" ")
    master = socket.create_connection((master_host, int(master_port)))

    master.sendall(array(1)+bulk("PING"))
    response = master.recv(1024)
    handle_data(master, response, {"in_multi": False, "queue": [], "watched_keys": {}}, is_master=True)

    master.sendall(array(3) + bulk("REPLCONF") + bulk("listening-port") + bulk("6380"))

    response = master.recv(1024)
    handle_data(master, response, {"in_multi": False, "queue": [], "watched_keys": {}}, is_master=True)
    

    master.sendall(array(3) + bulk("REPLCONF") + bulk("capa") + bulk("psync2"))
    response = master.recv(1024)
    handle_data(master, response, {"in_multi": False, "queue": [], "watched_keys": {}}, is_master=True)
    print(response)

    master.sendall(array(3) + bulk("PSYNC") + bulk("?") + bulk("-1"))

    response = master.recv(1024)
    handle_data(master, response, {"in_multi": False, "queue": [], "watched_keys": {}}, is_master=True)
    print(f"PSYNC response: {response}")  # FULLRESYNC

    rdb = master.recv(1024)
    handle_data(master, rdb, {"in_multi": False, "queue": [], "watched_keys": {}}, is_master=True)
    
    print(f"RDB: {rdb}")       # RDB

    server["master_conn"] = master
    threading.Thread(target=handle, args=(master,), daemon=True).start()
            

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof")
    args = parser.parse_args()
    replication_handling(args)
    with socket.create_server(("localhost", args.port), reuse_port=True) as server_socket:
        while True:
            connection, _ = server_socket.accept() 
            threading.Thread(target=handle, args=(connection,)).start()
            


if __name__ == "__main__":
    main()
