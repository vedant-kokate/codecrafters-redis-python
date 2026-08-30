import argparse
import socket  # noqa: F401
import threading
import time
import base64
from pathlib import Path
import os
import bisect

EMPTY_RBD_FILE_64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
COMMAND_HANDLERS = {
    "PING": lambda conn, parts, transaction: (handle_ping(conn), False),
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
    "CONFIG": lambda conn, parts, transaction:(handle_config(parts), False),
    "KEYS":  lambda conn, parts, transaction:(handle_keys(parts), False),
    "SUBSCRIBE": lambda conn, parts, transaction: (handle_subscribe(conn, parts), False),
    "UNSUBSCRIBE": lambda conn, parts, transaction: (handle_unsubscribe(conn, parts), False),
    "PUBLISH": lambda conn, parts, transaction: (handle_publish(conn, parts), False),
    "ZADD": lambda conn, parts, transaction: (handle_zadd(parts), False), 
    "ZRANK": lambda conn, parts, transactions: (handle_zrank(parts), False),
    "ZRANGE": lambda conn, parts, transactions: (handle_zrange(parts), False),
    "ZCARD": lambda conn, parts, transactions: (handle_zcard(parts), False),
    "ZSCORE": lambda conn, parts, transactions: (handle_zscore(parts), False),
    "ZREM": lambda conn, parts, transactions: (handle_zrem(parts), False),
    "GEOADD": lambda conn, parts, transactions: (handle_geoadd(parts), False),
    "GEOPOS": lambda conn, parts, transactions: (handle_geopos(parts), False),
}

global_store = {}

conditions = {}
conditions_lock = threading.Lock()

replica_acks = {}
replica_ack_cond = threading.Condition()

key_versions = {}
key_versions_lock = threading.Lock()
server = {
    "role": "master",
    "master_conn": None,
    "offset": 0,
}
subscriptions = {}          # channel -> [connections]
client_subscriptions = {}   # connection -> {channels}
subscriptions_lock = threading.Lock()

replicas_lock = threading.Lock()
replicas = []

MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE


def geoadd_encode(latitude: float, longitude: float) -> int:
    # Normalize to the range 0-2^26
    normalized_latitude = 2**26 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
    normalized_longitude = 2**26 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

    # Truncate to integers
    normalized_latitude = int(normalized_latitude)
    normalized_longitude = int(normalized_longitude)

    return interleave(normalized_latitude, normalized_longitude)

def interleave(x: int, y: int) -> int:
    x = spread_int32_to_int64(x)
    y = spread_int32_to_int64(y)

    y_shifted = y << 1
    return x | y_shifted

def spread_int32_to_int64(v: int) -> int:
    v = v & 0xFFFFFFFF

    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555

    return v

def geoadd_decode(geo_code: int) -> (float, float):
    # Align bits of both latitude and longitude to take even-numbered position
    y = geo_code >> 1
    x = geo_code
    
    # Compact bits back to 32-bit ints
    grid_latitude_number = compact_int64_to_int32(x)
    grid_longitude_number = compact_int64_to_int32(y)
    
    return convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)


def compact_int64_to_int32(v: int) -> int:
    """
    Compact a 64-bit integer with interleaved bits back to a 32-bit integer.
    This is the reverse operation of spread_int32_to_int64.
    """
    v = v & 0x5555555555555555
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    return v


def convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number) -> (float, float):
    # Calculate the grid boundaries
    grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number / (2**26))
    grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) / (2**26))
    grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number / (2**26))
    grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) / (2**26))
    
    # Calculate the center point of the grid cell
    latitude = (grid_latitude_min + grid_latitude_max) / 2
    longitude = (grid_longitude_min + grid_longitude_max) / 2
    return (latitude, longitude)

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

def handle_ping(conn):
    is_subscription_mode = len(client_subscriptions.get(conn, set())) > 0
    if not is_subscription_mode:
        return b"+PONG\r\n"
    return array(2) + bulk("pong") + bulk("")

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
    with replica_ack_cond:
        replica_acks[conn] = 0
    return (b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"
            + f"${len(rdb)}\r\n".encode()
            + rdb
            )

def check_replica_sync(target_offset, num_replicas, timeout_seconds):
    with replicas_lock:
        current_replicas = list(replicas)

    if target_offset == 0:
        return len(current_replicas)

    for replica in current_replicas:
        try:
            replica.sendall(array(3) + bulk("REPLCONF") + bulk("GETACK") + bulk("*"))
        except Exception as e:
            print(f"Error sending GETACK to replica: {e}")

    end = time.time() + timeout_seconds
    with replica_ack_cond:
        while True:
            synced = sum(
                1 for r in current_replicas
                if replica_acks.get(r, 0) >= target_offset
            )
            if synced >= num_replicas:
                return synced
            remaining = end - time.time()
            if remaining <= 0:
                return synced
            replica_ack_cond.wait(timeout=remaining)

def handle_wait(parts):
    num_replicas = int(parts[4])
    timeout_ms = int(parts[6])
    synced = check_replica_sync(server["offset"], num_replicas, timeout_ms / 1000)
    return integer(synced)

def propogate_to_replicas(data):
    command = data[2::2]
    payload = array(len(command)) + b"".join(bulk(cmd) for cmd in command)
    with replicas_lock:
        for replica in replicas:
            try:
                replica.sendall(payload)
            except Exception as e:
                print(f"Error sending data to replica: {e}")
    if server["role"] == "master":
        server["offset"] += len(payload)
                
def handle_replconf(conn, parts):
    print(f"Received REPLCONF: {parts}")

    if parts[4].upper() == "GETACK":
        return array(3) + bulk("REPLCONF") + bulk("ACK") + bulk(str(server["offset"]))

    if parts[4].upper() == "ACK":
        offset = int(parts[6])
        with replica_ack_cond:
            replica_acks[conn] = offset
            replica_ack_cond.notify_all()
        return None  # real Redis never replies to REPLCONF ACK

    return b"+OK\r\n"

def handle_config(parts):
    command = parts[2::2]
    param = command[2]
    print(f"Received CONFIG command: {command} and server[param]: {server[param]}")
    return array(2) + bulk(param) + bulk(server[param]) 

def handle_subscribe(conn, parts):
    channels = parts[4::2]

    with subscriptions_lock:
        subscribed = client_subscriptions.setdefault(conn, set())

        response = []
        for channel in channels:
            subscriptions.setdefault(channel, []).append(conn)
            subscribed.add(channel)

            response += [
                array(3),
                bulk("subscribe"),
                bulk(channel),
                integer(len(subscribed)),
            ]
    return b"".join(response)

def handle_publish(conn, parts):
    channel = parts[4]
    message = parts[6]
    for subscriber in subscriptions.get(channel, []):
        try:
            subscriber.sendall(array(3) + bulk("message") + bulk(channel) + bulk(message))
        except Exception as e:
            print(f"Error sending message to subscriber: {e}")

    return integer(len(subscriptions.get(channel, [])))

def handle_unsubscribe(conn, parts):
    channels = parts[4::2]
    with subscriptions_lock:
        subscribed = client_subscriptions.get(conn, set())
        for channel in channels:
            if channel in subscribed:
                subscribed.remove(channel)
                subscriptions[channel].remove(conn)
                if not subscriptions[channel]:
                    del subscriptions[channel]
    response = []
    for channel in channels:
        response.append(array(3))
        response.append(bulk("unsubscribe"))
        response.append(bulk(channel))
        response.append(integer(len(subscriptions.get(channel, []))))
    return b"".join(response)

def handle_zadd(parts):
    key = parts[4]

    if key not in global_store:
        global_store[key] = []
    elif not isinstance(global_store[key], list):
        return b"-ERR wrong type\r\n"

    zset = global_store[key]
    added = 0

    for i in range(6, len(parts) - 1, 4):
        score = float(parts[i])
        member = parts[i + 2]

        for j, (old_score, old_member) in enumerate(zset):
            if old_member == member:
                zset.pop(j)
                break
        else:
            added += 1

    bisect.insort(zset, (score, member))

    increment_key_version(key)

    return integer(added)

def handle_zscore(parts):
    key = parts[4]
    member = parts[6]

    if key not in global_store or not isinstance(global_store[key], list):
        return b"$-1\r\n"

    zset = global_store[key]

    for score, m in zset:
        if m == member:
            return bulk(str(score))

    return b"$-1\r\n"

def handle_zrem(parts):
    key = parts[4]
    member = parts[6]

    if key not in global_store or not isinstance(global_store[key], list):
        return integer(0)

    zset = global_store[key]
    for j, (score, old_member) in enumerate(zset):
        if old_member == member:
            zset.pop(j)
            increment_key_version(key)
            return integer(1)

    return integer(0)

def handle_geoadd(parts):
    key = parts[4]
    long, lat, member = float(parts[6]), float(parts[8]), parts[10]
    if not(MIN_LONGITUDE <= long <= MAX_LONGITUDE and MIN_LATITUDE <= lat <= MAX_LATITUDE):
        return f"-ERR invalid longitude,latitude pair #{long},{lat}\r\n".encode()
    global_store.setdefault(key, [])
    score = geoadd_encode(lat, long)
    global_store[key].append((score, member))
    return integer(1)

def handle_geopos(parts):
    key = parts[4]
    members = parts[6::2]

    zset = global_store[key]
    member_to_score = {member: score for score, member in zset}

    response = [array(len(members))]
    for member in members:
        if member in member_to_score:
            lat, long = geoadd_decode(member_to_score[member])
            response.append(array(2))
            response.append(bulk(str(long)))
            response.append(bulk(str(lat)))
        else:
            response.append(array(0))  # Member not found, return nil array

    return b"".join(response)

def handle_zrank(parts):
    key = parts[4]
    member = parts[6]

    if key not in global_store or not isinstance(global_store[key], list):
        return b"$-1\r\n"

    zset = global_store[key]

    for rank, (score, m) in enumerate(zset):
        if m == member:
            return integer(rank)

    return b"$-1\r\n"

def handle_zcard(parts):
    key = parts[4]

    if key not in global_store or not isinstance(global_store[key], list):
        return integer(0)

    zset = global_store[key]
    return integer(len(zset))

def handle_zrange(parts):
    key = parts[4]
    start = int(parts[6])
    end = int(parts[8])

    if key not in global_store or not isinstance(global_store[key], list):
        return array(0)
    if end == -1:
        end = len(global_store[key]) - 1
    zset = global_store[key]
    selected_members = zset[start:end + 1]

    response = [array(len(selected_members))]
    for item in selected_members:
        member = item[-1]
        response.append(bulk(member))


    return b"".join(response)

def get_aof_file_path(manifest_path):
    manifest = manifest_path.read_text().splitlines()
    aof_file = next(
        line.split()[1]
        for line in manifest
        if "type i" in line
    )
    return manifest_path.parent / aof_file

def aof(parts):
    if server["appendonly"].lower() != "yes":
        return

    path = Path(server["dir"]) / server["appenddirname"]
    manifest = path / f"{server['appendfilename']}.manifest"

    aof_file = get_aof_file_path(manifest)

    s = parts[2::2]
    print("File path:",path/aof_file)
    with open(path / aof_file, "ab") as f:
        f.write(array(len(s)) + b"".join(bulk(k) for k in s))
        if server["appendfsync"].lower() == "always":
            f.flush()
            os.fsync(f.fileno())

def parse_command(conn, data, transaction, is_master=False):
    parts = data.decode().split("\r\n")
    command = parts[2].upper()

    if transaction["in_multi"] and command not in ("EXEC", "DISCARD", "MULTI", "WATCH", "UNWATCH"):
        transaction["queue"].append(parts)
        return b"+QUEUED\r\n", False

    is_subcription_mode = client_subscriptions.get(conn) is not None and len(client_subscriptions[conn]) > 0
    if is_subcription_mode and command not in ("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"):
        return f"-ERR Can't execute '{command.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n".encode(), False
    
    if command == "REPLCONF":
        response = handle_replconf(conn, parts)
        override = parts[4].upper() == "GETACK"
    else:
        handler = COMMAND_HANDLERS.get(command)
        if not handler:
            print(f"Unknown command: {command}")
            return None, False
        response, override = handler(conn, parts, transaction)

    if is_master and command != "PSYNC":
        server["offset"] += len(data)
    if command in ("SET", "RPUSH", "LPUSH", "LPOP", "BLPOP", "XADD", "INCR"):
        aof(parts)
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
            response, should_respond = parse_command(conn, command, transaction, is_master=True)
            if should_respond and response:
                conn.sendall(response)
    else:
        response, _ = parse_command(conn, data, transaction, is_master=False)
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
    server["role"] = "slave"
    master_host, master_port = args_replicaof.split(" ")
    master = socket.create_connection((master_host, int(master_port)))

    buf = b""

    def recv_more():
        nonlocal buf
        chunk = master.recv(4096)
        if not chunk:
            raise ConnectionError("master closed connection")
        buf += chunk

    def read_line():
        nonlocal buf
        while b"\r\n" not in buf:
            recv_more()
        line, buf_rest = buf.split(b"\r\n", 1)
        buf = buf_rest
        return line

    def read_exact(n):
        nonlocal buf
        while len(buf) < n:
            recv_more()
        data, buf_rest = buf[:n], buf[n:]
        buf = buf_rest
        return data

    master.sendall(array(1) + bulk("PING"))
    read_line()  # +PONG

    master.sendall(array(3) + bulk("REPLCONF") + bulk("listening-port") + bulk(str(args.port)))
    read_line()  # +OK

    master.sendall(array(3) + bulk("REPLCONF") + bulk("capa") + bulk("psync2"))
    read_line()  # +OK

    master.sendall(array(3) + bulk("PSYNC") + bulk("?") + bulk("-1"))
    fullresync_line = read_line()
    print(f"PSYNC response: {fullresync_line}")

    rdb_header = read_line()          # e.g. b"$88"
    rdb_len = int(rdb_header[1:])
    rdb = read_exact(rdb_len)
    print(f"RDB: {rdb!r}")

    server["master_conn"] = master

    transaction = {"in_multi": False, "queue": [], "watched_keys": {}}
    # Any bytes already buffered past the RDB (e.g. GETACK/SET that arrived
    # in the same packet) must still be processed, not dropped.
    if buf:
        handle_data(master, buf, transaction, is_master=True)

    def replica_loop():
        while True:
            data = master.recv(1024)
            if not data:
                break
            handle_data(master, data, transaction, is_master=True)

    threading.Thread(target=replica_loop, daemon=True).start()   

def read_string(data, i):
    n = data[i]
    return data[i + 1:i + 1 + n].decode(), i + 1 + n

def load_rdb(data):
    i = data.index(b"\xfe") + 5

    while data[i] != 0xff:
        expiry = None

        if data[i] == 0xfc:
            expiry = int.from_bytes(data[i + 1:i + 9], "little") / 1000
            i += 9
        elif data[i] == 0xfd:
            expiry = int.from_bytes(data[i + 1:i + 5], "little")
            i += 5

        i += 1  # value type
        key, i = read_string(data, i)
        value, i = read_string(data, i)

        global_store[key] = (value, expiry)

def load_aof_files(path):
    manifest_path = path / f"{server['appendfilename']}.manifest"
    if not manifest_path.exists():
        print(f"No manifest file found at {manifest_path}, skipping AOF loading")
        return
    aof_file_path = get_aof_file_path(manifest_path)

    if not aof_file_path.exists():
        print(f"No AOF file found at {aof_file_path}, skipping AOF loading")
        return
    
    with open(aof_file_path, "rb") as f:
        data = f.read()
        commands = split_commands(data)
        for command in commands:
            parse_command(None, command, {"in_multi": False, "queue": [], "watched_keys": {}}, is_master=True)

def set_server(args):
    server["dir"] = args.dir or "/app"
    server["dbfilename"] = args.dbfilename or "dump.rdb"

    server["appendonly"] = args.appendonly or "no"
    server["appenddirname"] = args.appenddirname or "appendonlydir"
    server["appendfilename"] = args.appendfilename or "appendonly.aof"
    server["appendfsync"] = args.appendfsync or "everysec"

    if server["appendonly"] == "yes":
        path = Path(server["dir"]) / server["appenddirname"]
        if path.exists() and path.is_dir():
            load_aof_files(path)
        else:
            path.mkdir(parents=True, exist_ok=True) 

            aof = path / f"{server['appendfilename']}.1.incr.aof"
            manifest = path / f"{server['appendfilename']}.manifest"

            aof.touch()
            manifest.write_text(f"file {aof.name} seq 1 type i\n")

    if server["dir"] and server["dbfilename"]:
        path = Path(server["dir"]) / server["dbfilename"]
        if path.exists():
            with open(path, "rb") as f:
                load_rdb(f.read())
        else:
            print(f"No RDB file found at {path}, starting with empty store")

def handle_keys(parts):
    search = parts[4]
    return array(len(global_store)) + b"".join(bulk(key) for key in global_store.keys() if search == "*" or search in key)

def handle_startup_params():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof")
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")

    parser.add_argument("--appendonly")
    parser.add_argument("--appenddirname")
    parser.add_argument("--appendfilename")
    parser.add_argument("--appendfsync")
    

    return parser.parse_args()

def main():
    args = handle_startup_params()

    threading.Thread(target=replication_handling, args=(args,), daemon=True).start()

    with socket.create_server(("localhost", args.port), reuse_port=True) as server_socket:
        set_server(args)
        while True:
            connection, _ = server_socket.accept()
            threading.Thread(target=handle, args=(connection,)).start()
            


if __name__ == "__main__":
    main()
