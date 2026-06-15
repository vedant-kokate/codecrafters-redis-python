import socket  # noqa: F401
import threading

def handle(conn):
    while data := conn.recv(1024):
        conn.sendall(b"+PONG\r\n")

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    

    
    while True:
        connection, _ = server_socket.accept() 
        threading.Thread(target=handle, args=(connection,)).start()
        


if __name__ == "__main__":
    main()
