import socket
import threading
import json
import struct
import os
from cryptography.fernet import Fernet

# =====================
# 基本設定
# =====================

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 59181))

FERNET_KEY = os.environ.get(
    "FERNET_KEY",
    "ISKsmqonrkcgCnFkvKmW0cHuR1gSDWWzueTh2jsOsDY="
).encode()

fernet = Fernet(FERNET_KEY)

clients = []
calendar_data = {}
lock = threading.Lock()

# =====================
# packet helpers
# =====================

def recvall(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def send_packet(sock, payload: dict):
    raw = json.dumps(payload).encode()
    enc = fernet.encrypt(raw)
    sock.sendall(struct.pack("!I", len(enc)) + enc)


def recv_packet(sock):
    header = recvall(sock, 4)
    if not header:
        return None
    size = struct.unpack("!I", header)[0]
    return recvall(sock, size)

# =====================
# broadcast
# =====================

def broadcast_calendar():
    payload = {
        "type": "calendar_all",
        "data": [
            {"date": d, "events": e}
            for d, e in calendar_data.items()
        ]
    }

    dead = []
    for c in clients:
        try:
            send_packet(c, payload)
        except Exception as e:
            print("broadcast error:", e)
            dead.append(c)

    for d in dead:
        if d in clients:
            clients.remove(d)

# =====================
# client handler
# =====================

def handle_client(conn, addr):
    print("接続:", addr)
    clients.append(conn)

    try:
        while True:
            raw = recv_packet(conn)
            if not raw:
                print("recv None → 切断")
                break

            data = json.loads(fernet.decrypt(raw).decode())
            print("RECV:", data)

            msg_type = data.get("type")

            if msg_type == "calendar_get":
                broadcast_calendar()

            elif msg_type == "calendar_set":
                date = data["date"]
                events = data["events"]

                with lock:
                    calendar_data[date] = events

                print("SAVE:", date, events)
                broadcast_calendar()

    except Exception as e:
        print("client error:", e)

    finally:
        print("切断:", addr)
        if conn in clients:
            clients.remove(conn)
        conn.close()

# =====================
# main
# =====================

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"server listening on {PORT}")

        while True:
            conn, addr = s.accept()
            threading.Thread(
                target=handle_client,
                args=(conn, addr),
                daemon=True
            ).start()

if __name__ == "__main__":
    main()
