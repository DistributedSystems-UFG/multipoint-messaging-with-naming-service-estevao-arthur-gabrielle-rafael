import socket
import threading
import json
from requests import get
import duckdb
from datetime import datetime
from config import *
import zmq
import uuid


def get_public_ip():
    #return "127.0.0.1"
    return get("https://api.ipify.org").content.decode("utf8")


class Peer:
    def __init__(self, port=None):
        self.host = "0.0.0.0"
        # permite passar a porta por parametro (testes); senao, pergunta
        self.port = port if port is not None else int(input("Porta deste peer: "))
        self.name = "peer_" + str(self.port) + "_" + str(uuid.uuid4())[:4]
        # endereco canonico deste peer (o mesmo que e registrado no Servico de Nomes)
        self.address = f"{get_public_ip()}:{self.port}"
        self.peers = set()
        self.oldest_peer = None
        self.lock = threading.Lock()
        self.conn = duckdb.connect(f"peer_{self.port}.db")

        self._init_database()

    def _init_database(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER,
            sender VARCHAR,
            receiver VARCHAR,
            amount DOUBLE,
            timestamp TIMESTAMP
        )
        """)

    def conectar_servico_nomes(self):
        context = zmq.Context()
        s = context.socket(zmq.REQ)
        s.connect(f"tcp://{NS_HOST}:{NS_PORT}")

        #  bind
        s.send_json({"op": "bind", "name": self.name, "address": self.address})
        s.recv_json()

        # register
        s.send_json({"op": "register", "name": self.name, "type": "peer"})
        s.recv_json()

        # Descobre os outros
        s.send_json({"op": "discover", "type": "peer"})
        resposta = s.recv_json()

        lista = resposta.get("result", [])

        for p in lista:
            if p["name"] != self.name:
                self.peers.add(p["address"])

        # Pega o primeiro da lista como mais velho (a ordem de insercao no
        # Servico de Nomes preserva a ordem de chegada dos peers)
        if len(lista) > 1 and lista[0]["name"] != self.name:
            self.oldest_peer = lista[0]["address"]
        else:
            self.oldest_peer = None

        print(f"[PEERS CONHECIDOS] {self.peers}")
        print(f"[NO MAIS VELHO] {self.oldest_peer}")

    def notificar_peers(self):
        # Substitui o notify_all do antigo Group Manager, de forma
        # descentralizada. Como o Servico de Nomes (REQ/REP) nao consegue
        # empurrar mensagens para os peers, e o proprio peer recem-chegado
        # que avisa os peers ja existentes da sua entrada. Assim todos os
        # nos passam a conhecer todos (incl. os que entraram antes dele).
        msg = json.dumps({"type": "NEW_PEER", "peer": self.address})

        for peer in list(self.peers):
            try:
                host, port = peer.split(":")
                s = socket.socket()
                s.connect((host, int(port)))
                s.sendall(msg.encode())
                s.close()
            except Exception:
                pass

    def sync_with_oldest(self):
        if not self.oldest_peer:
            print("[SYNC] Nenhum peer antigo (primeiro nó)")
            return

        try:
            host, port = self.oldest_peer.split(":")
            s = socket.socket()
            s.connect((host, int(port)))

            msg = {"type": "SYNC_REQUEST"}
            s.sendall(json.dumps(msg).encode())

            response = json.loads(s.recv(65536).decode())
            s.close()

            if response["type"] == "SYNC_DATA":
                with self.lock:
                    for row in response["data"]:
                        self.conn.execute("""
                            INSERT INTO transactions VALUES (?, ?, ?, ?, ?)
                            """, (
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            datetime.fromisoformat(row[4]) if row[4] else datetime.now()
                        ))

                print(f"[SYNC] {len(response['data'])} registros recebidos")

        except Exception as e:
            print("[SYNC ERROR]", e)

    def handle_message(self, msg, is_replica=False):
        msg = json.loads(msg)
        msg_type = msg["type"]

        if msg_type == "NEW_PEER":
            new_peer = msg["peer"]
            if new_peer != self.address:
                self.peers.add(new_peer)
                print(f"[NOVO PEER] {new_peer}")

        elif msg_type == "SYNC_REQUEST":
            data = self.conn.execute("SELECT * FROM transactions").fetchall()

            serialized = []
            for row in data:
                serialized.append((
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4].isoformat() if row[4] else None
                ))

            response = {
                "type": "SYNC_DATA",
                "data": serialized
            }
            return json.dumps(response)

        elif msg_type == "INSERT":
            data = msg["data"]

            with self.lock:
                self.conn.execute("""
                    INSERT INTO transactions VALUES (?, ?, ?, ?, ?)
                """, (
                    data["id"],
                    data["sender"],
                    data["receiver"],
                    data["amount"],
                    datetime.now()
                ))

            print(f"[INSERT] {data}")

            if not is_replica:
                self.replicate(msg)

        elif msg_type == "REPLICA":
            self.handle_message(json.dumps(msg["data"]), True)

        elif msg_type == "SELECT":
            result = self.conn.execute("SELECT * FROM transactions").fetchall()
            print("[SELECT RESULT]")
            for row in result:
                print(row)

    def replicate(self, msg):
        replica_msg = json.dumps({
            "type": "REPLICA",
            "data": msg
        })

        for peer in list(self.peers):
            try:
                host, port = peer.split(":")
                s = socket.socket()
                s.connect((host, int(port)))
                s.sendall(replica_msg.encode())
                s.close()
            except:
                pass

    def server(self):
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen()

        print(f"[LISTENING] {self.port}")

        while True:
            conn_sock, _ = s.accept()
            threading.Thread(target=self.handle_client, args=(conn_sock,)).start()

    def handle_client(self, conn_sock):
        data = conn_sock.recv(65536).decode()
        if not data:
            conn_sock.close()
            return

        response = self.handle_message(data)

        if response:
            conn_sock.sendall(response.encode())

        conn_sock.close()

    def client(self):
        while True:
            cmd = input(">> ")

            if cmd.startswith("insert"):
                _, id, sender, receiver, amount = cmd.split()

                msg = {
                    "type": "INSERT",
                    "data": {
                        "id": int(id),
                        "sender": sender,
                        "receiver": receiver,
                        "amount": float(amount)
                    }
                }

                self.handle_message(json.dumps(msg))

            elif cmd == "select":
                self.handle_message(json.dumps({"type": "SELECT"}))

    def run(self):
        self.conectar_servico_nomes()
        threading.Thread(target=self.server, daemon=True).start()
        # avisa os peers antigos da sua entrada (restaura o "todos conhecem todos")
        self.notificar_peers()
        self.sync_with_oldest()
        self.client()


if __name__ == "__main__":
    peer = Peer()
    peer.run()
