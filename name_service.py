import zmq
import json
from config import NS_PORT


class ServicoDeNomes:
    def __init__(self):
        self.porta = NS_PORT
        self.registros = {} 

    def rodar(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{self.porta}")

        print(f"Servidor de Nomes iniciado na porta {self.porta}...")

        while True:

            resposta = {}
            try:
                mensagem = socket.recv_json()
                operacao = mensagem["op"]

                if operacao == "bind":
                    nome = mensagem["name"]
                    endereco = mensagem["address"]

                    if nome not in self.registros:
                        self.registros[nome] = {}

                    self.registros[nome]["address"] = endereco
                    resposta = {"status": "ok"}
                    print("Adicionou IP:", nome, endereco)

                elif operacao == "register":
                    nome = mensagem["name"]
                    tipo = mensagem["type"]
                    if nome in self.registros:
                        self.registros[nome]["type"] = tipo
                        resposta = {"status": "ok"}
                        print("Registrou tipo:", nome, tipo)
                    else:
                        resposta = {"status": "erro", "message": "nome nao registrado"}

                elif operacao == "lookup":
                    nome = mensagem["name"]
                    if nome in self.registros:
                        resposta = {"status": "ok", "address": self.registros[nome]["address"]}
                    else:
                        resposta = {"status": "erro", "message": "nome nao encontrado"}

                elif operacao == "unbind":
                    nome = mensagem["name"]
                    if nome in self.registros:
                        del self.registros[nome]
                        resposta = {"status": "ok"}
                        print("Removeu:", nome)
                    else:
                        resposta = {"status": "erro", "message": "nome nao encontrado"}

                elif operacao == "discover":
                    tipo_procurado = mensagem["type"]
                    lista_peers = []

                    for n, info in self.registros.items():
                        if info.get("type") == tipo_procurado:
                            lista_peers.append({"name": n, "address": info["address"]})

                    resposta = {"status": "ok", "result": lista_peers}

                else:
                    resposta = {"status": "erro", "message": "operacao desconhecida"}

            except Exception as e:
                resposta = {"status": "erro", "message": str(e)}

            socket.send_json(resposta)

if __name__ == "__main__":
    servidor = ServicoDeNomes()
    servidor.rodar()
