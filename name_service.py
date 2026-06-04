import zmq
import json

# Codigo baseado no tutorial oficial do ZeroMQ 

class ServicoDeNomes:
    def __init__(self):
        self.porta = 5555
        self.registros = {} 
    def rodar(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{self.porta}")
        
        print(f"Servidor de Nomes iniciado na porta {self.porta}...")

        while True:
          
            mensagem = socket.recv_json()
            operacao = mensagem["op"]
            
            resposta = {}

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
                self.registros[nome]["type"] = tipo
                resposta = {"status": "ok"}
                print("Registrou tipo:", nome, tipo)

            elif operacao == "discover":
                tipo_procurado = mensagem["type"]
                lista_peers = []
                
                for n, info in self.registros.items():
                    if info.get("type") == tipo_procurado:
                        lista_peers.append({"name": n, "address": info["address"]})
                
                resposta = {"status": "ok", "result": lista_peers}

            
            socket.send_json(resposta)

if __name__ == "__main__":
    servidor = ServicoDeNomes()
    servidor.rodar()
