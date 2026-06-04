# Sistema P2P com Serviço de Nomes

Este projeto implementa um sistema distribuído de mensagens P2P. Para o descobrimento de nós na rede e resolução de endereços, o sistema utiliza um **Serviço de Nomes**, dispensando a necessidade de configurações de IPs estáticos nos nós.

A comunicação de registro e descobrimento com o Serviço de Nomes é feita utilizando o middleware **ZeroMQ**.

## Dependências

Para rodar o projeto, é necessário instalar as bibliotecas utilitárias, o banco de dados embarcado e o middleware de rede:

```bash
pip install pyzmq duckdb requests
```

## Como testar

### 1. Iniciar o Serviço de Nomes
O endereço padrão de escuta está definido globalmente no arquivo `config.py`.

```bash
python3 name_service.py
```

### 2. Iniciar os Peers (Nós P2P)
Abra novos terminais para rodar os peers. Você pode rodar quantos peers desejar na mesma máquina, desde que atribua portas diferentes para cada um.

**Terminal 2 (Primeiro Nó):**
```bash
python3 peer.py
> Porta deste peer: 5000
```
O primeiro peer a entrar na rede fará a requisição `discover` para o Serviço de Nomes, está sozinho e se assumirá como o nó mais velho.

**Terminal 3 (Segundo Nó):**
```bash
python3 peer.py
> Porta deste peer: 5001
```
O segundo peer consultará o Serviço de Nomes, descobrirá o endereço do primeiro nó e se conectará diretamente a ele para realizar a Sincronização de Banco de Dados.

### 3. Inserir e Consultar Transações
Com os nós rodando e o prompt disponível, você pode testar o multicast entre eles. 

No terminal de um Peer, insira os dados no formato `insert <id> <remetente> <destinatario> <valor>`:
```bash
>> insert 1 João Maria 150.50
```

No terminal de qualquer outro Peer, consulte a base de dados:
```bash
>> select
```
Você verá a transação sincronizada e salva localmente no banco de dados do peer que efetuou a consulta.
