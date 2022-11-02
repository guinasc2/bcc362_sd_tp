all: cliente armazenamento processamento

cliente: tp3/cliente.cpp
	g++ tp3/cliente.cpp -o cliente -std=c++17 -lredis++ -lhiredis -pthread
	
armazenamento: tp3/servidorArmazenamento.cpp
	g++ tp3/servidorArmazenamento.cpp -o servidorArmazenamento -std=c++17 -lredis++ -lhiredis -pthread
	
processamento: tp3/servidorProcessamento.cpp
	g++ tp3/servidorProcessamento.cpp -o servidorProcessamento -std=c++17 -lredis++ -lhiredis -pthread