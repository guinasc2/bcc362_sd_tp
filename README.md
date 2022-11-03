# bcc362_sd_tp2
Trabalho Prático 2 da disciplina BCC362 - Sistemas Distribuídos

Feito em c++ usando Redis e a biblioteca redis-plus-plus (https://github.com/sewenew/redis-plus-plus) no Ubuntu WSL.

Para compilar:
  TP2:
    g++ PubSub.cpp -o PubSub -std=c++17 -lredis++ -lhiredis -pthread
    g++ pub.cpp -o pub -std=c++17 -lredis++ -lhiredis -pthread
    g++ sub.cpp -o sub -std=c++17 -lredis++ -lhiredis -pthread

  TP3:
    g++ tp3/servidorArmazenamento.cpp -o servidorArmazenamento -std=c++17 -lredis++ -lhiredis -pthread
    g++ tp3/servidorProcessamento.cpp -o servidorProcessamento -std=c++17 -lredis++ -lhiredis -pthread
    g++ tp3/cliente.cpp -o cliente -std=c++17 -lredis++ -lhiredis -pthread

Para executar
  ./PubSub

uDKGJbxFcy3qfNKE4Tfn