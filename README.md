# bcc362_sd_tp2
Trabalho Prático 2 da disciplina BCC362 - Sistemas Distribuídos

Feito em c++ usando Redis e a biblioteca redis-plus-plus (https://github.com/sewenew/redis-plus-plus) no Ubuntu WSL.

Para compilar:
  g++ PubSub.cpp -o PubSub -std=c++17 -lredis++ -lhiredis -pthread

Para executar
  ./PubSub
