#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include "PubSub.cpp"

using namespace std;

bool podeComecar = false;

void comecarPeer(PubSub peer) {
    while (!podeComecar);
    peer.start();
}

int main() {

    int quantPeers, requests;

    cout << "Digite o número de peers e a quantidade de requests: ";
    cin >> quantPeers >> requests;

    vector<PubSub> peers(quantPeers);
    for (int i = 0; i < quantPeers; i++) {
        peers[i] = PubSub("127.0.0.1", 6379, i+1, requests);
    }

    vector<thread> threadList(quantPeers);
    for (int i = 0; i < quantPeers; i++) {
        threadList[i] = thread(comecarPeer, peers[i]);
    }

    podeComecar = true;

    for (int i = 0; i < quantPeers; i++) {
        threadList[i].join();
    }

    cout << "Main finalizado." << endl;

    return 0;
}