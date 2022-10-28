#include <iostream>
#include <string>
#include <deque>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <thread>
#include <sw/redis++/redis++.h>

#define NUM_REQUESTS 100

using namespace std;
using namespace sw::redis;

typedef struct {
	int id;
	string conteudo;
} Mensagem;

class PubSub {

public:
	ConnectionOptions _build_options(string host, int port, string password) {
		ConnectionOptions opts;
		opts.host = host;
		opts.port = port;
		opts.password = password;
		return opts;
	}

	int id, tempoRegiaoCritica, maxRequests, numRequests, numReleases;
	ConnectionOptions connection_options_;
	Redis redis_;
	Subscriber sub;
	static deque<Mensagem> logMensagens;
	bool podePedir;

	PubSub(string host = "127.0.0.1", int port = 6379, string password = "", int idPeer = 0, int requests = NUM_REQUESTS) : 
		connection_options_(_build_options(host, port, password)),
		redis_(connection_options_), sub(redis_.subscriber()) {
			sub.on_message(tratarMensagem);
			podePedir = true;
			id = idPeer;
			tempoRegiaoCritica = 3;
			numRequests = 0;
			numReleases = 0;
			maxRequests = requests;
			srand(time(NULL));
		}

	void subscribe(string canal) {
		sub.subscribe(canal);
	}

	void consumeLoop() {
		while (numReleases < maxRequests) {
			try {
				sub.consume();
				if (logMensagens.size() > 0) {
					if (logMensagens.front().id == id) {
						logMensagens.pop_front();
						accessRegiaoCritica();
						releaseAccess();
						numReleases++;
					}
				}
			} catch (const Error &err) {
				cout << err.what() << endl;
			}
		}
	}

	void requestAccess() {
		string mensagem = to_string(id) + ":requestAccess";
		redis_.publish("todos", mensagem);
		podePedir = false;
	}

	void releaseAccess() {
		string mensagem = to_string(id) + ":releaseAccess";
		redis_.publish("todos", mensagem);
		podePedir = true;
	}

	void accessRegiaoCritica() {
		int tempo = (rand() % tempoRegiaoCritica) + 1;
		cout << id << ": Estou usando a região crítica" << endl;
		this_thread::sleep_for(chrono::milliseconds(tempo * 1000));
	}

	void startPub() {
		while (numRequests < maxRequests) {
			if (podePedir) {
				requestAccess();
				numRequests++;
			}
		} 
	}

	void startSub() {
		consumeLoop();
	}

	void start() {
		cout << "PubSub " << id << " iniciado." << endl;
		
		thread subscriber(&PubSub::startSub, this);
		this_thread::sleep_for(chrono::milliseconds(1000));
		thread publisher(&PubSub::startPub, this);

		publisher.join();
		subscriber.join();
	}

	static void tratarMensagem(string canal, string mensagem) {
		size_t token = mensagem.find_first_of(":", 0);
		string idString = mensagem.substr(0, token);
		string conteudo = mensagem.substr(token+1, mensagem.size());

		Mensagem msg;
		msg.id = stoi(idString);
		msg.conteudo = conteudo;

		cout << "Mensagem recebida: " << mensagem << endl;

		if (msg.conteudo == "requestAccess") {
			logMensagens.push_back(msg);
			cout << "Mensagem recebida colocada na fila: " << mensagem << endl;
		} else if (msg.conteudo == "releaseAccess") {
			if (logMensagens.front().id == msg.id) {
				cout << "Mensagem tirada da fila: " << mensagem << endl;
				logMensagens.pop_front();
			}
			cout << "Mensagem recebida de release: " << mensagem << endl;
		} else {
			cout << "Mensagem desconhecida: " << mensagem << endl;
		}
	}

};

deque<Mensagem> PubSub::logMensagens;

int main() {

	string id, pedidos, senha;

	cout << "Digite a senha do servidor, o ID do peer, o número de acesso à região crítica e aperte enter para começar (delay de 2 segundos)" << endl;
	cin >> senha >> id >> pedidos;
	
	this_thread::sleep_for(chrono::milliseconds(2000));

	PubSub peer("containers-us-west-50.railway.app", 6310, senha, stoi(id), stoi(pedidos));
	peer.subscribe("todos");

	peer.start();

	cout << "\n\nPubSub encerrou." << endl;

	return 0;
}


// bool podeComecar = false;

// int main() {

//     int quantPeers, requests;

//     cout << "Digite o número de peers e a quantidade de requests: ";
//     cin >> quantPeers >> requests;

//     vector<PubSub> peers(quantPeers);
//     for (int i = 0; i < quantPeers; i++) {
//         peers[i] = PubSub("127.0.0.1", 6379, i+1, requests);
//     }

//     vector<thread> threadList(quantPeers);
//     for (int i = 0; i < quantPeers; i++) {
//         threadList[i] = thread(peers[i].start);
//     }

//     podeComecar = true;

//     for (int i = 0; i < quantPeers; i++) {
//         threadList[i].join();
//     }

//     cout << "Main finalizado." << endl;

//     return 0;
// }