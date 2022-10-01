#include <iostream>
#include <string>
#include <deque>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <thread>
#include <sw/redis++/redis++.h>

#define NUM_REQUESTS 3

using namespace std;
using namespace sw::redis;

void tratarMensagem(string canal, string mensagem);

typedef struct {
	int id;
	string conteudo;
} Mensagem;

class PubSub {

public:
	ConnectionOptions _build_options(string host, int port) {
		ConnectionOptions opts;
		opts.host = host;
		opts.port = port;
		return opts;
	}

	int id, tempoRegiaoCritica, numRequests;
	ConnectionOptions connection_options_;
	Redis redis_;
	Subscriber sub;
	deque<Mensagem> logMensagens;
	bool podePedir;

	PubSub(string host = "127.0.0.1", int port = 6379, int idPeer = 0) : 
		connection_options_(_build_options(host, port)),
		redis_(connection_options_), sub(redis_.subscriber()) {
			sub.on_message(tratarMensagem);
			podePedir = true;
			id = idPeer;
			tempoRegiaoCritica = 3;
			numRequests = 0;
			srand(time(NULL));
		}

	void subscribe(string canal) {
		sub.subscribe(canal);
	}

	void consumeLoop() {
		while (true) {
			try {
				sub.consume();
				if (logMensagens.size() > 0) {
					if (logMensagens.front().id == id) {
						logMensagens.pop_front();
						accessRegiaoCritica();
						releaseAccess();
					}
				}
				// cout << "Tentando ler" << endl;
			} catch (const Error &err) {
				cout << "Não foi possível consumir uma mensagem!" << endl;
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
		int tempo = 2;//(rand() % tempoRegiaoCritica) + 1;
		this_thread::sleep_for(chrono::milliseconds(tempo * 1000));
	}

	void startPub() {
		while (numRequests < NUM_REQUESTS) {
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
		cout << "PubSub iniciado." << endl;
		thread subscriber(&PubSub::startSub, this);
		this_thread::sleep_for(chrono::milliseconds(1000));
		thread publisher(&PubSub::startPub, this);

		publisher.join();
		subscriber.join();
	}

};

// deque<Mensagem> PubSub::logMensagens;
// int PubSub::id;
// bool PubSub::podePedir;

PubSub peer;

int main() {

	string enter;

	cout << "Digite o ID do peer e aperte enter para começar (delay de 2 segundos)" << endl;
	cin >> enter;
	
	this_thread::sleep_for(chrono::milliseconds(2000));

	peer = PubSub("127.0.0.1", 6379, stoi(enter));
	peer.subscribe("todos");

	peer.start();

	cout << "\n\nPubSub encerrou." << endl;

	return 0;
}

void tratarMensagem(string canal, string mensagem) {
	size_t token = mensagem.find_first_of(":", 0);
	string idString = mensagem.substr(0, token);
	string conteudo = mensagem.substr(token+1, mensagem.size());

	Mensagem msg;
	msg.id = stoi(idString);
	msg.conteudo = conteudo;

	cout << "Mensagem recebida: " << mensagem << endl;

	if (msg.conteudo == "requestAccess") {
		peer.logMensagens.push_back(msg);
		cout << "Mensagem recebida colocada na fila: " << mensagem << endl;
	} else if (msg.conteudo == "releaseAccess") {
		if (peer.logMensagens.front().id == msg.id) {
			cout << "Mensagem tirada da fila: " << mensagem << endl;
			peer.logMensagens.pop_front();
		}
		cout << "Mensagem recebida de release: " << mensagem << endl;
	} else {
		cout << "Mensagem desconhecida: " << mensagem << endl;
	}
}