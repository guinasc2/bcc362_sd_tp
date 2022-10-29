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

#define topicoArmazenamento "armazenamento"
#define topicoProcessamento "processamento"

class Cliente {

public:
	ConnectionOptions _build_options(string host, int port, string password) {
		ConnectionOptions opts;
		opts.host = host;
		opts.port = port;
		opts.password = password;
		return opts;
	}

	ConnectionOptions connection_options_;
	Redis redis_;
	Subscriber sub;

	Cliente(string host = "127.0.0.1", int port = 6379, string password = "") : 
		connection_options_(_build_options(host, port, password)),
		redis_(connection_options_), sub(redis_.subscriber()) {
			sub.on_message(tratarMensagem);
			srand(time(NULL));
		}

	void subscribe(string canal) {
		sub.subscribe(canal);
	}

	void consumeLoop() {
		while (true) {
			try {
				sub.consume();
			} catch (const Error &err) {
				cout << err.what() << endl;
			}
		}
	}

	void startPub() {
		string message = "";

		while (message != "!q") {
			getline(cin, message);

			if (message == "!q") {
				cout << "Adeus!" << endl;
			} else if (message == "!help" || message == "!h") {
				cout << "!read <position> | !r <position>" << endl;
				cout << "\tLê o que está na posição <position> nos servidores de armazenamento." << endl;
				cout << "!write <position> <value>" << endl;
				cout << "\tEscreve <value> na posição <position> nos servidores de armazenamento." << endl;

				cout << "Os servidores de processamento não estão prontos!!!!!!!!!" << endl;
			} else {
				makeMessage(message);
			}
		}
	}

	void startSub() {
		consumeLoop();
	}

	void start() {
		thread subscriber(&Cliente::startSub, this);
		this_thread::sleep_for(chrono::milliseconds(1000));
		thread publisher(&Cliente::startPub, this);

		subscriber.join();
		publisher.join();
	}

	void makeMessage(string mensagem) {
		string m = "", aux;
		size_t token, token2;
		int posicao, value;
		if (mensagem.find("!r") != std::string::npos || mensagem.find("!read") != std::string::npos) {
			token = mensagem.find_first_of(" ", 0);
			aux = mensagem.substr(token+1, mensagem.size());
			posicao = stoi(aux);
			if (posicao < 0 || posicao >= 100) {
				cout << "\tPosição inválida!\n\n";
				return;
			}
			m = "read," + aux;
			redis_.publish(topicoArmazenamento, m);
		} else if (mensagem.find("!w") != std::string::npos || mensagem.find("!write") != std::string::npos) {
			token = mensagem.find_first_of(" ", 0);
			token2 = mensagem.find_first_of(" ", token+1);
			aux = mensagem.substr(token+1, token2);
			posicao = stoi(aux);
			if (posicao < 0 || posicao >= 100) {
				cout << "\tPosição inválida!\n\n";
				return;
			}
			m = "write," + to_string(posicao);
			aux = mensagem.substr(token2+1, mensagem.size());
			m += "," + aux;
			redis_.publish(topicoArmazenamento, m);
		} else {
			cout << "\n\tComando desconhecido!\n\n";
		}
	}

	static void tratarMensagem(string canal, string mensagem) {
		if (canal.compare(topicoArmazenamento) == 0) {
			cout << "Mensagem dos servidores de armazenamento:\n\t";
		} else if (canal.compare(topicoProcessamento) == 0) {
			cout << "Mensagem dos servidores de processamento:\n\t";
		} else {
			cout << "Mensagem de um canal desconhecido!\n\t" << endl;
		}
		cout << mensagem << endl;
	}

};

int main() {

	int delay = 1;
	string senha = "";


	// cout << "Digite a senha do servidor: ";
	// cin >> senha;
	cout << "Seja bem vindo! Use \"!help\" para ver os comandos!" << endl;
	
	this_thread::sleep_for(chrono::milliseconds(delay * 1000));

	Cliente peer("127.0.0.1", 6379, senha);
	peer.subscribe(topicoArmazenamento);
	peer.subscribe(topicoProcessamento);

	peer.start();

	cout << "\n\nCliente encerrou." << endl;

	return 0;
}