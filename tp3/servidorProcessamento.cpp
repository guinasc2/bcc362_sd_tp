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

typedef struct {
	int conteudo;
	bool valido;
} Data;

typedef struct {
	int idServidor;
	int tentativa;
} Try;

typedef struct {
	Try tentativas[11];
	size_t size;
} TryQueue;

typedef struct {
	int posicao;
	int conteudo;
} Escrita;

#define topicoCliente "processamento"
#define topicoServidores "processamentoInterno"

class ServidorProcessamento {

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
	Data array[100];
	
	static int id, idPrimario, posicaoLeitura;
	static deque<Mensagem> logMensagens;
	static TryQueue ordem;
	static bool requestIdPrimario, temPrimario;
	static Escrita escrita;

	ServidorProcessamento(string host = "127.0.0.1", int port = 6379, string password = "", int idPeer = 0) : 
		connection_options_(_build_options(host, port, password)),
		redis_(connection_options_), sub(redis_.subscriber()) {
			sub.on_message(tratarMensagem);
			id = idPeer;
			idPrimario = -1;
			requestIdPrimario = false;
			temPrimario = false;
			posicaoLeitura = -1;
			escrita.posicao = -1;
			for (int i = 0; i < 100; i++) array[i].valido = false;
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
		int tentativa = rand() % 100;

		string message = "temPrimario?" + to_string(tentativa);
		cout << "DEBUG -> " << message << endl;
		redis_.publish(topicoServidores, makeMessage(message));
		this_thread::sleep_for(chrono::milliseconds(3000)); // dorme por 3 segundos enquanto decide o primário
		if (!temPrimario)
			idPrimario = ordem.tentativas[0].idServidor;
		cout << "DEBUG -> ordem.size = " << ordem.size << endl;
		for (int i = 0; i < ordem.size; i++) {
			cout << "DEBUG -> " << ordem.tentativas[i].idServidor << ":" << ordem.tentativas[i].tentativa << endl;
		}

		message = "servidorPrimario:" + to_string(idPrimario);
		cout << "DEBUG -> " << message << endl;
	}

	void startSub() {
		consumeLoop();
	}

	void start() {
		cout << "Servidor de armazenamento " << id << " iniciado." << endl;

		thread subscriber(&ServidorProcessamento::startSub, this);
		thread responder(&ServidorProcessamento::responderMensagens, this);
		this_thread::sleep_for(chrono::milliseconds(1000));
		thread publisher(&ServidorProcessamento::startPub, this);

		subscriber.join();
		responder.join();
		publisher.join();
	}

	string makeMessage(string mensagem) {
		string m = to_string(id) + ":" + mensagem;
		return m;
	}

	void responderMensagens() {
		string message;
		while (true) {
			if (requestIdPrimario) {
				message = "temPrimario!" + to_string(id);
				redis_.publish(topicoServidores, makeMessage(message));
				requestIdPrimario = false;
			}

			if (posicaoLeitura >= 0 && id == idPrimario) {
				if (array[posicaoLeitura].valido)
					message = to_string(array[posicaoLeitura].conteudo);
				else
					message = "Empty";
				
				redis_.publish(topicoCliente, message);
				posicaoLeitura = -1;
			}

			if (escrita.posicao >= 0) {
				array[escrita.posicao].conteudo = escrita.conteudo;
				array[escrita.posicao].valido = true;
				escrita.posicao = -1;

				if (id == idPrimario) {	
					redis_.publish(topicoServidores, makeMessage("acceptRequestFromClient"));
					redis_.publish(topicoCliente, "Escrita realizada com sucesso");
				}
			}
		}
	}

	static void insertTry(Try t) {
		if (ordem.size == 11) return;

		int i;
		for (i = ordem.size - 1; i >= 0; i--) {
			if (ordem.tentativas[i].tentativa < t.tentativa)
				ordem.tentativas[i+1] = ordem.tentativas[i];
			else
				break;
		}

		ordem.tentativas[i+1] = t;
		ordem.size++;
	}

	static void tratarMensagem(string canal, string mensagem) {
		cout << "DEBUG Nova mensagem -> " << canal << ": " << mensagem << endl;

		if (canal.compare(topicoServidores) == 0) {
			size_t token = mensagem.find_first_of(":", 0);
			string idString = mensagem.substr(0, token);
			string conteudo = mensagem.substr(token+1, mensagem.size());

			Mensagem msg;
			msg.id = stoi(idString);
			msg.conteudo = conteudo;

			if (msg.conteudo.find("temPrimario?", 0) != std::string::npos) {
				if (id == idPrimario) { // servidor primário recebeu essa mensagem
					requestIdPrimario = true;
				} else if (idPrimario == -1) {
					Try t;
					token = msg.conteudo.find_first_of("?", 0);
					conteudo = msg.conteudo.substr(token+1, msg.conteudo.size());
					t.idServidor = msg.id;
					t.tentativa = stoi(conteudo);
					insertTry(t);
				}
			} else if (msg.conteudo.find("temPrimario!") != std::string::npos && idPrimario == -1) {
				token = msg.conteudo.find_first_of("!", 0);
				conteudo = msg.conteudo.substr(token+1, msg.conteudo.size());
				temPrimario = true;
				idPrimario = stoi(conteudo);
			} else if (msg.conteudo.find("acceptRequestFromClient", 0)  != std::string::npos && id != idPrimario) {
				Mensagem m = logMensagens[0];
				logMensagens.pop_back();
				cout << "DEBUG acceptRequestFromClient -> " << m.conteudo << endl;
				size_t token = m.conteudo.find_first_of(",", 0);
				size_t token2 = m.conteudo.find_first_of(",", token+1);
				string posicaoString = m.conteudo.substr(token+1, token2);
				string valorString = m.conteudo.substr(token2+1, mensagem.size());

				escrita.conteudo = stoi(valorString);
				escrita.posicao = stoi(posicaoString);
			}
		} else if (canal.compare(topicoCliente) == 0) {
			if (id == idPrimario) {
				cout << "DEBUG -> Servidor primário recebeu mensagem do cliente" << endl;
				if (mensagem.find("read") != std::string::npos) {
					cout << "DEBUG Mensagem de read -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string posicaoString = mensagem.substr(token+1, mensagem.size());

					posicaoLeitura = stoi(posicaoString);
				} else if (mensagem.find("write") != std::string::npos) {
					cout << "DEBUG Mensagem de write -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					size_t token2 = mensagem.find_first_of(",", token+1);
					string posicaoString = mensagem.substr(token+1, token2);
					string valorString = mensagem.substr(token2+1, mensagem.size());

					escrita.conteudo = stoi(valorString);
					escrita.posicao = stoi(posicaoString);
				}
			} else {
				if (mensagem.find("write") != std::string::npos) {
					cout << "DEBUG Mensagem de write -> " + mensagem << endl;
					Mensagem msg;
					msg.id = 0;
					msg.conteudo = mensagem;
					logMensagens.push_back(msg);
				}
			}
		} else {
			cout << "Essa mensagem nem devia ter chegado aqui e será descartada!" << endl;
		}
	}

};

int ServidorProcessamento::id;
int ServidorProcessamento::idPrimario;
deque<Mensagem> ServidorProcessamento::logMensagens;
TryQueue ServidorProcessamento::ordem;
bool ServidorProcessamento::requestIdPrimario;
bool ServidorProcessamento::temPrimario;
int ServidorProcessamento::posicaoLeitura;
Escrita ServidorProcessamento::escrita;

int main() {

	int delay = 2;
	string id, senha = "";

	// cout << "Digite a senha do servidor, o ID do servidor e aperte enter para começar (delay de " << delay << " segundos)" << endl;
	// cin >> senha >> id >> pedidos;

	cout << "Digite o ID do servidor e aperte enter para começar (delay de " << delay << " segundos)" << endl;
	cin >> id;
	
	this_thread::sleep_for(chrono::milliseconds(delay * 1000));

	ServidorProcessamento peer("127.0.0.1", 6379, senha, stoi(id));
	peer.subscribe(topicoServidores);
	peer.subscribe(topicoCliente);

	peer.start();

	cout << "\n\nServidorProcessamento encerrou." << endl;

	return 0;
}