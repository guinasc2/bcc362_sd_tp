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

#define topicoCliente "armazenamento"
#define topicoServidores "armazenamentoInterno"
#define numServidores 11

typedef struct {
	int id;
	string conteudo;
} Mensagem;

typedef struct {
	string conteudo;
	bool valido;
} Data;

typedef struct {
	int idServidor;
	int tentativa;
} Try;

typedef struct {
	Try tentativas[numServidores];
	size_t size;
} TryQueue;

typedef struct {
	int posicao;
	string conteudo;
} Escrita;

typedef struct {
	int id;
	bool seguro;
} Server;

typedef struct {
	Server servidores[numServidores];
	size_t size;
} ServerList;

class ServidorArmazenamento {

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
	ServerList serverList;
	
	static int id, idPrimario, posicaoLeitura;
	static deque<Mensagem> logMensagens;
	static TryQueue ordem;
	static bool requestIdPrimario, temPrimario, requestFromClient, responseFromPrimario;
	static Escrita escrita;

	ServidorArmazenamento(string host = "127.0.0.1", int port = 6379, string password = "", int idPeer = 0) : 
		connection_options_(_build_options(host, port, password)),
		redis_(connection_options_), sub(redis_.subscriber()) {
			sub.on_message(tratarMensagem);
			id = idPeer;
			idPrimario = -1;
			requestIdPrimario = false;
			temPrimario = false;
			requestFromClient = false;
			responseFromPrimario = false;
			posicaoLeitura = -1;
			escrita.posicao = -1;
			ordem.size = 0;
			serverList.size = 0;
			for (int i = 0; i < 100; i++) array[i].valido = false;
			for (int i = 0; i < numServidores; i++) serverList.servidores[i].seguro = false;
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
			serverList.servidores[i].id = ordem.tentativas[i].idServidor;
			serverList.servidores[i].seguro = true;
			serverList.size++;
			cout << "DEBUG -> " << ordem.tentativas[i].idServidor << ":" << ordem.tentativas[i].tentativa << endl;
		}

		temPrimario = true;
		message = "servidorPrimario:" + to_string(idPrimario);
		cout << "DEBUG -> " << message << endl;
	}

	void startSub() {
		consumeLoop();
	}

	void start() {
		cout << "Servidor de armazenamento " << id << " iniciado." << endl;

		thread subscriber(&ServidorArmazenamento::startSub, this);
		thread responder(&ServidorArmazenamento::responderMensagens, this);
		this_thread::sleep_for(chrono::milliseconds(1000));
		thread publisher(&ServidorArmazenamento::startPub, this);

		publisher.join();
		responder.join();
		subscriber.join();
	}

	string makeMessage(string mensagem) {
		string m = to_string(id) + ":" + mensagem;
		return m;
	}

	void responderMensagens() {
		Mensagem m;
		string message;
		while (true) {
			if (requestIdPrimario) {
				m = logMensagens[0];
				logMensagens.pop_front();
				if (serverList.size < numServidores) {
					serverList.servidores[serverList.size].id = m.id;
					serverList.servidores[serverList.size].seguro = true;
					serverList.size++;
				}
				message = "temPrimario!" + to_string(id);
				redis_.publish(topicoServidores, makeMessage(message));
				requestIdPrimario = false;
			}

			if (logMensagens.size() > 0 && id == idPrimario) {
				m = logMensagens[0];
				logMensagens.pop_front();
				message = m.conteudo;
				if (message.find("read") != std::string::npos) {
					cout << "DEBUG Mensagem de read novo primário -> " + message << endl;
					size_t token = message.find_first_of(",", 0);
					string posicaoString = message.substr(token+1, message.size());

					posicaoLeitura = stoi(posicaoString);
				} else if (message.find("write") != std::string::npos) {
					cout << "DEBUG Mensagem de write novo primário -> " + message << endl;
					size_t token = message.find_first_of(",", 0);
					size_t token2 = message.find_first_of(",", token+1);
					string posicaoString = message.substr(token+1, token2);
					string valorString = message.substr(token2+1, message.size());

					escrita.conteudo = valorString;
					escrita.posicao = stoi(posicaoString);
				}
			}

			if (posicaoLeitura >= 0 && id == idPrimario) {
				deque<string> respostas;
				deque<int> totalPorResposta;
				int totalRespostas = 0, posicaoMaioria;
				bool servidorOK, novaResposta;

				message = "response:";
				if (array[posicaoLeitura].valido)
					message += array[posicaoLeitura].conteudo;
				else
					message += "Empty";
				
				redis_.publish(topicoServidores, makeMessage("acceptRequestFromClient"));
				this_thread::sleep_for(chrono::milliseconds(3000)); // Espera uma resposta dos outros servidores

				respostas.push_back(message);
				totalPorResposta.push_back(1);
				for (int j = 0; j < logMensagens.size(); j++) {
					m = logMensagens[j];
					servidorOK = false;
					for (int i = 0; i < serverList.size; i++) {
						if (serverList.servidores[i].id == m.id && serverList.servidores[i].seguro) {
							servidorOK = true;
							break;
						}
					}
					if (servidorOK) {
						novaResposta = true;
						for (int i = 0; i < respostas.size(); i++) {
							if (respostas[i].compare(m.conteudo) == 0) {
								novaResposta = false;
								totalPorResposta[i]++;
								break;
							}
						}
						if (novaResposta) {
							respostas.push_back(m.conteudo);
							totalPorResposta.push_back(1);
						}
					}
				}

				posicaoMaioria = 0;
				for (int i = 1; i < totalPorResposta.size(); i++) {
					if (totalPorResposta[i] > totalPorResposta[posicaoMaioria]) {
						posicaoMaioria = i;
					}
				}

				if (message.compare(respostas[posicaoMaioria]) != 0) {
					idPrimario = -1;
					temPrimario = false;
				} else {
					for (int i = 0; i < logMensagens.size(); i++) {
						m = logMensagens[i];
						if (m.conteudo.compare(respostas[posicaoMaioria]) != 0) {
							for (int j = 0; j < serverList.size; i++) {
								if (m.id == serverList.servidores[j].id) {
									serverList.servidores[j].seguro = false;
									break;
								}
							}
						}
					}
				}

				while (!logMensagens.empty()) {
					logMensagens.pop_front();
				}

				message = respostas[posicaoMaioria];
				redis_.publish(topicoCliente, message);
				posicaoLeitura = -1;
			}

			if (escrita.posicao >= 0 && id == idPrimario) {
				deque<string> respostas;
				deque<int> totalPorResposta;
				int totalRespostas = 0, posicaoMaioria;
				bool servidorOK, novaResposta;
				
				array[escrita.posicao].conteudo = escrita.conteudo;
				array[escrita.posicao].valido = true;

				message = "response:" + array[escrita.posicao].conteudo;

				redis_.publish(topicoServidores, makeMessage("acceptRequestFromClient"));
				this_thread::sleep_for(chrono::milliseconds(3000)); // Espera uma resposta dos outros servidores

				respostas.push_back(message);
				totalPorResposta.push_back(1);
				for (int j = 0; j < logMensagens.size(); j++) {
					m = logMensagens[j];
					servidorOK = false;
					for (int i = 0; i < serverList.size; i++) {
						if (serverList.servidores[i].id == m.id && serverList.servidores[i].seguro) {
							servidorOK = true;
							break;
						}
					}
					if (servidorOK) {
						novaResposta = true;
						for (int i = 0; i < respostas.size(); i++) {
							if (respostas[i].compare(m.conteudo) == 0) {
								novaResposta = false;
								totalPorResposta[i]++;
								break;
							}
						}
						if (novaResposta) {
							respostas.push_back(m.conteudo);
							totalPorResposta.push_back(1);
						}
					}
				}

				posicaoMaioria = 0;
				for (int i = 1; i < totalPorResposta.size(); i++) {
					if (totalPorResposta[i] > totalPorResposta[posicaoMaioria]) {
						posicaoMaioria = i;
					}
				}

				if (message.compare(respostas[posicaoMaioria]) != 0) {
					idPrimario = -1;
					temPrimario = false;
				} else {
					for (int i = 0; i < logMensagens.size(); i++) {
						m = logMensagens[i];
						if (m.conteudo.compare(respostas[posicaoMaioria]) != 0) {
							for (int j = 0; j < serverList.size; i++) {
								if (m.id == serverList.servidores[j].id) {
									serverList.servidores[j].seguro = false;
									break;
								}
							}
						}
					}
				}

				while (!logMensagens.empty()) {
					logMensagens.pop_front();
				}

				message = respostas[posicaoMaioria];

				redis_.publish(topicoCliente, message);
				escrita.posicao = -1;
			}

			if (requestFromClient) {
				requestFromClient = false;
				this_thread::sleep_for(chrono::milliseconds(1500)); // Espera uma resposta do primário
				if (responseFromPrimario) {
					cout << "DEBUG -> Primário respondeu para o cliente" << endl;
					responseFromPrimario = false;
					if (posicaoLeitura >= 0) {
						message = "response:";
						if (array[posicaoLeitura].valido)
							message += array[posicaoLeitura].conteudo;
						else
							message += "Empty";

						// servidor deve avisar ao primário que recebeu a mensagem
						redis_.publish(topicoServidores, makeMessage(message));
						cout << "DEBUG Mensagem para o primário: " << message << endl;
						posicaoLeitura = -1;
					} else if (escrita.posicao >= 0) {
						array[escrita.posicao].conteudo = escrita.conteudo;
						array[escrita.posicao].valido = true;
						cout << "DEBUG -> secundário tá salvando isso na posição " << escrita.posicao <<": "<<array[escrita.posicao].conteudo<<" - "<<array[escrita.posicao].valido<<endl;

						// servidor deve avisar ao primário que recebeu a mensagem
						message = "response:" + array[escrita.posicao].conteudo;
						redis_.publish(topicoServidores, makeMessage(message));

						escrita.posicao = -1;
					}
				} else { // Não teve resposta, deve decidir um novo primário
					cout << "DEBUG -> Primário não respondeu para cliente" << endl;
					ordem.size = 0;
					serverList.size = 0;
					for (int i = 0; i < numServidores; i++) serverList.servidores[i].seguro = false;
					idPrimario = -1;
					temPrimario = false;
					startPub();
				}
			}
		}
	}

	static void insertTry(Try t) {
		if (ordem.size == numServidores) return;

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
					logMensagens.push_back(msg);
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
				responseFromPrimario = true;
				Mensagem m = logMensagens[0];
				logMensagens.pop_front();
				cout << "DEBUG acceptRequestFromClient -> " << m.conteudo << endl;
				if (m.conteudo.find("read") != std::string::npos) {
					size_t token = m.conteudo.find_first_of(",", 0);
					string posicaoString = m.conteudo.substr(token+1, mensagem.size());

					posicaoLeitura = stoi(posicaoString);
				} else if (m.conteudo.find("write") != std::string::npos) {
					size_t token = m.conteudo.find_first_of(",", 0);
					size_t token2 = m.conteudo.find_first_of(",", token+1);
					string posicaoString = m.conteudo.substr(token+1, token2);
					string valorString = m.conteudo.substr(token2+1, mensagem.size());

					escrita.conteudo = valorString;
					escrita.posicao = stoi(posicaoString);
				}
			} else if (msg.conteudo.find("response:", 0)  != std::string::npos && id == idPrimario) {
				cout << "DEBUG -> resposta de um servidor secundário" << endl;
				logMensagens.push_back(msg);
			}
		} else if (canal.compare(topicoCliente) == 0) {
			if (id == idPrimario && mensagem.find("response:") == std::string::npos) {
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

					escrita.conteudo = valorString;
					escrita.posicao = stoi(posicaoString);
				}
			} else {
				if (mensagem.find("response:") == std::string::npos) {
					cout << "DEBUG Mensagem do cliente salva -> " + mensagem << endl;
					Mensagem msg;
					msg.id = 0;
					msg.conteudo = mensagem;
					logMensagens.push_back(msg);
					
					requestFromClient = true;
				}
			}
		} else {
			cout << "Essa mensagem nem devia ter chegado aqui e será descartada!" << endl;
		}
	}

};

int ServidorArmazenamento::id;
int ServidorArmazenamento::idPrimario;
deque<Mensagem> ServidorArmazenamento::logMensagens;
TryQueue ServidorArmazenamento::ordem;
bool ServidorArmazenamento::requestIdPrimario;
bool ServidorArmazenamento::temPrimario;
bool ServidorArmazenamento::requestFromClient;
bool ServidorArmazenamento::responseFromPrimario;
int ServidorArmazenamento::posicaoLeitura;
Escrita ServidorArmazenamento::escrita;

int main() {

	int delay = 2;
	string id, senha = "";

	// cout << "Digite a senha do servidor, o ID do servidor e aperte enter para começar (delay de " << delay << " segundos)" << endl;
	// cin >> senha >> id >> pedidos;

	cout << "Digite o ID do servidor e aperte enter para começar (delay de " << delay << " segundos)" << endl;
	cin >> id;
	
	this_thread::sleep_for(chrono::milliseconds(delay * 1000));

	ServidorArmazenamento peer("127.0.0.1", 6379, senha, stoi(id));
	peer.subscribe(topicoServidores);
	peer.subscribe(topicoCliente);

	peer.start();

	cout << "\n\nServidorArmazenamento encerrou." << endl;

	return 0;
}