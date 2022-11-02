#include <iostream>
#include <string>
#include <deque>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <thread>
#include <cmath>
#include <sw/redis++/redis++.h>

#define NUM_REQUESTS 100

using namespace std;
using namespace sw::redis;

#define topicoCliente "processamento"
#define topicoServidores "processamentoInterno"
#define numServidores 11

typedef struct
{
	int id;
	string conteudo;
} Mensagem;

typedef struct
{
	string conteudo;
	bool valido;
} Data;

typedef struct
{
	int idServidor;
	int tentativa;
} Try;

typedef struct
{
	Try tentativas[11];
	size_t size;
} TryQueue;

typedef struct
{
	int posicao;
	string conteudo;
} Escrita;

typedef struct
{
	bool pronto;
	string conteudo;
} Resposta;

typedef struct {
	int id;
	bool seguro;
} Server;

typedef struct {
	Server servidores[numServidores];
	size_t size;
} ServerList;


class ServidorProcessamento
{

public:
	ConnectionOptions _build_options(string host, int port, string password)
	{
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

	static int id, idPrimario;
	static deque<Mensagem> logMensagens;
	static TryQueue ordem;
	static bool requestIdPrimario, temPrimario, requestFromClient, responseFromPrimario;
	static Resposta resposta;

	ServidorProcessamento(string host = "127.0.0.1", int port = 6379, string password = "", int idPeer = 0) : connection_options_(_build_options(host, port, password)),
																											  redis_(connection_options_), sub(redis_.subscriber())
	{
		sub.on_message(tratarMensagem);
		id = idPeer;
		idPrimario = -1;
		requestIdPrimario = false;
		temPrimario = false;
		requestFromClient = false;
		responseFromPrimario = false;
		ordem.size = 0;
		serverList.size = 0;
		for (int i = 0; i < 100; i++)
			array[i].valido = false;
		for (int i = 0; i < numServidores; i++)
			serverList.servidores[i].seguro = false;
		srand(time(NULL));
	}

	void subscribe(string canal)
	{
		sub.subscribe(canal);
	}

	void consumeLoop()
	{
		while (true)
		{
			try
			{
				sub.consume();
			}
			catch (const Error &err)
			{
				cout << err.what() << endl;
			}
		}
	}

	void startPub()
	{
		int tentativa = rand() % 100;

		string message = "temPrimario?" + to_string(tentativa);
		cout << "DEBUG -> " << message << endl;
		redis_.publish(topicoServidores, makeMessage(message));
		this_thread::sleep_for(chrono::milliseconds(3000)); // dorme por 3 segundos enquanto decide o primário
		if (!temPrimario)
			idPrimario = ordem.tentativas[0].idServidor;
		cout << "DEBUG -> ordem.size = " << ordem.size << endl;
		for (int i = 0; i < ordem.size; i++)
		{
			cout << "DEBUG -> " << ordem.tentativas[i].idServidor << ":" << ordem.tentativas[i].tentativa << endl;
		}

		message = "servidorPrimario:" + to_string(idPrimario);
		cout << "DEBUG -> " << message << endl;
	}

	void startSub()
	{
		consumeLoop();
	}

	void start()
	{
		cout << "Servidor de armazenamento " << id << " iniciado." << endl;

		thread subscriber(&ServidorProcessamento::startSub, this);
		thread responder(&ServidorProcessamento::responderMensagens, this);
		this_thread::sleep_for(chrono::milliseconds(1000));
		thread publisher(&ServidorProcessamento::startPub, this);

		subscriber.join();
		responder.join();
		publisher.join();
	}

	string makeMessage(string mensagem)
	{
		string m = to_string(id) + ":" + mensagem;
		return m;
	}

	void responderMensagens()
	{
		Mensagem m;
		string message;
		while (true)
		{
			if (requestIdPrimario)
			{
				message = "temPrimario!" + to_string(id);
				redis_.publish(topicoServidores, makeMessage(message));
				requestIdPrimario = false;
			}

			if (logMensagens.size() > 0 && id == idPrimario)
			{
				m = logMensagens[0];
				logMensagens.pop_front();
				message = m.conteudo;
				cout << "DEBUG -> Servidor primário recebeu mensagem do cliente" << endl;
				if (m.conteudo.find("fib") != std::string::npos)
				{

					cout << "DEBUG Fibonacci -> " + m.conteudo << endl;
					size_t token = m.conteudo.find_first_of(",", 0);
					string valorString = m.conteudo.substr(token + 1, m.conteudo.size());

					message = to_string(fibonacci(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (m.conteudo.find("exp") != std::string::npos)
				{

					cout << "DEBUG Exponencial -> " + m.conteudo << endl;
					size_t token = m.conteudo.find_first_of(",", 0);
					string valorString = m.conteudo.substr(token + 1, m.conteudo.size());

					message = to_string(exp(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (m.conteudo.find("fat") != std::string::npos)
				{

					cout << "DEBUG Fatorial -> " + m.conteudo << endl;
					size_t token = m.conteudo.find_first_of(",", 0);
					string valorString = m.conteudo.substr(token + 1, m.conteudo.size());

					message = to_string(fatorial(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (m.conteudo.find("primo") != std::string::npos)
				{

					cout << "DEBUG É primo -> " + m.conteudo << endl;
					size_t token = m.conteudo.find_first_of(",", 0);
					string valorString = m.conteudo.substr(token + 1, m.conteudo.size());

					message = isPrimo(stoi(valorString));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else
				{

					cout << "DEBUG Não entendi -> " + m.conteudo << endl;

					resposta.conteudo = "Não entendi -> " + m.conteudo;
					resposta.pronto = true;
				}
			}

			if (resposta.pronto) // o pimario respondeu e tem a resposta pronta
			{
				message = "response:" + resposta.conteudo;

				deque<string> respostas;
				deque<int> totalPorResposta;
				int totalRespostas = 0, posicaoMaioria;
				bool servidorOK, novaResposta;

				redis_.publish(topicoServidores, makeMessage("acceptRequestFromClient"));
				this_thread::sleep_for(chrono::milliseconds(3000)); // Espera uma resposta dos outros servidores

				respostas.push_back(message);
				totalPorResposta.push_back(1);
				for (int j = 0; j < logMensagens.size(); j++)
				{
					m = logMensagens[j];
					servidorOK = false;
					for (int i = 0; i < serverList.size; i++)
					{
						if (serverList.servidores[i].id == m.id && serverList.servidores[i].seguro)
						{
							servidorOK = true;
							break;
						}
					}
					if (servidorOK)
					{
						novaResposta = true;
						for (int i = 0; i < respostas.size(); i++)
						{
							if (respostas[i].compare(m.conteudo) == 0)
							{
								novaResposta = false;
								totalPorResposta[i]++;
								break;
							}
						}
						if (novaResposta)
						{
							respostas.push_back(m.conteudo);
							totalPorResposta.push_back(1);
						}
					}
				}

				posicaoMaioria = 0;
				for (int i = 1; i < totalPorResposta.size(); i++)
				{
					if (totalPorResposta[i] > totalPorResposta[posicaoMaioria])
					{
						posicaoMaioria = i;
					}
				}

				if (message.compare(respostas[posicaoMaioria]) != 0)
				{ // resposta do primário é diferente da maioria
					idPrimario = -1;
					temPrimario = false;
				}
				else
				{ // caso contrário, é igual
					for (int i = 0; i < logMensagens.size(); i++)
					{
						m = logMensagens[i];
						if (m.conteudo.compare(respostas[posicaoMaioria]) != 0)
						{
							for (int j = 0; j < serverList.size; i++)
							{
								if (m.id == serverList.servidores[j].id)
								{
									serverList.servidores[j].seguro = false;
									break;
								}
							}
						}
					}
				}

				while (!logMensagens.empty())
				{
					logMensagens.pop_front();
				}

				redis_.publish(topicoCliente, message);
				resposta.pronto = false;
			}

			if (requestFromClient)
			{
				requestFromClient = false;
				this_thread::sleep_for(chrono::milliseconds(1500)); // Espera uma resposta do primário
				if (responseFromPrimario)
				{
					cout << "DEBUG -> Primário deu ok para os secundários" << endl;
					responseFromPrimario = false;

					message = "response:" + resposta.conteudo;

					// servidor deve avisar ao primário que recebeu a mensagem
					redis_.publish(topicoServidores, makeMessage(message));
					cout << "DEBUG Mensagem para o primário: " << message << endl;
				}
				else
				{ // Não teve resposta, deve decidir um novo primário
					cout << "DEBUG -> Primário não respondeu para cliente" << endl;
					ordem.size = 0;
					serverList.size = 0;
					for (int i = 0; i < numServidores; i++)
						serverList.servidores[i].seguro = false;
					idPrimario = -1;
					temPrimario = false;
					startPub();
				}
			}
		}
	}

	static void insertTry(Try t)
	{
		if (ordem.size == 11)
			return;

		int i;
		for (i = ordem.size - 1; i >= 0; i--)
		{
			if (ordem.tentativas[i].tentativa < t.tentativa)
				ordem.tentativas[i + 1] = ordem.tentativas[i];
			else
				break;
		}

		ordem.tentativas[i + 1] = t;
		ordem.size++;
	}

	static void tratarMensagem(string canal, string mensagem)
	{
		cout << "DEBUG Nova mensagem -> " << canal << ": " << mensagem << endl;
		string message;

		if (canal.compare(topicoServidores) == 0)
		{ // conversa interna entre os servidores
			size_t token = mensagem.find_first_of(":", 0);
			string idString = mensagem.substr(0, token);
			string conteudo = mensagem.substr(token + 1, mensagem.size());

			Mensagem msg;
			msg.id = stoi(idString);
			msg.conteudo = conteudo;

			if (msg.conteudo.find("temPrimario?", 0) != std::string::npos)
			{
				if (id == idPrimario)
				{ // servidor primário recebeu essa mensagem
					requestIdPrimario = true;
				}
				else if (idPrimario == -1)
				{
					Try t;
					token = msg.conteudo.find_first_of("?", 0);
					conteudo = msg.conteudo.substr(token + 1, msg.conteudo.size());
					t.idServidor = msg.id;
					t.tentativa = stoi(conteudo);
					insertTry(t);
				}
			}
			else if (msg.conteudo.find("temPrimario!") != std::string::npos && idPrimario == -1)
			{
				token = msg.conteudo.find_first_of("!", 0);
				conteudo = msg.conteudo.substr(token + 1, msg.conteudo.size());
				temPrimario = true;
				idPrimario = stoi(conteudo);
			}
			else if (msg.conteudo.find("acceptRequestFromClient", 0) != std::string::npos && id != idPrimario)
			{ // posso responder e não sou primario
				responseFromPrimario = true;
				Mensagem m = logMensagens[0];
				logMensagens.pop_front();

				cout << "DEBUG acceptRequestFromClient -> " << m.conteudo << endl;
				if (mensagem.find("fib") != std::string::npos)
				{

					cout << "DEBUG Fibonacci -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = to_string(fibonacci(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (mensagem.find("exp") != std::string::npos)
				{

					cout << "DEBUG Exponencial -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = to_string(exp(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (mensagem.find("fat") != std::string::npos)
				{

					cout << "DEBUG Fatorial -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = to_string(fatorial(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (mensagem.find("primo") != std::string::npos)
				{

					cout << "DEBUG É primo -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = isPrimo(stoi(valorString));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else
				{

					cout << "DEBUG Não entendi -> " + mensagem << endl;

					resposta.conteudo = "Não entendi -> " + mensagem;
					resposta.pronto = true;
				}
			}
			else if (msg.conteudo.find("response:", 0) != std::string::npos && id == idPrimario)
			{
				cout << "DEBUG -> resposta de um servidor secundário" << endl;
				logMensagens.push_back(msg);
			}
		}
		else if (canal.compare(topicoCliente) == 0)
		{ // mensagem do cliente
			if (id == idPrimario && mensagem.find("response:") == std::string::npos)
			{ // açoes do servidor primário

				cout << "DEBUG -> Servidor primário recebeu mensagem do cliente" << endl;
				if (mensagem.find("fib") != std::string::npos)
				{

					cout << "DEBUG Fibonacci -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = to_string(fibonacci(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (mensagem.find("exp") != std::string::npos)
				{

					cout << "DEBUG Exponencial -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = to_string(exp(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (mensagem.find("fat") != std::string::npos)
				{

					cout << "DEBUG Fatorial -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = to_string(fatorial(stoi(valorString)));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else if (mensagem.find("primo") != std::string::npos)
				{

					cout << "DEBUG É primo -> " + mensagem << endl;
					size_t token = mensagem.find_first_of(",", 0);
					string valorString = mensagem.substr(token + 1, mensagem.size());

					message = isPrimo(stoi(valorString));
					resposta.conteudo = message;
					resposta.pronto = true;
				}
				else
				{

					cout << "DEBUG Não entendi -> " + mensagem << endl;

					resposta.conteudo = "Não entendi -> " + mensagem;
					resposta.pronto = true;
				}
			}
			else
			{
				if (mensagem.find("response:") == std::string::npos)
				{
					cout << "DEBUG Mensagem do cliente salva -> " + mensagem << endl;
					Mensagem msg;
					msg.id = 0;
					msg.conteudo = mensagem;
					logMensagens.push_back(msg);

					requestFromClient = true;
				}
			}
		}
	}
	
	static long fibonacci(int n)
	{
		int cont;
		long int x = 0, z = 1;

		if (n % 2 == 0)
		{
			for (cont = 2; cont != n; cont = cont + 2)
			{
				x = x + z;
				z = x + z;
			}
			return z;
		}
		else
		{
			for (cont = 1; cont != n; cont = cont + 2)
			{
				x = x + z;
				z = x + z;
			}
			return x;
		}
	}

	static long fatorial(int n)
	{
		int aux = 1, prod = 1;

		while (aux <= n)
		{
			prod *= aux;
			aux++;
		}
		return prod;
	}

	static string isPrimo(int num)
	{
		int aux;
		bool primo = true;

		for (aux = 2; aux <= sqrt(num); aux++)
		{
			if (num % aux == 0)
			{
				primo = false;
				break;
			}
		}

		if (primo)
			return "É primo";
		else
			return "Não é primo";
	}
};

int ServidorProcessamento::id;
int ServidorProcessamento::idPrimario;
deque<Mensagem> ServidorProcessamento::logMensagens;
TryQueue ServidorProcessamento::ordem;
bool ServidorProcessamento::requestIdPrimario;
bool ServidorProcessamento::temPrimario;
bool ServidorProcessamento::requestFromClient;
bool ServidorProcessamento::responseFromPrimario;
Resposta ServidorProcessamento::resposta;

int main()
{

	int delay = 2;
	string id, senha = "";

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