#include <iostream>
#include <sw/redis++/redis++.h>

using namespace std;
using namespace sw::redis;

bool pare;

void trataMensagem(std::string channel, std::string msg) {
	if (msg == ":q") pare = true;
	cout << "mensagem recebida: \"" << msg  << "\" do canal \"" << channel << "\"" << endl;
}

void trataMensagem2(string channel, string msg) {
	if (msg == ":q") pare = true;
	cout << "Recebeu uma mensagem!!\n" << endl;
}

int main() {

	string senha;

	cout << "Digite a senha: ";
	cin >> senha;

	ConnectionOptions opts;
	opts.host = "containers-us-west-50.railway.app";
	opts.port = 6310;
	opts.password = senha;

	auto redis = Redis(opts);

	auto sub = redis.subscriber();

	sub.on_message(trataMensagem);

	sub.subscribe("todos");

	pare = false;
	while (!pare) {
		try {
			sub.consume();
		} catch (const Error &err) {
			cout << err.what() << endl;
		}
	}

	cout << "Programa finalizado\n";

	return 0;
}