#include <iostream>
#include <sw/redis++/redis++.h>

using namespace std;
using namespace sw::redis;

// class Pub {
// 	auto redis;
// }

int main() {

	int quit = 0;
	string mensagem;
	ConnectionOptions opts;
	opts.host = "127.0.0.1";
	opts.port = 6379;

	auto redis = Redis(opts);

	while (!quit) {
		cout << "Digite sua mensagem (:q para sair)\n";
		getline(cin, mensagem);

		redis.publish("todos", mensagem);
		
		if (mensagem == ":q") {
			quit = !quit;
		}
	}

	cout << "Programa finalizado\n";

	return 0;
}