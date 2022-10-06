#include <iostream>
#include <sw/redis++/redis++.h>

using namespace std;
using namespace sw::redis;

int main() {

	bool quit = false;
	string mensagem;
	ConnectionOptions opts;
	opts.host = "containers-us-west-50.railway.app";
	opts.port = 6310;
	opts.password = "eresgQzk0RVX75hEXx79";

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