#include <iostream>
#include <sw/redis++/redis++.h>

using namespace std;
using namespace sw::redis;

bool pare;

void trataMensagem(std::string channel, std::string msg) {
	if (msg == ":q") pare = true;
	cout << "mensagem recebida: \"" << msg  << "\" do canal \"" << channel << "\"" << endl;
}

int main() {

	ConnectionOptions opts;
	opts.host = "127.0.0.1";
	opts.port = 6379;

	auto redis = Redis(opts);

	auto sub = redis.subscriber();

	sub.on_message(trataMensagem);

	sub.subscribe("todos");

	pare = false;
	while (!pare) {
		try {
			sub.consume();
		} catch (const Error &err) {
			cout << "deu ruim" << endl;
		}

	}

	return 0;
}