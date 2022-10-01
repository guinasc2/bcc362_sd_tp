#include <iostream>
#include <sw/redis++/redis++.h>

using namespace std;
using namespace sw::redis;

int main() {

	ConnectionOptions opts;
	opts.host = "127.0.0.1";
	opts.port = 6379;

	auto redis = Redis(opts);

	auto sub = redis.subscriber();

	sub.on_message([](std::string channel, std::string msg) {
            cout << "received message: " << msg  << " from channel: " << channel << endl;
        });

	sub.subscribe("todos");

	while (true) {
		try {
			sub.consume();
		} catch (const Error &err) {
			cout << "deu ruim";
		}

	}

	return 0;
}