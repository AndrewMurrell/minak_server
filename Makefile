CFLAGS = -g -std=gnu99 -Wall -Wextra -Werror

all: minak-server

minak-server: -lpq

clean:
	rm -f minak-server
