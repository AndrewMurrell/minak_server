#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <libpq-fe.h>

// TODO: rollback transaction on failure

void
nread(int fd, void *buf, size_t n) {
	size_t bytes = 0;
	ssize_t r;
	while (bytes < n) {
		r = read(fd, &(((char*)buf)[bytes]), n-bytes);
		if (r < 1) {
			printf("Status: 400 Client error\r\n\r\nInput ended unexpectedly\n");
			exit(1);
		}
		bytes += (size_t)r;
	}
}

void
nwrite(int fd, void *buf, size_t n) {
	size_t bytes = 0;
	ssize_t r;
	while (bytes < n) {
		r = write(fd, &(((char*)buf)[bytes]), n-bytes);
		if (r < 1) {
			exit(1);
		}
		bytes += (size_t)r;
	}
}

#define deflip16(var) do { (var) = ntohs(var); } while(0)
#define deflip32(var) do { (var) = ntohl(var); } while(0)
#define deflip64(var) TODO

#define reflip16(var) do { (var) = htons(var); } while(0)
#define reflip32(var) do { (var) = htonl(var); } while(0)
#define reflip64(var) TODO

#define GET(fd, var)							\
	do {								\
		nread(fd, &(var), sizeof(var));				\
	} while (0)

#define COUNT(ary) (sizeof(ary)/sizeof((ary)[0]))

static void exit_nicely(PGconn *conn) {
	printf("Status: 500 Server error\r\n"
		"\r\n"
		"There was an error.  It's not you, it's us.\n"
		"%s",
		PQerrorMessage(conn));
	PQfinish(conn);
	exit(1);
}

#define PG_EXEC(conn, command)					\
	({							\
		PGresult *res = PQexec(conn, command);		\
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {	\
			exit_nicely(conn);			\
		}						\
		int val = atoi(PQgetvalue(res, 0, 0));		\
		PQclear(res);					\
		val;						\
	})


#define PG_EXEC_PARAMS(conn, command, ...)				\
	({								\
		char *params[] = { __VA_ARGS__ };			\
		PGresult *res = PQexecParams(conn, command,		\
			COUNT(params), /* nParams */			\
			NULL, /* paramTypes */				\
			(const char * const *)params, /* paramValues */ \
			NULL, /* paramLengths */			\
			NULL, /* paramFormats */			\
			0); /* resultFormat */				\
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {		\
			exit_nicely(conn);				\
		}							\
		int val = atoi(PQgetvalue(res, 0, 0));			\
		PQclear(res);						\
		val;							\
	})

#define PG_QUERY(conn, command, ...)					\
	({								\
		char *params[] = { __VA_ARGS__ };			\
		PGresult *res = PQexecParams(conn, command,		\
			COUNT(params), /* nParams */			\
			NULL, /* paramTypes */				\
			(const char * const *)params, /* paramValues */ \
			NULL, /* paramLengths */			\
			NULL, /* paramFormats */			\
			0); /* resultFormat */				\
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {		\
			exit_nicely(conn);				\
		}							\
		res;							\
	})


struct gesture_store {
	uint16_t file_format;
	uint32_t num_entries;
} __attribute__((packed));

char itoa_buf[64];
char *itoa(int i) {
	snprintf(itoa_buf, 63, "%d", i);
	return itoa_buf;
}

int load_gesture_point(int fd, PGconn *conn, int stroke_id);
int load_gesture_stroke(int fd, PGconn* conn, int gesture_id);
int load_gesture(int fd, PGconn* conn, int entry_id);
int load_gesture_entry(int fd, PGconn* conn, int store_id);

int
_load_gesture_store(int fd, PGconn* conn, int *store_id) {
	struct gesture_store store;
	GET(fd, store);
	deflip16(store.file_format);
	deflip32(store.num_entries);

	*store_id = PG_EXEC(conn, "INSERT INTO gesture_libraries (bs) VALUES(1) RETURNING id");

	for (ssize_t i = 0; i < store.num_entries; i++)
		if (load_gesture_entry(fd, conn, *store_id) != 0) return 1;
	return 0;
}

int
load_gesture_store(int fd, PGconn* conn) {
	int store_id;
	int ret = _load_gesture_store(fd, conn, &store_id);
	if (ret != 0)
		return -1;
	return store_id;
}

struct gesture_entry {
	uint16_t name_len;
	char *name;
	uint32_t num_gestures;
};

int
load_gesture_entry(int fd, PGconn* conn, int store_id) {
	struct gesture_entry entry;

	GET(fd, entry.name_len);
	deflip16(entry.name_len);

	entry.name = calloc(entry.name_len+1, 1);
	nread(fd, entry.name, entry.name_len);
	entry.name[entry.name_len] = '\0';

	GET(fd, entry.num_gestures);
	deflip32(entry.num_gestures);

	int entry_id = PG_EXEC_PARAMS(conn,
		"INSERT INTO gesture_entries (name, library_id) VALUES($1::text, $2::integer) RETURNING id",
		entry.name, itoa(store_id));

	for (ssize_t i = 0; i < entry.num_gestures; i++)
		if (load_gesture(fd, conn, entry_id) != 0) return 1;
	return 0;
}

struct gesture {
	uint64_t id;
	uint32_t num_strokes;
} __attribute__((packed));

int
load_gesture(int fd, PGconn* conn, int entry_id) {
	struct gesture gesture;
	GET(fd, gesture);
	//TODO: deflip64(gesture.id);
	deflip32(gesture.num_strokes);

	int gesture_id = PG_EXEC_PARAMS(conn,
		"INSERT INTO gestures (entry_id) VALUES($1::integer) RETURNING id",
		itoa(entry_id));

	for (ssize_t i = 0; i < gesture.num_strokes; i++)
		if (load_gesture_stroke(fd, conn, gesture_id) != 0) return 1;
	return 0;
}

int
load_gesture_stroke(int fd, PGconn* conn, int gesture_id) {
	int32_t num_points;
	GET(fd, num_points);
	deflip32(num_points);

	int stroke_id = PG_EXEC_PARAMS(conn,
		"INSERT INTO gesture_strokes (gesture_id) VALUES($1::integer) RETURNING id",
		itoa(gesture_id));

	for (ssize_t i = 0; i < num_points; i++)
		if (load_gesture_point(fd, conn, stroke_id) != 0) return 1;
	return 0;
}

// these are all un-fliped, because they are not really integers
struct gesture_point {
	int32_t x;
	int32_t y;
	int64_t time;
} __attribute__((packed));

int
load_gesture_point(int fd, PGconn* conn, int stroke_id) {
	struct gesture_point point;
	GET(fd, point);

	//TODO: xstrdup
	char *s_stroke_id; asprintf(&s_stroke_id, "%d", stroke_id);
	char *s_x        ; asprintf(&s_x, "%d", point.x);;
	char *s_y        ; asprintf(&s_y, "%d", point.y);;
	char *s_time     ; asprintf(&s_time, "%ld", point.time);

	PG_EXEC_PARAMS(conn,
		"INSERT INTO gesture_points "
		"      (stroke_id  , x          , y          , time      ) "
		"VALUES($1::integer, $2::integer, $3::integer, $4::bigint) "
		"RETURNING id",
		s_stroke_id, s_x, s_y, s_time);

	free(s_stroke_id);
	free(s_x);
	free(s_y);
	free(s_time);

	return 0;
}


void
download(int fd, PGconn *conn, int id) {
	char *buf;

	dprintf(fd,
		"Status: 200 Document follows\r\n"
		"Content-Type: application/octet-stream\r\n"
		"\r\n");
	nwrite(fd, "\x00\x01", 2);
	PGresult *entries = PG_QUERY(conn, "SELECT id, name FROM gesture_entries WHERE library_id = $1::integer", itoa(id));
	int fnum_id   = PQfnumber(entries, "id");
	int fnum_name = PQfnumber(entries, "name");

	fprintf(stderr, "%d, %d, %d, %d\n\n", num_entries, num_gestures, num_strokes, num_points);

	uint32_t num_entries = PQntuples(entries);
	reflip32(num_entries);
	nwrite(fd, &num_entries, sizeof(num_entries));
	deflip32(num_entries);

	for (size_t i = 0; i < num_entries; i++) {
		buf = PQgetvalue(entries, i, fnum_name);
		uint16_t len = strlen(buf);
		reflip16(len);
		nwrite(fd, &len, sizeof(len));
		dprintf(fd, "%s", buf);
		
		buf = PQgetvalue(entries, i, fnum_id);
		PGresult *gestures = PG_QUERY(conn, "SELECT id FROM gestures WHERE entry_id = $1::integer", buf);
		uint32_t num_gestures = PQntuples(gestures);
		reflip32(num_gestures);
		nwrite(fd, &num_gestures, sizeof(num_gestures));
		deflip32(num_gestures);

		for (size_t j = 0; j < num_gestures; j++) {
			buf = PQgetvalue(gestures, j, 0);

			uint32_t gesture_id = atoi(buf);
			reflip32(gesture_id);
			nwrite(fd, "\0\0\0\0", 4);
			nwrite(fd, &gesture_id, 4);

			PGresult *strokes = PG_QUERY(conn, "SELECT id FROM gesture_strokes WHERE gesture_id = $1::integer", buf);
			uint32_t num_strokes = PQntuples(strokes);
			reflip32(num_strokes);
			nwrite(fd, &num_strokes, sizeof(num_strokes));
			deflip32(num_strokes);

			for (size_t k = 0; k < num_strokes; k++) {
				buf = PQgetvalue(strokes, k, 0);

				PGresult *points = PG_QUERY(conn, "SELECT x, y, time FROM gesture_points WHERE stroke_id = $1::integer", buf);
				uint32_t num_points = PQntuples(points);
				reflip32(num_points);
				nwrite(fd, &num_points, sizeof(num_points));
				deflip32(num_points);
				
				int fnum_x = PQfnumber(points, "x");
				int fnum_y = PQfnumber(points, "y");
				int fnum_time = PQfnumber(points, "time");

				for (size_t l; l < num_points; l++) {
					int32_t x = atoi(PQgetvalue(entries, l, fnum_x));
					int32_t y = atoi(PQgetvalue(entries, l, fnum_y));
					int64_t t = atol(PQgetvalue(entries, l, fnum_time));

					nwrite(fd, &x, sizeof(x));
					nwrite(fd, &y, sizeof(y));
					nwrite(fd, &t, sizeof(t));
				}
			}
		}
	}
}


void
upload(int fd, PGconn* conn) {
	int id = load_gesture_store(fd, conn);
	if (id < 0) {
		printf("Status: 400 error\r\n"
			"\r\n"
			"There was an error loading the gesture file.\n");
	} else {
		printf("Status: 201 Created\r\n"
			"\r\n"
			"ID: %d\n", id);
	}
}

/*

http://www.postgresql.org/docs/current/static/libpq-example.html
http://www.postgresql.org/docs/9.0/static/libpq-exec.html
http://www.postgresql.org/docs/6.4/static/libpq-chapter16943.htm
http://stackoverflow.com/questions/2944297/postgresql-function-for-last-inserted-id

*/

int
main(int argc, char **argv) {
	const char *conninfo;
	PGconn     *conn;
	PGresult   *res;

	if (argc > 1)
		conninfo = argv[1];
	else
		conninfo = "dbname = minak";

	/* Begin the connection/transaction */
	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK) {
		exit_nicely(conn);
	}
	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);

	/* Main body */
	char *method = getenv("REQUEST_METHOD");
	if (method == NULL) {
		/* do nothing */
	} else if (strcasecmp(method, "POST") == 0) {
		upload(0, conn);
	} else if (strcasecmp(method, "GET") == 0) {
		int id = atoi(getenv("QUERY_STRING"));
		download(1, conn, id);
	}

	/* End the connection/transaction */
	res = PQexec(conn, "END");
	PQclear(res);
	PQfinish(conn);

	return 0;
}
