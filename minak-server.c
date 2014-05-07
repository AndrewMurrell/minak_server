#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

#define GET(fd, var)							\
	do {								\
		if (read(fd, &(var), sizeof(var)) != sizeof(var))	\
			return 1;					\
	} while (0)

#define COUNT(ary) (sizeof(ary)/sizeof((ary)[0]))

#define PG_EXEC(conn, command)					\
	({							\
		PGresult *res = PQexec(conn, command);		\
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {	\
			PQclear(res);				\
			return 1;				\
		}						\
		int val = atoi(PQgetvalue(res, 0, 1));		\
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
			PQclear(res);					\
			return 1;					\
		}							\
		int val = atoi(PQgetvalue(res, 0, 1));			\
		PQclear(res);						\
		val;							\
	})


struct gesture_store {
	uint16_t file_format;
	uint32_t num_entries;
};

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

	*store_id = PG_EXEC(conn, "INSERT INTO gesture_libraries RETURNING id");

	for (size_t i = 0; i < store.num_entries; i++)
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
	entry.name = calloc(entry.name_len, 1);
	if (read(fd, entry.name, entry.name_len) != entry.name_len)
		return 1;
	GET(fd, entry.num_gestures);

	int entry_id = PG_EXEC_PARAMS(conn,
		"INSERT VALUES(name=$1::text, library_id=$2::integer) INTO gesture_entries RETURNING id",
		entry.name, itoa(store_id));

	for (size_t i = 0; i < entry.num_gestures; i++)
		if (load_gesture(fd, conn, entry_id) != 0) return 1;
	return 0;
}

struct gesture {
	uint64_t id;
	uint32_t num_strokes;
};

int
load_gesture(int fd, PGconn* conn, int entry_id) {
	struct gesture gesture;
	GET(fd, gesture);

	int gesture_id = PG_EXEC_PARAMS(conn,
		"INSERT VALUES(entry_id=$1::integer) INTO gestures RETURNING id",
		itoa(entry_id));

	for (size_t i = 0; i < gesture.num_strokes; i++)
		if (load_gesture_stroke(fd, conn, gesture_id) != 0) return 1;
	return 0;
}

int
load_gesture_stroke(int fd, PGconn* conn, int gesture_id) {
	uint32_t num_points;
	GET(fd, num_points);

	int stroke_id = PG_EXEC_PARAMS(conn,
		"INSERT VALUES(gesture_id=$1::integer) INTO gesture_strokes RETURNING id",
		itoa(gesture_id));

	for (size_t i = 0; i < num_points; i++)
		if (load_gesture_point(fd, conn, stroke_id) != 0) return 1;
	return 0;
}

struct gesture_point {
	uint32_t x;
	uint32_t y;
	uint64_t time;
};

int load_gesture_point(int fd, PGconn* conn, int stroke_id) {
	struct gesture_point point;
	GET(fd, point);
	PG_EXEC_PARAMS(conn,
		"INSERT VALUES(x=$1::integer, y=$2::integer, time=$3::integer, stroke_id=$4::integer) INTO gesture_points RETURNING id",
		itoa(point.x), itoa(point.y), itoa(point.time), itoa(stroke_id));
	return 0;
}

static void exit_nicely(PGconn *conn) {
	PQfinish(conn);
	exit(1);
}

int
main(int argc, char **argv) {
	//from http://www.postgresql.org/docs/current/static/libpq-example.html

	const char *conninfo;
	PGconn     *conn;
	PGresult   *res;
	int        nFields;
	int        i, j;

	/*
	 * If the user supplies a parameter on the command line, use it as the
	 * conninfo string; otherwise default to setting dbname=postgres and using
	 * environment variables or defaults for all other connection parameters.
	 */
	if (argc > 1)
		conninfo = argv[1];
	else
		conninfo = "dbname = postgres";
	
	/* Make a connection to the database */
	conn = PQconnectdb(conninfo);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK) {
		fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
		exit_nicely(conn);
	}

	/* Start a transaction block */
	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}

/*

http://www.postgresql.org/docs/current/static/libpq-example.html
http://www.postgresql.org/docs/9.0/static/libpq-exec.html
http://www.postgresql.org/docs/6.4/static/libpq-chapter16943.htm
http://stackoverflow.com/questions/2944297/postgresql-function-for-last-inserted-id

*/

	/*
	 * Should PQclear PGresult whenever it is no longer needed to avoid memory
	 * leaks
	 */
	PQclear(res);

	/*
	 * Fetch rows from pg_database, the system catalog of databases
	 */
	res = PQexec(conn, "DECLARE myportal CURSOR FOR select * from pg_database");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit_nicely(conn);
	}
	PQclear(res);

	res = PQexec(conn, "FETCH ALL in myportal");
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		PQclear(res);
		exit_nicely(conn);
	}


	/* first, print out the attribute names */
	nFields = PQnfields(res);
	for (i = 0; i < nFields; i++)
		printf("%-15s", PQfname(res, i));
	printf("\n\n");

	/* next, print out the rows */
	for (i = 0; i < PQntuples(res); i++) {
		for (j = 0; j < nFields; j++)
			printf("%-15s", PQgetvalue(res, i, j));
		printf("\n");
	}

	PQclear(res);

	/* close the portal ... we don't bother to check for errors ... */
	res = PQexec(conn, "CLOSE myportal");
	PQclear(res);

	/* end the transaction */
	res = PQexec(conn, "END");
	PQclear(res);

	/* close the connection to the database and cleanup */
	PQfinish(conn);

	return 0;
}

