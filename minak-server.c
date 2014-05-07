#include <stdint.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

#define GET(fd, var)                                                          \
	do {                                                                      \
		if (read(fd, &(var), sizeof(var)) != sizeof(var))                     \
			return 1;                                                         \
	} while (0)

#define DONTPANIC(res, conn, cond)                                            \
	do {                                                                      \
		if (PQresultStatus(res) != cond) {                                    \
			fprintf(stderr, "Query Failed: %s", PQerrorMessage(conn));        \
			PQclear(res);                                                     \
			exit_nicely(conn);                                                \
		}                                                                     \
	} while (0)

struct gesture_store {
	uint16_t file_format;
	uint32_t num_entries;
};

/*
 *	SQL Library Functions
 */
static void
exit_nicely(PGconn *conn) {
	PQfinish(conn);
	exit(1);
}

int
_load_gesture_store(int fd, PGconn* conn, int *store_id) {
	struct gesture_store store;
	GET(fd, store);

	PGResult *res = PQexec(conn, "INSERT INTO gesture_libraries RETURNING id"); //SQL: INSERT VALUES() INTO gesture_libraries;
	DONTPANIC(res, conn, PGRES_TUPLES_OK);
	store_id = atoi(PQgetvalue(PGresult *res, 0, 1));
	//TODO get the entry location ^^ assuming that's right

	for (size_t i = 0; i < store.num_entries; i++)
		if (load_gesture_entry(fd, store_id) != 0) return 1;
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
	GET(fd, entry.num_gestrues);

	//int entry_id = ;// SQL: INSERT VALUES(name=name, library_id=store_id) INTO gesture_entries;
	PQexec(conn, "INSERT VALUES(name=?, library_id=?) INTO gesture_entries", 2, {name, store_id});
	DONTPANIC(res, conn, PGRES_TUPLES_OK);

	for (size_t i = 0; i < entry.num_gestures; i++)
		if (load_gesture(fd, entry_id) != 0) return 1;
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

	int gesture_id = ;// SQL: INSERT VALUES(entry_id=entry_id) INTO gestures;
	DONTPANIC(res, conn, PGRES_TUPLES_OK);

	for (size_t i = 0; i < gesture.num_strokes; i++)
		if (load_gesture_stroke(fd, gesture_id) != 0) return 1;
	return 0;
}

int
load_gesture_stroke(int fd, PGconn* conn, int gesture_id) {
	uint32_t num_points;
	GET(fd, num_points);

	int stroke_id = ; // SQL: INSERT VALUES(gesture_id=gesture_id) INTO gesture_strokes;
	DONTPANIC(res, conn, PGRES_TUPLES_OK);

	for (size_t i = 0; i < num_points; i++)
		if (load_gesture_point(fd, stroke_id) != 0) return 1;
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
	// SQL: INSERT VALUES(x=point.x, y=point.y, time=point.time, stroke_id=stroke_id) INTO gesture_points;
	DONTPANIC(res, conn, PGRES_TUPLES_OK);

}

int int main(int argc, char const *argv[]) {
	//from http://www.postgresql.org/docs/current/static/libpq-example.html

	const char *conninfo;
	PGconn	 *conn;
	PGresult   *res;
	int		 nFields;
	int		 i, j;

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

///////////////////*

http://www.postgresql.org/docs/current/static/libpq-example.html
http://www.postgresql.org/docs/9.0/static/libpq-exec.html
http://www.postgresql.org/docs/6.4/static/libpq-chapter16943.htm
http://stackoverflow.com/questions/2944297/postgresql-function-for-last-inserted-id


/////////////////*/
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
	DONTPANIC(res, conn, PGRES_TUPLES_OK);


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