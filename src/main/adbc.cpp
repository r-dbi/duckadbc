#include "duckdb/common/adbc.hpp"
#include "duckdb.h"
#include <string.h>
#include <stdlib.h>

#define CHECK_TRUE(p, e, m)                                                                                            \
	if (!(p)) {                                                                                                        \
		if (e) {                                                                                                       \
			e->message = strdup(m);                                                                                    \
		}                                                                                                              \
		return ADBC_STATUS_INVALID_ARGUMENT;                                                                           \
	}

#define CHECK_RES(res, e, m)                                                                                           \
	if (res != DuckDBSuccess) {                                                                                        \
		if (e) {                                                                                                       \
			e->message = strdup(m);                                                                                    \
		}                                                                                                              \
		return ADBC_STATUS_INTERNAL;                                                                                   \
	} else {                                                                                                           \
		return ADBC_STATUS_OK;                                                                                         \
	}


AdbcStatusCode duckdb_adbc_init(size_t count, struct AdbcDriver* driver,
                                             size_t* initialized) {
	if(!driver) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	driver->ConnectionInit = AdbcConnectionInit;
	driver->ConnectionRelease = AdbcConnectionRelease;
	driver->ConnectionSqlExecute = AdbcConnectionSqlExecute;
	driver->DatabaseInit = AdbcDatabaseInit;
	driver->DatabaseRelease = AdbcDatabaseRelease;
	driver->ErrorRelease = AdbcErrorRelease;
	driver->StatementGetStream = AdbcStatementGetStream;
	driver->StatementInit = AdbcStatementInit;
	driver->StatementRelease = AdbcStatementRelease;

	return ADBC_STATUS_OK;
}



AdbcStatusCode AdbcDatabaseInit(const struct AdbcDatabaseOptions *options, struct AdbcDatabase *out,
                                struct AdbcError *error) {
	CHECK_TRUE(options, error, "Missing options");
	CHECK_TRUE(out, error, "Missing result pointer");

	char *errormsg;
	auto res = duckdb_open_ext(options->target, (duckdb_database *)&out->private_data, nullptr, &errormsg);
	// TODO this leaks memory because errormsg is malloc-ed
	CHECK_RES(res, error, errormsg);
}

AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions *options, struct AdbcConnection *out,
                                  struct AdbcError *error) {
	CHECK_TRUE(options, error, "Missing options");
	CHECK_TRUE(options->database || options->database->private_data, error, "Missing database");
	CHECK_TRUE(out, error, "Missing result pointer");

	auto res =
	    duckdb_connect((duckdb_database)options->database->private_data, (duckdb_connection *)&out->private_data);
	CHECK_RES(res, error, "Failed to connect to Database");
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	if (connection && connection->private_data) {
		duckdb_disconnect((duckdb_connection *)&connection->private_data);
		connection->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {
	if (database && database->private_data) {
		duckdb_close((duckdb_database *)&database->private_data);
		database->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

// this is a nop?
AdbcStatusCode AdbcStatementInit(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                 struct AdbcError *error) {
	statement->private_data = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionSqlExecute(struct AdbcConnection *connection, const char *query, size_t query_length,
                                        struct AdbcStatement *statement, struct AdbcError *error) {

	CHECK_TRUE(connection || connection->private_data, error, "Invalid connection");
	CHECK_TRUE(query, error, "Missing query");
	CHECK_TRUE(statement, error, "Missing statement pointer");

	auto res = duckdb_query_arrow((duckdb_connection)connection->private_data, query,
	                              (duckdb_arrow *)&statement->private_data);
	CHECK_RES(res, error, duckdb_query_arrow_error((duckdb_arrow *)&statement->private_data));
}

static int get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	return duckdb_query_arrow_schema((duckdb_arrow *)stream->private_data, (duckdb_arrow_schema *)&out);
}

static int get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream || !stream->private_data || !out) {
		return DuckDBError;
	}
	return duckdb_query_arrow_array((duckdb_arrow *)stream->private_data, (duckdb_arrow_array *)&out);
}

void release(struct ArrowArrayStream *stream) {
	stream->private_data = nullptr;
}

AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                      struct AdbcError *error) {
	CHECK_TRUE(statement || statement->private_data, error, "Invalid statement");
	CHECK_TRUE(out, error, "Missing result pointer");

	out->private_data = statement->private_data;
	out->get_schema = get_schema;
	out->get_next = get_next;
	out->release = release;

	// TODO error callback
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {
	if (statement && statement->private_data) {
		duckdb_destroy_arrow((duckdb_arrow *)&statement->private_data);
		statement->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

void AdbcErrorRelease(struct AdbcError *error) {
	if (!error) {
		return;
	}
	// error messages are statically allocated, no need to free
	if (error->message) {
		free(error->message);
	}
	error->message = nullptr;
}