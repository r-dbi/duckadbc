#include "duckdb/common/adbc.hpp"
#include "duckdb.h"
// TODO it seems like the duckdb c api is a better fit here
// TODO arg checking and error handling everywhere

AdbcStatusCode AdbcDatabaseInit(const struct AdbcDatabaseOptions *options, struct AdbcDatabase *out,
                                struct AdbcError *error) {
	auto res = duckdb_open(options->target, (duckdb_database*) &out->private_data);
	return res == DuckDBSuccess ? ADBC_STATUS_OK : ADBC_STATUS_UNKNOWN;
}

AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions *options, struct AdbcConnection *out,
                                  struct AdbcError *error) {
	auto res = duckdb_connect((duckdb_database)options->database->private_data, (duckdb_connection*) &out->private_data);
	return res == DuckDBSuccess ? ADBC_STATUS_OK : ADBC_STATUS_UNKNOWN;
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	duckdb_disconnect((duckdb_connection*) &connection->private_data);
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {
	duckdb_close((duckdb_database*) &database->private_data);
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
	auto res = duckdb_query_arrow((duckdb_connection)connection->private_data, query, (duckdb_arrow *) &statement->private_data);
	return res == DuckDBSuccess ? ADBC_STATUS_OK : ADBC_STATUS_UNKNOWN;
}

static int get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	auto res = duckdb_query_arrow_schema((duckdb_arrow *)stream->private_data, (duckdb_arrow_schema*) &out);
	return res == DuckDBSuccess ? ADBC_STATUS_OK : ADBC_STATUS_UNKNOWN;
}

static int get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	auto res = duckdb_query_arrow_array((duckdb_arrow *)stream->private_data, (duckdb_arrow_array*) &out);
	return res == DuckDBSuccess ? ADBC_STATUS_OK : ADBC_STATUS_UNKNOWN;
}

void release(struct ArrowArrayStream *stream) {
	stream->private_data = nullptr;
}

AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                      struct AdbcError *error) {

	out->private_data = statement->private_data;
	out->get_schema = get_schema;
	out->get_next = get_next;
	out->release = release;

	// TODO error callback
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {
	duckdb_destroy_arrow((duckdb_arrow *) &statement->private_data);
	return ADBC_STATUS_OK;
}