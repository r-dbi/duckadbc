#include "duckdb/common/adbc.hpp"
#include "duckdb.hpp"

// TODO arg checking and error handling everywhere

AdbcStatusCode AdbcDatabaseInit(const struct AdbcDatabaseOptions *options, struct AdbcDatabase *out,
                                struct AdbcError *error) {
	D_ASSERT(options);
	D_ASSERT(out);
	out->private_data = new duckdb::DuckDB(options->target);
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions *options, struct AdbcConnection *out,
                                  struct AdbcError *error) {
	D_ASSERT(options);
	D_ASSERT(options->database);
	D_ASSERT(options->database->private_data);

	out->private_data = new duckdb::Connection(*(duckdb::DuckDB *)options->database->private_data);

	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	D_ASSERT(connection);
	D_ASSERT(connection->private_data);
	delete (duckdb::Connection *)connection->private_data;
	connection->private_data = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {

	D_ASSERT(database);
	D_ASSERT(database->private_data);
	delete (duckdb::DuckDB *)database->private_data;
	database->private_data = nullptr;
	return ADBC_STATUS_OK;
}

// this is a nop?
AdbcStatusCode AdbcStatementInit(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                 struct AdbcError *error) {
	statement->private_data = nullptr;
	return ADBC_STATUS_OK;
}

struct DuckDBQueryResultHolder {
	DuckDBQueryResultHolder(duckdb::unique_ptr<duckdb::QueryResult> result_p) : result(move(result_p)) {};
	duckdb::unique_ptr<duckdb::QueryResult> result;
};

AdbcStatusCode AdbcConnectionSqlExecute(struct AdbcConnection *connection, const char *query, size_t query_length,
                                        struct AdbcStatement *statement, struct AdbcError *error) {
	auto q = std::string(query, query_length);
	statement->private_data =
	    new DuckDBQueryResultHolder(((duckdb::Connection *)connection->private_data)->SendQuery(q));
	return ADBC_STATUS_OK;
}

static int get_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	auto &result = ((DuckDBQueryResultHolder *)stream->private_data)->result;
	auto timezone_config = duckdb::QueryResult::GetConfigTimezone(*result);

	result->ToArrowSchema(out, result->types, result->names, timezone_config);

	return ADBC_STATUS_OK;
}

static int get_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	auto &result = ((DuckDBQueryResultHolder *)stream->private_data)->result;

	result->Fetch()->ToArrowArray(out);
	return ADBC_STATUS_OK;
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
	delete (DuckDBQueryResultHolder *)statement->private_data;
	statement->private_data = nullptr;
	return ADBC_STATUS_OK;
}