#include "duckdb/common/adbc.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb.h"
#include "duckdb/main/connection.hpp" // ugly, but we need to create a table function
#include <string.h>
#include <stdlib.h>

#define CHECK_TRUE(p, e, m)                                                                                            \
	if (!(p)) {                                                                                                        \
		if (e) {                                                                                                       \
			e->message = strdup(m); /* TODO Set cleanup callback */                                                    \
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

AdbcStatusCode duckdb_adbc_init(size_t count, struct AdbcDriver *driver, size_t *initialized) {
	if (!driver) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	driver->DatabaseNew = AdbcDatabaseNew;
	driver->DatabaseSetOption = AdbcDatabaseSetOption;
	driver->DatabaseInit = AdbcDatabaseInit;
	driver->DatabaseRelease = AdbcDatabaseRelease;
	driver->ConnectionNew = AdbcConnectionNew;
	driver->ConnectionSetOption = AdbcConnectionSetOption;
	driver->ConnectionInit = AdbcConnectionInit;
	driver->ConnectionRelease = AdbcConnectionRelease;
	driver->ConnectionGetCatalogs = AdbcConnectionGetCatalogs;
	driver->ConnectionGetDbSchemas = AdbcConnectionGetDbSchemas;
	driver->ConnectionGetTableTypes = AdbcConnectionGetTableTypes;
	driver->ConnectionGetTables = AdbcConnectionGetTables;
	driver->StatementNew = AdbcStatementNew;
	driver->StatementRelease = AdbcStatementRelease;
	//	driver->StatementBind = AdbcStatementBind;
	driver->StatementBindStream = AdbcStatementBindStream;
	driver->StatementExecute = AdbcStatementExecute;
	driver->StatementPrepare = AdbcStatementPrepare;
	driver->StatementGetStream = AdbcStatementGetStream;
	driver->StatementSetOption = AdbcStatementSetOption;
	driver->StatementSetSqlQuery = AdbcStatementSetSqlQuery;

	*initialized = ADBC_VERSION_0_0_1;
	return ADBC_STATUS_OK;
}

struct DuckDBAdbcDatabaseWrapper {
	duckdb_config config;
	duckdb_database database;
};

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase *database, struct AdbcError *error) {
	CHECK_TRUE(database, error, "Missing database object");

	database->private_data = nullptr;
	auto wrapper = (DuckDBAdbcDatabaseWrapper *)malloc(sizeof(DuckDBAdbcDatabaseWrapper));
	CHECK_TRUE(wrapper, error, "Allocation error");

	database->private_data = wrapper;
	auto res = duckdb_create_config(&wrapper->config);
	CHECK_RES(res, error, "Failed to allocate");
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                     struct AdbcError *error) {
	CHECK_TRUE(database, error, "Missing database object");
	CHECK_TRUE(key, error, "Missing key");

	auto wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;
	auto res = duckdb_set_config(wrapper->config, key, value);

	CHECK_RES(res, error, "Failed to set configuration option");
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase *database, struct AdbcError *error) {
	char *errormsg;
	// TODO can we set the database path via option, too? Does not look like it...
	auto wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;
	auto res = duckdb_open_ext(":memory:", &wrapper->database, wrapper->config, &errormsg);

	// TODO this leaks memory because errormsg is malloc-ed
	CHECK_RES(res, error, errormsg);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {

	if (database && database->private_data) {
		auto wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;

		duckdb_close(&wrapper->database);
		duckdb_destroy_config(&wrapper->config);
		free(database->private_data);
		database->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

struct DuckDBAdbcConnectionWrapper {
	duckdb_database database;
	duckdb_connection connection;
};

AdbcStatusCode AdbcConnectionNew(struct AdbcDatabase *database, struct AdbcConnection *connection,
                                 struct AdbcError *error) {

	CHECK_TRUE(database, error, "Missing database object");
	CHECK_TRUE(connection, error, "Missing connection object");

	connection->private_data = nullptr;
	auto connection_wrapper = (DuckDBAdbcConnectionWrapper *)malloc(sizeof(DuckDBAdbcConnectionWrapper));
	CHECK_TRUE(connection_wrapper, error, "Allocation error");

	auto database_wrapper = (DuckDBAdbcDatabaseWrapper *)database->private_data;

	connection_wrapper->database = database_wrapper->database;
	connection->private_data = connection_wrapper;

	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                       struct AdbcError *error) {
	// there are no connection-level options that need to be set before connecting
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection *connection, struct AdbcError *error) {
	CHECK_TRUE(connection, error, "Missing connection");
	CHECK_TRUE(connection->private_data, error, "Invalid connection");
	auto wrapper = (DuckDBAdbcConnectionWrapper *)connection->private_data;
	wrapper->connection = nullptr;
	auto res = duckdb_connect(wrapper->database, &wrapper->connection);
	CHECK_RES(res, error, "Failed to connect to Database");
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {

	if (connection && connection->private_data) {
		auto wrapper = (DuckDBAdbcConnectionWrapper *)connection->private_data;
		duckdb_disconnect(&wrapper->connection);
		free(connection->private_data);
		connection->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

// some stream callbacks

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
	if (stream && stream->private_data) {
		duckdb_destroy_arrow((duckdb_arrow *)&stream->private_data);
		stream->private_data = nullptr;
		return;
	}
}

const char *get_last_error(struct ArrowArrayStream *stream) {
	if (!stream) {
		return nullptr;
	}
	return duckdb_query_arrow_error(stream);
}

// this is an evil hack, normally we would need a stream factory here, but its probably much easier if the adbc clients
// just hand over a stream

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
stream_produce(uintptr_t factory_ptr,
               std::pair<std::unordered_map<idx_t, std::string>, std::vector<std::string>> &project_columns,
               duckdb::TableFilterSet *filters) {

	// TODO this will ignore any projections or filters but since we don't expose the scan it should be sort of fine
	auto res = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
	res->arrow_array_stream = *(ArrowArrayStream *)factory_ptr;
	return res;
}

void stream_schema(uintptr_t factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
	auto stream = (ArrowArrayStream *)factory_ptr;
	get_schema(stream, &schema.arrow_schema);
}

AdbcStatusCode AdbcIngest(duckdb_connection connection, const char *table_name, struct ArrowArrayStream *input,
                          struct AdbcError *error) {

	CHECK_TRUE(connection, error, "Invalid connection");
	CHECK_TRUE(input, error, "Missing input arrow stream pointer");
	CHECK_TRUE(table_name, error, "Missing database object name");

	try {
		// TODO evil cast, do we need a way to do this from the C api?
		auto cconn = (duckdb::Connection *)connection;
		cconn
		    ->TableFunction("arrow_scan", {duckdb::Value::POINTER((uintptr_t)input),
		                                   duckdb::Value::POINTER((uintptr_t)stream_produce),
		                                   duckdb::Value::POINTER((uintptr_t)get_schema),
		                                   duckdb::Value::UBIGINT(100000)}) // TODO make this a parameter somewhere
		    ->Create(table_name);                                           // TODO this should probably be a temp table
	} catch (std::exception &ex) {
		if (error) {
			error->message = strdup(ex.what());
		}
		return ADBC_STATUS_INTERNAL;
	} catch (...) {
		return ADBC_STATUS_INTERNAL;
	}
	return ADBC_STATUS_OK;
}

struct DuckDBAdbcStatementWrapper {
	duckdb_connection connection;
	duckdb_arrow result;
	duckdb_prepared_statement statement;
	char *ingestion_table_name;
	ArrowArrayStream *ingestion_stream;
};

AdbcStatusCode AdbcStatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                struct AdbcError *error) {

	CHECK_TRUE(connection, error, "Missing connection object");
	CHECK_TRUE(connection->private_data, error, "Invalid connection object");
	CHECK_TRUE(statement, error, "Missing statement object");

	statement->private_data = nullptr;

	auto statement_wrapper = (DuckDBAdbcStatementWrapper *)malloc(sizeof(DuckDBAdbcStatementWrapper));
	CHECK_TRUE(statement_wrapper, error, "Allocation error");

	auto connection_wrapper = (DuckDBAdbcConnectionWrapper *)connection->private_data;

	statement->private_data = statement_wrapper;
	statement_wrapper->connection = connection_wrapper->connection;
	statement_wrapper->statement = nullptr;
	statement_wrapper->result = nullptr;
	statement_wrapper->ingestion_stream = nullptr;
	statement_wrapper->ingestion_table_name = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {

	if (statement && statement->private_data) {
		auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;
		if (wrapper->statement) {
			duckdb_destroy_prepare(&wrapper->statement);
			wrapper->statement = nullptr;
		}
		if (wrapper->result) {
			duckdb_destroy_arrow(&wrapper->result);
			wrapper->result = nullptr;
		}
		if (wrapper->ingestion_stream) {
			wrapper->ingestion_stream->release(wrapper->ingestion_stream);
			wrapper->ingestion_stream = nullptr;
		}
		if (wrapper->ingestion_table_name) {
			free(wrapper->ingestion_table_name);
			wrapper->ingestion_table_name = nullptr;
		}
		free(statement->private_data);
		statement->private_data = nullptr;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementExecute(struct AdbcStatement *statement, struct AdbcError *error) {
	CHECK_TRUE(statement, error, "Missing statement object");
	CHECK_TRUE(statement->private_data, error, "Invalid statement object");
	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;

	if (wrapper->ingestion_stream && wrapper->ingestion_table_name) {
		auto stream = wrapper->ingestion_stream;
		wrapper->ingestion_stream = nullptr;
		return AdbcIngest(wrapper->connection, wrapper->ingestion_table_name, stream, error);
	}

	auto res = duckdb_execute_prepared_arrow(wrapper->statement, &wrapper->result);
	CHECK_RES(res, error, duckdb_query_arrow_error(&wrapper->result));
}

// this is a nop for us
AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement *statement, struct AdbcError *error) {
	CHECK_TRUE(statement, error, "Missing statement object");
	CHECK_TRUE(statement->private_data, error, "Invalid statement object");
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error) {
	CHECK_TRUE(statement, error, "Missing statement object");
	CHECK_TRUE(query, error, "Missing query");

	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;
	auto res = duckdb_prepare(wrapper->connection, query, &wrapper->statement);

	CHECK_RES(res, error, duckdb_prepare_error(&wrapper->result));
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *values,
                                       struct AdbcError *error) {
	CHECK_TRUE(statement, error, "Missing statement object");
	CHECK_TRUE(values, error, "Missing stream object");
	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;
	wrapper->ingestion_stream = values;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                      struct AdbcError *error) {
	CHECK_TRUE(statement, error, "Missing statement object");
	CHECK_TRUE(key, error, "Missing key object");
	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;

	if (strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
		wrapper->ingestion_table_name = strdup(value);
		return ADBC_STATUS_OK;
	}
	return ADBC_STATUS_INVALID_ARGUMENT;
}

AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                      struct AdbcError *error) {
	CHECK_TRUE(statement, error, "Missing statement object");
	CHECK_TRUE(statement->private_data, error, "Invalid statement object");
	auto wrapper = (DuckDBAdbcStatementWrapper *)statement->private_data;
	CHECK_TRUE(wrapper->result, error, "Missing result");

	out->private_data = wrapper->result;
	out->get_schema = get_schema;
	out->get_next = get_next;
	out->release = release;
	out->get_last_error = get_last_error;

	// because we handed out the stream pointer its no longer our responsibility to destroy it in AdbcStatementRelease,
	// this is now done in release()
	wrapper->result = nullptr;

	return ADBC_STATUS_OK;
}

static AdbcStatusCode QueryInternal(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                    const char *query, struct AdbcError *error) {
	AdbcStatusCode res;
	res = AdbcStatementNew(connection, statement, error);
	CHECK_TRUE(!res, error, "unable to initialize statement");

	res = AdbcStatementSetSqlQuery(statement, query, error);
	CHECK_TRUE(!res, error, "unable to initialize statement");

	res = AdbcStatementExecute(statement, error);
	CHECK_TRUE(!res, error, "unable to execute statement");

	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionGetCatalogs(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                         struct AdbcError *error) {
	const char *q = "SELECT 'duckdb' catalog_name";

	return QueryInternal(connection, statement, q, error);
}

AdbcStatusCode AdbcConnectionGetDbSchemas(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                          struct AdbcError *error) {
	const char *q = "SELECT 'duckdb' catalog_name, schema_name db_schema_name FROM information_schema.schemata ORDER "
	                "BY schema_name";
	return QueryInternal(connection, statement, q, error);
}
AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                           struct AdbcError *error) {
	const char *q = "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type";
	return QueryInternal(connection, statement, q, error);
}

AdbcStatusCode AdbcConnectionGetTables(struct AdbcConnection *connection, const char *catalog, const char *db_schema,
                                       const char *table_name, const char **table_types,
                                       struct AdbcStatement *statement, struct AdbcError *error) {

	CHECK_TRUE(catalog == nullptr || strcmp(catalog, "duckdb") == 0, error, "catalog must be NULL or 'duckdb'");

	// let's wait for https://github.com/lidavidm/arrow/issues/6
	CHECK_TRUE(table_types == nullptr, error, "table types parameter not yet supported");
	auto q = duckdb::StringUtil::Format(
	    "SELECT 'duckdb' catalog_name, table_schema db_schema_name, table_name, table_type FROM "
	    "information_schema.tables WHERE table_schema LIKE '%s' AND table_name LIKE '%s' ORDER BY table_schema, "
	    "table_name",
	    db_schema ? db_schema : "%", table_name ? table_name : "%");

	return QueryInternal(connection, statement, q.c_str(), error);
}
