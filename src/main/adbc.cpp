#include "duckdb/common/adbc.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

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

AdbcStatusCode duckdb_adbc_init(size_t count, struct AdbcDriver *driver, size_t *initialized) {
	if (!driver) {
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
	driver->ConnectionGetCatalogs = AdbcConnectionGetCatalogs;
	driver->ConnectionGetDbSchemas = AdbcConnectionGetDbSchemas;
	driver->ConnectionGetTableTypes = AdbcConnectionGetTableTypes;
	driver->ConnectionGetTables = AdbcConnectionGetTables;

	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseInit(const struct AdbcDatabaseOptions *options, struct AdbcDatabase *out,
                                struct AdbcError *error) {
	CHECK_TRUE(options, error, "Missing options");
	CHECK_TRUE(out, error, "Missing result pointer");

	char *errormsg;
	auto res = duckdb_open_ext(options->target, (duckdb_database *)&out->private_data, nullptr, &errormsg);
	// TODO this leaks memory because errormsg is malloc-ed
	out->private_driver = options->driver;
	CHECK_RES(res, error, errormsg);
}

AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions *options, struct AdbcConnection *out,
                                  struct AdbcError *error) {
	CHECK_TRUE(options, error, "Missing options");
	CHECK_TRUE(options->database && options->database->private_data, error, "Missing database");
	CHECK_TRUE(out, error, "Missing result pointer");

	auto res =
	    duckdb_connect((duckdb_database)options->database->private_data, (duckdb_connection *)&out->private_data);
	out->private_driver = options->database->private_driver;
	CHECK_RES(res, error, "Failed to connect to Database");
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	if (connection && connection->private_data) {
		duckdb_disconnect((duckdb_connection *)&connection->private_data);
		connection->private_data = nullptr;
		connection->private_driver = nullptr;
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
	CHECK_TRUE(connection && connection->private_data, error, "Invalid connection");
	CHECK_TRUE(statement, error, "Missing statement pointer");

	statement->private_data = nullptr;
	statement->private_driver = connection->private_driver;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionSqlExecute(struct AdbcConnection *connection, const char *query,
                                        struct AdbcStatement *statement, struct AdbcError *error) {
	auto init_res = AdbcStatementInit(connection, statement, error);
	if (init_res != ADBC_STATUS_OK) {
		return init_res;
	}
	// other stuff checked by AdbcStatementInit
	CHECK_TRUE(query, error, "Missing query");

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

AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                      struct AdbcError *error) {
	CHECK_TRUE(statement && statement->private_data, error, "Invalid statement");
	CHECK_TRUE(out, error, "Missing result pointer");

	out->private_data = statement->private_data;
	out->get_schema = get_schema;
	out->get_next = get_next;
	out->release = release;
	out->get_last_error = get_last_error;

	// because we handed out the stream pointer its no longer our responsibility to destroy it in AdbcStatementRelease,
	// this is now done in release()
	statement->private_data = nullptr;

	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {
	if (statement && statement->private_data) {
		duckdb_destroy_arrow((duckdb_arrow *)&statement->private_data);
		statement->private_data = nullptr;
		statement->private_driver = nullptr;
	}
	return ADBC_STATUS_OK;
}

void AdbcErrorRelease(struct AdbcError *error) {
	if (!error) {
		return;
	}
	if (error->message) {
		free(error->message);
	}
	error->message = nullptr;
}

AdbcStatusCode AdbcConnectionGetCatalogs(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                         struct AdbcError *error) {
	// faking a catalog name because ADBC does not allow NULL there
	const char *q = "SELECT 'duckdb' catalog_name";
	return AdbcConnectionSqlExecute(connection, q, statement, error);
}

AdbcStatusCode AdbcConnectionGetDbSchemas(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                          struct AdbcError *error) {
	const char *q = "SELECT 'duckdb' catalog_name, schema_name db_schema_name FROM information_schema.schemata ORDER "
	                "BY schema_name";
	return AdbcConnectionSqlExecute(connection, q, statement, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                           struct AdbcError *error) {
	const char *q = "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type";
	return AdbcConnectionSqlExecute(connection, q, statement, error);
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

	return AdbcConnectionSqlExecute(connection, q.c_str(), statement, error);
}