#include "catch.hpp"
#include "duckdb/common/adbc.hpp"

using namespace std;

TEST_CASE("Happy path", "[adbc]") {
	AdbcStatusCode adbc_status;
	AdbcError adbc_error;
	AdbcDatabaseOptions adbc_database_options;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcConnectionOptions adbc_connection_options;
	AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;
	ArrowArray arrow_array;
	int arrow_status;

	adbc_database_options.target = ":memory:";
	adbc_database_options.target_length = strlen(adbc_database_options.target);

	adbc_status = AdbcDatabaseInit(&adbc_database_options, &adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// connect!
	adbc_connection_options.database = &adbc_database;
	adbc_status = AdbcConnectionInit(&adbc_connection_options, &adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can query catalogs
	adbc_status = AdbcConnectionGetCatalogs(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(arrow_status == 0);
	REQUIRE((arrow_array.n_children == 1 && arrow_array.children[0]->length == 1));

	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	adbc_status = AdbcStatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can release again
	adbc_status = AdbcStatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can release a nullptr
	adbc_status = AdbcStatementRelease(nullptr, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can query schemata
	adbc_status = AdbcConnectionGetDbSchemas(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(arrow_status == 0);
	REQUIRE((arrow_array.n_children == 2 && arrow_array.children[0]->length > 0));

	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	adbc_status = AdbcStatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// create a dummy table
	const char *q = "CREATE TABLE dummy(a INTEGER)";
	adbc_status = AdbcConnectionSqlExecute(&adbc_connection, q, strlen(q), &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	adbc_status = AdbcStatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can query tables
	adbc_status = AdbcConnectionGetTables(&adbc_connection, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0,
	                                      &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(arrow_status == 0);
	REQUIRE((arrow_array.n_children == 4 && arrow_array.children[0]->length == 1));

	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	adbc_status = AdbcStatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can query tables using specific schema names and prefixes
	adbc_status = AdbcConnectionGetTables(&adbc_connection, nullptr, 0, "main", 4, "dum%", 4, nullptr, 0,
	                                      &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(arrow_status == 0);
	REQUIRE((arrow_array.n_children == 4 && arrow_array.children[0]->length == 1));

	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	adbc_status = AdbcStatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// tear down connection and database again
	adbc_status = AdbcConnectionRelease(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can release again
	adbc_status = AdbcConnectionRelease(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can also release a nullptr
	adbc_status = AdbcConnectionRelease(nullptr, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = AdbcDatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// can release twice no problem
	adbc_status = AdbcDatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// can release a nullptr
	adbc_status = AdbcDatabaseRelease(nullptr, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
}

TEST_CASE("Error conditions", "[adbc]") {
	AdbcError adbc_error;
	AdbcStatusCode adbc_status;
	AdbcDatabaseOptions adbc_database_options;
	AdbcDatabase adbc_database;

	// NULL options
	adbc_status = AdbcDatabaseInit(nullptr, &adbc_database, &adbc_error);
	REQUIRE(adbc_status != ADBC_STATUS_OK);
	REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
	AdbcErrorRelease(&adbc_error);

	// NULL database
	adbc_status = AdbcDatabaseInit(&adbc_database_options, nullptr, &adbc_error);
	REQUIRE(adbc_status != ADBC_STATUS_OK);
	REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
	AdbcErrorRelease(&adbc_error);

	// null error
	adbc_status = AdbcDatabaseInit(&adbc_database_options, nullptr, nullptr);
	REQUIRE(adbc_status != ADBC_STATUS_OK);

	// non-writeable path
	adbc_database_options.target = "/cant/write/this";
	adbc_database_options.target_length = strlen(adbc_database_options.target);
	adbc_status = AdbcDatabaseInit(&adbc_database_options, &adbc_database, &adbc_error);
	REQUIRE(adbc_status != ADBC_STATUS_OK);
	REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
	AdbcErrorRelease(&adbc_error);

	// also, we can release an error again
	AdbcErrorRelease(&adbc_error);
	// and we can release a nullptr
	AdbcErrorRelease(nullptr);
}
