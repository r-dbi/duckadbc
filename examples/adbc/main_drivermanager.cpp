#include "adbc.h"
#include "driver_manager.h"
#include <stdio.h>
#include <string.h>

// TOOD make a 'nice' macro for error checking here

int main() {
	AdbcError adbc_error;
	AdbcStatusCode adbc_status;

	AdbcDriver adbc_driver;
	size_t initialized;

	adbc_status = AdbcLoadDriver("Driver=../../build/release/src/libduckdb.dylib;Entrypoint=duckdb_adbc_init",
	                             ADBC_VERSION_0_0_1, &adbc_driver, &initialized);
	if (adbc_status != ADBC_STATUS_OK) {
		return -1;
	}

	AdbcDatabaseOptions adbc_database_options;
	adbc_database_options.driver = &adbc_driver;
	adbc_database_options.target = ":memory:";
	adbc_database_options.target_length = strlen(adbc_database_options.target);
	AdbcDatabase adbc_database;

	adbc_status = AdbcDatabaseInit(&adbc_database_options, &adbc_database, &adbc_error);

	AdbcConnection adbc_connection;
	AdbcConnectionOptions adbc_connection_options;
	adbc_connection_options.database = &adbc_database;
	adbc_status = AdbcConnectionInit(&adbc_connection_options, &adbc_connection, &adbc_error);

	AdbcStatement adbc_statement;

	adbc_status = AdbcStatementInit(&adbc_connection, &adbc_statement, &adbc_error);

	const char *q = "SELECT 42";

	adbc_status = AdbcConnectionSqlExecute(&adbc_connection, q, strlen(q), &adbc_statement, &adbc_error);

	ArrowArrayStream arrow_stream;
	adbc_status = AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);

	ArrowArray arrow_array;
	int arrow_status;
	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);

	printf("should be 42: %d\n", ((int *)arrow_array.children[0]->buffers[1])[0]);

	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	adbc_status = AdbcStatementRelease(&adbc_statement, &adbc_error);
	adbc_status = AdbcConnectionRelease(&adbc_connection, &adbc_error);
	adbc_status = AdbcDatabaseRelease(&adbc_database, &adbc_error);
	return 0;
}