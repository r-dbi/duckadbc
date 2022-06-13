#include "adbc.h"
#include "driver_manager.h"
#include <stdio.h>
#include <string.h>

// TOOD make a 'nice' macro for error checking here

int main() {
	AdbcDriver adbc_driver;

	AdbcError adbc_error;
	AdbcStatusCode adbc_status;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;

	size_t initialized;

	adbc_status = AdbcLoadDriver("../../build/release/src/libduckdb.dylib", "duckdb_adbc_init", 42, &adbc_driver,
	                             &initialized, &adbc_error);

	adbc_status = AdbcDatabaseNew(&adbc_database, &adbc_error);
	adbc_status = AdbcDatabaseInit(&adbc_database, &adbc_error);

	adbc_status = AdbcConnectionNew(&adbc_database, &adbc_connection, &adbc_error);
	adbc_status = AdbcConnectionInit(&adbc_connection, &adbc_error);

	adbc_status = AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error);
	adbc_status = AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error);

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