#include "adbc.h"
#include "driver_manager.h"
#include <stdio.h>
#include <string.h>

#define SUCCESS(res)                                                                                                   \
	if ((res) != ADBC_STATUS_OK) {                                                                                     \
		printf("ERROR %s\n", adbc_error.message);                                                                      \
		return -1;                                                                                                     \
	}

int main() {

	AdbcError adbc_error;
	AdbcStatusCode adbc_status;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;

	SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error));
	SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", "../../build/debug/src/libduckdb.dylib", &adbc_error));
	SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error));
	SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error));

	SUCCESS(AdbcConnectionNew(&adbc_database, &adbc_connection, &adbc_error));
	SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_error));

	SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error));
	SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error));
	SUCCESS(AdbcStatementExecute(&adbc_statement, &adbc_error));
	SUCCESS(AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error));

	ArrowArray arrow_array;
	int arrow_status;
	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);

	printf("should be 42: %d\n", ((int *)arrow_array.children[0]->buffers[1])[0]);

	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error));
	SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error));
	SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error));
	return 0;
}
